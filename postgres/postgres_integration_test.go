//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/sklinkert/go-outbox"
	"github.com/sklinkert/go-outbox/postgres"
	"github.com/testcontainers/testcontainers-go"
	testpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const schemaSQL = `
CREATE TABLE outbox_messages (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL,
    scheduled_at TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    processed_at TIMESTAMP
);

CREATE INDEX idx_outbox_messages_pending
ON outbox_messages (created_at)
WHERE processed_at IS NULL;

CREATE INDEX idx_outbox_messages_scheduled
ON outbox_messages (scheduled_at)
WHERE processed_at IS NULL AND scheduled_at IS NOT NULL;
`

// setupPostgres starts a PostgreSQL 18 container and returns a connected database.
func setupPostgres(t *testing.T) (*sql.DB, func()) {
	ctx := context.Background()

	// Start PostgreSQL 18 container
	pgContainer, err := testpostgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:18-alpine"),
		testpostgres.WithDatabase("testdb"),
		testpostgres.WithUsername("test"),
		testpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("Failed to start postgres container: %v", err)
	}

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Connect
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Wait for database to be ready
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Create schema
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	cleanup := func() {
		db.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return db, cleanup
}

// TestAdvisoryLock_SingleProcessor verifies only one processor can hold the lock at a time.
func TestAdvisoryLock_SingleProcessor(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	lockKey := int64(123456789)

	// Create first store and acquire lock
	store1, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store1: %v", err)
	}
	defer store1.Close()

	err = store1.AcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire lock on store1: %v", err)
	}

	if !store1.HasProcessorLock() {
		t.Error("store1 should have the lock")
	}

	// Create second store and try to acquire lock (should fail)
	store2, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store2: %v", err)
	}
	defer store2.Close()

	acquired, err := store2.TryAcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Unexpected error on store2: %v", err)
	}
	if acquired {
		t.Error("store2 should not be able to acquire the lock")
	}

	if store2.HasProcessorLock() {
		t.Error("store2 should not have the lock")
	}

	// Release lock from store1
	err = store1.ReleaseProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Failed to release lock on store1: %v", err)
	}

	if store1.HasProcessorLock() {
		t.Error("store1 should not have the lock after release")
	}

	// Now store2 should be able to acquire
	acquired, err = store2.TryAcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Unexpected error on store2: %v", err)
	}
	if !acquired {
		t.Error("store2 should be able to acquire the lock after store1 released it")
	}

	if !store2.HasProcessorLock() {
		t.Error("store2 should have the lock")
	}
}

// TestAdvisoryLock_AutomaticRelease verifies lock is released on connection close.
func TestAdvisoryLock_AutomaticRelease(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	lockKey := int64(987654321)

	// Create store and acquire lock
	store1, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store1: %v", err)
	}

	err = store1.AcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Close store (simulates connection close)
	store1.Close()

	// Create new store and verify we can acquire the lock
	store2, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store2: %v", err)
	}
	defer store2.Close()

	acquired, err := store2.TryAcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !acquired {
		t.Error("store2 should be able to acquire lock after store1 closed")
	}
}

// TestAdvisoryLock_Failover tests that a second processor can take over after first releases.
func TestAdvisoryLock_Failover(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	lockKey := int64(111222333)

	// Simulate first processor
	store1, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store1: %v", err)
	}

	err = store1.AcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire lock on store1: %v", err)
	}

	// Simulate second processor waiting
	store2, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store2: %v", err)
	}
	defer store2.Close()

	// Store2 tries but fails
	acquired, err := store2.TryAcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if acquired {
		t.Error("store2 should not acquire lock while store1 has it")
	}

	// Store1 releases (simulating graceful shutdown)
	err = store1.ReleaseProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}
	store1.Close()

	// Now store2 can acquire (failover)
	acquired, err = store2.TryAcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !acquired {
		t.Error("store2 should acquire lock after store1 released")
	}
}

// TestConcurrentFetch_NoDuplicates verifies no duplicate message fetching with lock enabled.
func TestConcurrentFetch_NoDuplicates(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	lockKey := int64(444555666)

	// Create store with lock enabled
	store, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Acquire lock
	err = store.AcquireProcessorLock(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Insert test messages
	messages := make([]*outbox.Message, 10)
	for i := 0; i < 10; i++ {
		messages[i] = &outbox.Message{
			Id:             string(rune('a' + i)),
			Topic:          "test.topic",
			Payload:        []byte("test payload"),
			Headers:        map[string]string{},
			IdempotencyKey: string(rune('a' + i)),
			CreatedAt:      time.Now(),
			Attempts:       0,
		}
	}

	err = store.Insert(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to insert messages: %v", err)
	}

	// Fetch messages concurrently from multiple goroutines
	var wg sync.WaitGroup
	fetchedIds := make(chan string, 10)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs, err := store.FetchPending(ctx, 5)
			if err != nil {
				t.Errorf("Failed to fetch: %v", err)
				return
			}
			for _, msg := range msgs {
				fetchedIds <- msg.Id
			}
		}()
	}

	wg.Wait()
	close(fetchedIds)

	// Count fetched messages
	idCount := make(map[string]int)
	for id := range fetchedIds {
		idCount[id]++
	}

	// Verify no duplicates (each message fetched at most once)
	for id, count := range idCount {
		if count > 1 {
			t.Errorf("Message %s was fetched %d times (expected 1)", id, count)
		}
	}
}

// TestMessageOrdering verifies FIFO ordering is maintained.
func TestMessageOrdering(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()
	lockKey := int64(777888999)

	store, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Insert messages with timestamps
	messages := make([]*outbox.Message, 5)
	baseTime := time.Now()
	for i := 0; i < 5; i++ {
		messages[i] = &outbox.Message{
			Id:             string(rune('a' + i)),
			Topic:          "test.topic",
			Payload:        []byte("test payload"),
			Headers:        map[string]string{},
			IdempotencyKey: string(rune('a' + i)),
			CreatedAt:      baseTime.Add(time.Duration(i) * time.Millisecond),
			Attempts:       0,
		}
	}

	err = store.Insert(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to insert messages: %v", err)
	}

	// Fetch messages
	fetched, err := store.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	// Verify ordering (should be a, b, c, d, e)
	if len(fetched) != 5 {
		t.Fatalf("Expected 5 messages, got %d", len(fetched))
	}

	for i, msg := range fetched {
		expected := string(rune('a' + i))
		if msg.Id != expected {
			t.Errorf("Message at position %d: expected Id %s, got %s", i, expected, msg.Id)
		}
	}
}

// TestLockDisabled verifies behavior when lock key is 0.
func TestLockDisabled(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	ctx := context.Background()

	// Create store with lock disabled (lockKey = 0)
	store, err := postgres.NewStore(db, "outbox_messages", 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	if store.IsProcessorLockEnabled() {
		t.Error("Lock should be disabled when lockKey = 0")
	}

	// Trying to acquire should return error
	err = store.AcquireProcessorLock(ctx)
	if err == nil {
		t.Error("Expected error when trying to acquire with lockKey = 0")
	}

	// HasProcessorLock should return false
	if store.HasProcessorLock() {
		t.Error("Should not have lock when disabled")
	}
}
