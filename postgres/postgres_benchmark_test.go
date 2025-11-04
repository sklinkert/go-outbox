//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/sklinkert/go-outbox"
	"github.com/sklinkert/go-outbox/postgres"
	"github.com/testcontainers/testcontainers-go"
	testpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// benchmarkPublisher is a no-op publisher for benchmarks.
type benchmarkPublisher struct{}

func newBenchmarkPublisher() *benchmarkPublisher {
	return &benchmarkPublisher{}
}

func (p *benchmarkPublisher) Publish(_ context.Context, _ *outbox.Message) error {
	return nil
}

func (p *benchmarkPublisher) Close() error {
	return nil
}

// benchmarkMetrics tracks performance metrics during benchmarks.
type benchmarkMetrics struct {
	publishedCount       atomic.Int64
	publishedMu          sync.Mutex
	publishedIds         map[string]bool
	latencies            []time.Duration
	messageCreationTimes map[string]time.Time
}

func newBenchmarkMetrics() *benchmarkMetrics {
	return &benchmarkMetrics{
		publishedIds:         make(map[string]bool),
		latencies:            make([]time.Duration, 0),
		messageCreationTimes: make(map[string]time.Time),
	}
}

func (m *benchmarkMetrics) OnMessagesFetched(count int) {}

func (m *benchmarkMetrics) OnMessagesPublished(count int, duration time.Duration) {
	m.publishedCount.Add(int64(count))
}

func (m *benchmarkMetrics) OnMessagesFailed(count int) {}

func (m *benchmarkMetrics) OnPollError(err error) {}

func (m *benchmarkMetrics) OnPublishError(err error) {}

func (m *benchmarkMetrics) getCount() int64 {
	return m.publishedCount.Load()
}

func (m *benchmarkMetrics) getAverageLatency() time.Duration {
	m.publishedMu.Lock()
	defer m.publishedMu.Unlock()

	if len(m.latencies) == 0 {
		return 0
	}

	var sum time.Duration
	for _, lat := range m.latencies {
		sum += lat
	}
	return sum / time.Duration(len(m.latencies))
}

// setupBenchmarkPostgres starts a PostgreSQL container for benchmarks.
func setupBenchmarkPostgres(b *testing.B) (*sql.DB, func()) {
	ctx := context.Background()

	// Start PostgreSQL 18 container
	pgContainer, err := testpostgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:18-alpine"),
		testpostgres.WithDatabase("benchdb"),
		testpostgres.WithUsername("bench"),
		testpostgres.WithPassword("bench"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		b.Fatalf("Failed to start postgres container: %v", err)
	}

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("Failed to get connection string: %v", err)
	}

	// Connect with larger pool for benchmarks
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	// Configure connection pool for performance
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Wait for database to be ready
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Create schema
	if _, err := db.Exec(schemaSQL); err != nil {
		b.Fatalf("Failed to create schema: %v", err)
	}

	cleanup := func() {
		db.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			b.Logf("Failed to terminate container: %v", err)
		}
	}

	return db, cleanup
}

// seedMessages inserts test messages into the database.
func seedMessages(b *testing.B, store outbox.Store, count int) {
	ctx := context.Background()
	batchSize := 1000

	for i := 0; i < count; i += batchSize {
		end := i + batchSize
		if end > count {
			end = count
		}

		messages := make([]*outbox.Message, end-i)
		for j := i; j < end; j++ {
			messages[j-i] = &outbox.Message{
				Id:             fmt.Sprintf("msg-%d", j),
				Topic:          "benchmark.topic",
				Payload:        []byte(fmt.Sprintf("benchmark payload %d", j)),
				Headers:        map[string]string{"test": "benchmark"},
				IdempotencyKey: fmt.Sprintf("key-%d", j),
				CreatedAt:      time.Now(),
				Attempts:       0,
			}
		}

		if err := store.Insert(ctx, messages); err != nil {
			b.Fatalf("Failed to insert messages: %v", err)
		}
	}
}

// runProcessorUntilComplete runs processor until all messages are published.
func runProcessorUntilComplete(b *testing.B, processor *outbox.Processor, metrics *benchmarkMetrics, expectedCount int, timeout time.Duration) {
	if err := processor.Start(); err != nil {
		b.Fatalf("Failed to start processor: %v", err)
	}

	// Wait for all messages to be published
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if metrics.getCount() >= int64(expectedCount) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := processor.Stop(); err != nil {
		b.Logf("Warning: processor stop error: %v", err)
	}
}

// BenchmarkProcessor_Throughput_Small benchmarks throughput with 1K messages.
func BenchmarkProcessor_Throughput_Small(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	const messageCount = 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Setup
		store, err := postgres.NewStore(db, "outbox_messages", 0) // Disable lock for benchmarking
		if err != nil {
			b.Fatalf("Failed to create store: %v", err)
		}

		publisher := newBenchmarkPublisher()
		metrics := newBenchmarkMetrics()
		config := outbox.DefaultConfig()
		config.PollInterval = 50 * time.Millisecond
		config.BatchSize = 100
		config.WorkerCount = 5
		config.FlushTimeout = 100 * time.Millisecond
		config.MetricsHook = metrics

		processor, err := outbox.NewProcessor(store, publisher, config)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}

		seedMessages(b, store, messageCount)

		b.StartTimer()
		start := time.Now()

		// Run processor
		runProcessorUntilComplete(b, processor, metrics, messageCount, 30*time.Second)

		elapsed := time.Since(start)
		b.StopTimer()

		// Report metrics
		throughput := float64(messageCount) / elapsed.Seconds()
		b.ReportMetric(throughput, "msgs/sec")

		// Cleanup
		_, _ = db.Exec("TRUNCATE outbox_messages")
		store.Close()
	}
}

// BenchmarkProcessor_Throughput_Medium benchmarks throughput with 10K messages.
func BenchmarkProcessor_Throughput_Medium(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	const messageCount = 10000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Setup
		store, err := postgres.NewStore(db, "outbox_messages", 0) // Disable lock for benchmarking
		if err != nil {
			b.Fatalf("Failed to create store: %v", err)
		}

		publisher := newBenchmarkPublisher()
		metrics := newBenchmarkMetrics()
		config := outbox.DefaultConfig()
		config.PollInterval = 50 * time.Millisecond
		config.BatchSize = 100
		config.WorkerCount = 5
		config.FlushTimeout = 100 * time.Millisecond
		config.MetricsHook = metrics

		processor, err := outbox.NewProcessor(store, publisher, config)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}

		seedMessages(b, store, messageCount)

		b.StartTimer()
		start := time.Now()

		// Run processor
		runProcessorUntilComplete(b, processor, metrics, messageCount, 60*time.Second)

		elapsed := time.Since(start)
		b.StopTimer()

		// Report metrics
		throughput := float64(messageCount) / elapsed.Seconds()
		b.ReportMetric(throughput, "msgs/sec")

		// Cleanup
		_, _ = db.Exec("TRUNCATE outbox_messages")
		store.Close()
	}
}

// BenchmarkProcessor_Throughput_Large benchmarks throughput with 100K messages.
func BenchmarkProcessor_Throughput_Large(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	const messageCount = 100000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Setup
		store, err := postgres.NewStore(db, "outbox_messages", 0) // Disable lock for benchmarking
		if err != nil {
			b.Fatalf("Failed to create store: %v", err)
		}

		publisher := newBenchmarkPublisher()
		metrics := newBenchmarkMetrics()
		config := outbox.DefaultConfig()
		config.PollInterval = 50 * time.Millisecond
		config.BatchSize = 100
		config.WorkerCount = 5
		config.FlushTimeout = 100 * time.Millisecond
		config.MetricsHook = metrics

		processor, err := outbox.NewProcessor(store, publisher, config)
		if err != nil {
			b.Fatalf("Failed to create processor: %v", err)
		}

		seedMessages(b, store, messageCount)

		b.StartTimer()
		start := time.Now()

		// Run processor
		runProcessorUntilComplete(b, processor, metrics, messageCount, 300*time.Second)

		elapsed := time.Since(start)
		b.StopTimer()

		// Report metrics
		throughput := float64(messageCount) / elapsed.Seconds()
		b.ReportMetric(throughput, "msgs/sec")

		// Cleanup
		_, _ = db.Exec("TRUNCATE outbox_messages")
		store.Close()
	}
}

// BenchmarkProcessor_BatchSize benchmarks different batch sizes.
func BenchmarkProcessor_BatchSize(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500, 1000}
	const messageCount = 10000

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			db, cleanup := setupBenchmarkPostgres(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Setup
				store, err := postgres.NewStore(db, "outbox_messages", 12345)
				if err != nil {
					b.Fatalf("Failed to create store: %v", err)
				}

				publisher := newBenchmarkPublisher()
				metrics := newBenchmarkMetrics()
				config := outbox.DefaultConfig()
				config.PollInterval = 50 * time.Millisecond
				config.BatchSize = batchSize
				config.WorkerCount = 5
				config.FlushTimeout = 100 * time.Millisecond
				config.MetricsHook = metrics

				processor, err := outbox.NewProcessor(store, publisher, config)
				if err != nil {
					b.Fatalf("Failed to create processor: %v", err)
				}

				seedMessages(b, store, messageCount)

				b.StartTimer()
				start := time.Now()

				// Run processor
				runProcessorUntilComplete(b, processor, metrics, messageCount, 60*time.Second)

				elapsed := time.Since(start)
				b.StopTimer()

				// Report metrics
				throughput := float64(messageCount) / elapsed.Seconds()
				b.ReportMetric(throughput, "msgs/sec")

				// Cleanup
				_, _ = db.Exec("TRUNCATE outbox_messages")
				store.Close()
			}
		})
	}
}

// BenchmarkProcessor_WorkerCount benchmarks different worker counts.
func BenchmarkProcessor_WorkerCount(b *testing.B) {
	workerCounts := []int{1, 5, 10, 20}
	const messageCount = 10000

	for _, workerCount := range workerCounts {
		b.Run(fmt.Sprintf("Workers_%d", workerCount), func(b *testing.B) {
			db, cleanup := setupBenchmarkPostgres(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Setup
				store, err := postgres.NewStore(db, "outbox_messages", 12345)
				if err != nil {
					b.Fatalf("Failed to create store: %v", err)
				}

				publisher := newBenchmarkPublisher()
				metrics := newBenchmarkMetrics()
				config := outbox.DefaultConfig()
				config.PollInterval = 50 * time.Millisecond
				config.BatchSize = 100
				config.WorkerCount = workerCount
				config.FlushTimeout = 100 * time.Millisecond
				config.MetricsHook = metrics

				processor, err := outbox.NewProcessor(store, publisher, config)
				if err != nil {
					b.Fatalf("Failed to create processor: %v", err)
				}

				seedMessages(b, store, messageCount)

				b.StartTimer()
				start := time.Now()

				// Run processor
				runProcessorUntilComplete(b, processor, metrics, messageCount, 60*time.Second)

				elapsed := time.Since(start)
				b.StopTimer()

				// Report metrics
				throughput := float64(messageCount) / elapsed.Seconds()
				b.ReportMetric(throughput, "msgs/sec")

				// Cleanup
				_, _ = db.Exec("TRUNCATE outbox_messages")
				store.Close()
			}
		})
	}
}

// BenchmarkProcessor_Combined benchmarks optimal configuration combinations.
func BenchmarkProcessor_Combined(b *testing.B) {
	type config struct {
		name        string
		batchSize   int
		workerCount int
	}

	configs := []config{
		{"Balanced", 100, 5},
		{"HighThroughput", 500, 10},
		{"LowLatency", 50, 10},
	}

	const messageCount = 10000

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			db, cleanup := setupBenchmarkPostgres(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Setup
				store, err := postgres.NewStore(db, "outbox_messages", 12345)
				if err != nil {
					b.Fatalf("Failed to create store: %v", err)
				}

				publisher := newBenchmarkPublisher()
				metrics := newBenchmarkMetrics()
				outboxConfig := outbox.DefaultConfig()
				outboxConfig.PollInterval = 50 * time.Millisecond
				outboxConfig.BatchSize = cfg.batchSize
				outboxConfig.WorkerCount = cfg.workerCount
				outboxConfig.FlushTimeout = 100 * time.Millisecond
				outboxConfig.MetricsHook = metrics

				processor, err := outbox.NewProcessor(store, publisher, outboxConfig)
				if err != nil {
					b.Fatalf("Failed to create processor: %v", err)
				}

				seedMessages(b, store, messageCount)

				b.StartTimer()
				start := time.Now()

				// Run processor
				runProcessorUntilComplete(b, processor, metrics, messageCount, 60*time.Second)

				elapsed := time.Since(start)
				b.StopTimer()

				// Report metrics
				throughput := float64(messageCount) / elapsed.Seconds()
				b.ReportMetric(throughput, "msgs/sec")

				// Cleanup
				_, _ = db.Exec("TRUNCATE outbox_messages")
				store.Close()
			}
		})
	}
}

// BenchmarkStore_FetchPending benchmarks database fetch performance.
func BenchmarkStore_FetchPending(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	store, err := postgres.NewStore(db, "outbox_messages", 12345)
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Seed messages
	seedMessages(b, store, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.FetchPending(ctx, 100)
		if err != nil {
			b.Fatalf("Failed to fetch: %v", err)
		}
	}
}

// BenchmarkStore_MarkSent benchmarks bulk update performance.
func BenchmarkStore_MarkSent(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	store, err := postgres.NewStore(db, "outbox_messages", 12345)
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Seed messages
		seedMessages(b, store, 100)

		// Fetch them
		messages, err := store.FetchPending(ctx, 100)
		if err != nil {
			b.Fatalf("Failed to fetch: %v", err)
		}

		// Get IDs
		ids := make([]string, len(messages))
		for j, msg := range messages {
			ids[j] = msg.Id
		}

		b.StartTimer()

		// Benchmark MarkSent
		err = store.MarkSent(ctx, ids)
		if err != nil {
			b.Fatalf("Failed to mark sent: %v", err)
		}

		b.StopTimer()
		_, _ = db.Exec("TRUNCATE outbox_messages")
	}
}

// BenchmarkStore_Insert benchmarks bulk insert performance.
func BenchmarkStore_Insert(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	store, err := postgres.NewStore(db, "outbox_messages", 12345)
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Prepare messages
	messages := make([]*outbox.Message, 100)
	for i := 0; i < 100; i++ {
		messages[i] = &outbox.Message{
			Id:             fmt.Sprintf("msg-%d", i),
			Topic:          "benchmark.topic",
			Payload:        []byte(fmt.Sprintf("payload %d", i)),
			Headers:        map[string]string{"test": "benchmark"},
			IdempotencyKey: fmt.Sprintf("key-%d", i),
			CreatedAt:      time.Now(),
			Attempts:       0,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Update IDs for uniqueness
		for j := 0; j < 100; j++ {
			messages[j].Id = fmt.Sprintf("msg-%d-%d", i, j)
			messages[j].IdempotencyKey = fmt.Sprintf("key-%d-%d", i, j)
		}

		b.StartTimer()

		err := store.Insert(ctx, messages)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}

		b.StopTimer()
		_, _ = db.Exec("TRUNCATE outbox_messages")
	}
}
