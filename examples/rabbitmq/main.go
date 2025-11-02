// Package main demonstrates how to use go-outbox with RabbitMQ.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sklinkert/go-outbox"
	"github.com/sklinkert/go-outbox/postgres"

	_ "github.com/lib/pq"
)

// RabbitMQPublisher implements outbox.BatchPublisher for RabbitMQ.
// In a real implementation, you would use the official RabbitMQ client.
type RabbitMQPublisher struct {
	url      string
	exchange string
}

// NewRabbitMQPublisher creates a new RabbitMQ publisher.
func NewRabbitMQPublisher(url, exchange string) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		url:      url,
		exchange: exchange,
	}
}

// Publish sends a single message to RabbitMQ.
func (p *RabbitMQPublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	// In a real implementation:
	// 1. Get channel from connection pool
	// 2. Publish to exchange with routing key from msg.Topic
	// 3. Handle connection errors with retry logic

	log.Printf("Publishing message %s to topic %s", msg.Id, msg.Topic)

	// Simulate publishing
	time.Sleep(10 * time.Millisecond)

	return nil
}

// PublishBatch sends multiple messages in a single operation.
func (p *RabbitMQPublisher) PublishBatch(ctx context.Context, msgs []*outbox.Message) error {
	// In a real implementation:
	// 1. Get channel from connection pool
	// 2. Use publisher confirms for reliability
	// 3. Publish all messages
	// 4. Wait for confirms

	log.Printf("Batch publishing %d messages", len(msgs))

	for _, msg := range msgs {
		if err := p.Publish(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

// Close releases resources.
func (p *RabbitMQPublisher) Close() error {
	// Close RabbitMQ connection
	return nil
}

// CustomLogger implements outbox.Logger using standard log package.
type CustomLogger struct{}

func (l *CustomLogger) Debug(msg string, fields map[string]interface{}) {
	log.Printf("[DEBUG] %s %v", msg, fields)
}

func (l *CustomLogger) Info(msg string, fields map[string]interface{}) {
	log.Printf("[INFO] %s %v", msg, fields)
}

func (l *CustomLogger) Warn(msg string, fields map[string]interface{}) {
	log.Printf("[WARN] %s %v", msg, fields)
}

func (l *CustomLogger) Error(msg string, fields map[string]interface{}) {
	log.Printf("[ERROR] %s %v", msg, fields)
}

// CustomMetricsHook implements outbox.MetricsHook for observability.
type CustomMetricsHook struct{}

func (m *CustomMetricsHook) OnMessagesFetched(count int) {
	log.Printf("[METRICS] Fetched %d messages", count)
}

func (m *CustomMetricsHook) OnMessagesPublished(count int, duration time.Duration) {
	log.Printf("[METRICS] Published %d messages in %s", count, duration)
}

func (m *CustomMetricsHook) OnMessagesFailed(count int) {
	log.Printf("[METRICS] Failed to publish %d messages", count)
}

func (m *CustomMetricsHook) OnPollError(err error) {
	log.Printf("[METRICS] Poll error: %v", err)
}

func (m *CustomMetricsHook) OnPublishError(err error) {
	log.Printf("[METRICS] Publish error: %v", err)
}

func main() {
	// Connect to PostgreSQL
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost/outbox_demo?sslmode=disable")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Note: Create the database schema first using the SQL from postgres/README.md
	// For this example, we assume the schema is already created

	// Initialize store with processor lock enabled
	// Using a consistent lock key ensures only one processor runs at a time
	lockKey := int64(987654321)
	store, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Initialize RabbitMQ publisher
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	publisher := NewRabbitMQPublisher(rabbitURL, "events")
	defer publisher.Close()

	// Configure outbox processor
	config := outbox.DefaultConfig()
	config.BatchSize = 50
	config.PollInterval = 2 * time.Second
	config.ProcessorLockKey = lockKey // Enable single-processor mode
	config.FlushTimeout = 100 * time.Millisecond
	config.WorkerCount = 3
	config.MaxRetries = 5
	config.Logger = &CustomLogger{}
	config.MetricsHook = &CustomMetricsHook{}

	// Create processor
	processor, err := outbox.NewProcessor(store, publisher, config)
	if err != nil {
		log.Fatal(err)
	}

	// Start processing
	if err := processor.Start(); err != nil {
		log.Fatal(err)
	}

	log.Println("Outbox processor started. Press Ctrl+C to stop.")

	// Insert some example messages
	go insertExampleMessages(db, store)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	if err := processor.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	log.Println("Shutdown complete")
}

// insertExampleMessages demonstrates inserting messages within a transaction.
func insertExampleMessages(db *sql.DB, store *postgres.Store) {
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		// Start transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Printf("Failed to begin transaction: %v", err)
			continue
		}

		// Your business logic would go here
		// e.g., insert order, update inventory, etc.

		// Create outbox message
		messages := []*outbox.Message{
			{
				Id:             generateMessageId(),
				Topic:          "orders.created",
				Payload:        []byte(fmt.Sprintf(`{"order_id": "order-%d", "total": 99.99}`, i)),
				Headers:        map[string]string{"content-type": "application/json"},
				IdempotencyKey: fmt.Sprintf("order-%d-created", i),
				CreatedAt:      time.Now(),
			},
		}

		// Insert message within transaction
		ctx = postgres.WithTx(ctx, tx)
		err = store.Insert(ctx, messages)
		if err != nil {
			tx.Rollback()
			log.Printf("Failed to insert message: %v", err)
			continue
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			log.Printf("Failed to commit transaction: %v", err)
			continue
		}

		log.Printf("Created message %s", messages[0].Id)
		time.Sleep(3 * time.Second)
	}
}

func generateMessageId() string {
	// In production, use UUIDv7
	// import "github.com/google/uuid"
	// return uuid.Must(uuid.NewV7()).String()
	return fmt.Sprintf("msg-%d", time.Now().UnixNano())
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
