// Package main demonstrates how to use go-outbox with Kafka.
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

// KafkaPublisher implements outbox.BatchPublisher for Kafka.
// In a real implementation, you would use the official Kafka client.
type KafkaPublisher struct {
	brokers []string
}

// NewKafkaPublisher creates a new Kafka publisher.
func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	return &KafkaPublisher{
		brokers: brokers,
	}
}

// Publish sends a single message to Kafka.
func (p *KafkaPublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	// In a real implementation:
	// 1. Get producer from pool
	// 2. Create ProducerMessage with topic, key, value
	// 3. Send with delivery confirmation
	// 4. Handle errors with retry logic

	log.Printf("Publishing message %s to topic %s", msg.Id, msg.Topic)

	// Simulate publishing
	time.Sleep(10 * time.Millisecond)

	return nil
}

// PublishBatch sends multiple messages in a single operation.
// Kafka excels at batch publishing for maximum throughput.
func (p *KafkaPublisher) PublishBatch(ctx context.Context, msgs []*outbox.Message) error {
	// In a real implementation:
	// 1. Get producer from pool
	// 2. Create multiple ProducerMessages
	// 3. Send batch with delivery reports
	// 4. Wait for all confirmations
	// 5. Handle partial failures

	log.Printf("Batch publishing %d messages to Kafka", len(msgs))

	// Group messages by topic for optimal batching
	topicMessages := make(map[string][]*outbox.Message)
	for _, msg := range msgs {
		topicMessages[msg.Topic] = append(topicMessages[msg.Topic], msg)
	}

	for topic, messages := range topicMessages {
		log.Printf("Publishing %d messages to topic %s", len(messages), topic)
		for _, msg := range messages {
			if err := p.Publish(ctx, msg); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close releases resources.
func (p *KafkaPublisher) Close() error {
	// Close Kafka producer
	return nil
}

// SimpleLogger implements outbox.Logger.
type SimpleLogger struct{}

func (l *SimpleLogger) Debug(msg string, fields map[string]interface{}) {
	log.Printf("[DEBUG] %s %v", msg, fields)
}

func (l *SimpleLogger) Info(msg string, fields map[string]interface{}) {
	log.Printf("[INFO] %s %v", msg, fields)
}

func (l *SimpleLogger) Warn(msg string, fields map[string]interface{}) {
	log.Printf("[WARN] %s %v", msg, fields)
}

func (l *SimpleLogger) Error(msg string, fields map[string]interface{}) {
	log.Printf("[ERROR] %s %v", msg, fields)
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
	lockKey := int64(123456789)
	store, err := postgres.NewStore(db, "outbox_messages", lockKey)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Initialize Kafka publisher
	kafkaBrokers := getEnvSlice("KAFKA_BROKERS", []string{"localhost:9092"})
	publisher := NewKafkaPublisher(kafkaBrokers)
	defer publisher.Close()

	// Configure outbox processor for high throughput
	config := outbox.DefaultConfig()
	config.BatchSize = 200 // Kafka handles large batches well
	config.PollInterval = 1 * time.Second
	config.FlushTimeout = 50 * time.Millisecond
	config.WorkerCount = 5
	config.MaxRetries = 10
	config.ProcessorLockKey = lockKey // Enable single-processor mode
	config.Logger = &SimpleLogger{}

	// Create processor
	processor, err := outbox.NewProcessor(store, publisher, config)
	if err != nil {
		log.Fatal(err)
	}

	// Start processing
	if err := processor.Start(); err != nil {
		log.Fatal(err)
	}

	log.Println("Kafka outbox processor started. Press Ctrl+C to stop.")

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

// insertExampleMessages demonstrates inserting messages for various Kafka topics.
func insertExampleMessages(db *sql.DB, store *postgres.Store) {
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	topics := []string{"user.events", "order.events", "inventory.events"}

	for i := 0; i < 20; i++ {
		// Start transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Printf("Failed to begin transaction: %v", err)
			continue
		}

		// Your business logic would go here
		// e.g., create user, process order, update inventory

		// Create outbox messages for different topics
		topic := topics[i%len(topics)]
		messages := []*outbox.Message{
			{
				Id:             generateMessageId(),
				Topic:          topic,
				Payload:        []byte(fmt.Sprintf(`{"event_id": "%d", "timestamp": "%s"}`, i, time.Now().Format(time.RFC3339))),
				Headers:        map[string]string{"content-type": "application/json", "version": "1.0"},
				IdempotencyKey: fmt.Sprintf("%s-%d", topic, i),
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

		log.Printf("Created message %s for topic %s", messages[0].Id, topic)
		time.Sleep(2 * time.Second)
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

func getEnvSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		return []string{value}
	}
	return defaultValue
}
