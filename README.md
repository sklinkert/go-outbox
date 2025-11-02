# go-outbox

A production-ready, high-performance implementation of the transactional outbox pattern for Go.

## Overview

The transactional outbox pattern ensures reliable message delivery by storing messages in a database within the same transaction as business data. Messages are then asynchronously published to message brokers (RabbitMQ, Kafka, etc.) guaranteeing **at-least-once delivery** semantics.

## Why Outbox Pattern?

### The Problem

When building distributed systems, you often need to:
1. Update your database (e.g., create an order)
2. Publish an event to a message broker (e.g., "OrderCreated")

This creates a distributed transaction problem:
- **Database succeeds, broker fails**: Event is lost, other services never know about the order
- **Broker succeeds, database fails**: Event is sent but order doesn't exist

### The Solution

The outbox pattern solves this by:
1. Storing messages in a database table within the same transaction as business data
2. A separate process polls the outbox table and publishes messages
3. Messages are marked as processed after successful publishing

This ensures **atomicity** - either both succeed or both fail.

## Features

- **Generic Interface**: Works with any message broker (RabbitMQ, Kafka, SQS, etc.)
- **Batch Publishing**: High-throughput batch operations with smart flushing strategies
- **PostgreSQL Implementation**: Production-ready reference implementation with optimized queries
- **Session-Level Locking**: Single-processor mode ensures strict FIFO ordering and automatic failover
- **Advisory Locks**: Prevents duplicate processing across multiple instances
- **High Availability**: Deploy multiple instances, only one processes (automatic failover)
- **Graceful Shutdown**: Ensures in-flight messages are processed before stopping
- **Retry Logic**: Exponential backoff with configurable max retries
- **Scheduled Messages**: Support for delayed message publishing
- **Observability**: Hooks for logging and metrics integration
- **Zero External Dependencies**: Core library uses only Go standard library
- **Idempotency**: Built-in support for idempotency keys

## Installation

```bash
go get github.com/sklinkert/go-outbox
```

For PostgreSQL support:

```bash
go get github.com/sklinkert/go-outbox/postgres
```

## Quick Start

### 1. Define Your Publisher

Implement the `Publisher` or `BatchPublisher` interface:

```go
type MyPublisher struct {
    // Your broker client
}

func (p *MyPublisher) Publish(ctx context.Context, msg *outbox.Message) error {
    // Publish to your message broker
    return nil
}

func (p *MyPublisher) PublishBatch(ctx context.Context, msgs []*outbox.Message) error {
    // Batch publish for higher throughput
    return nil
}

func (p *MyPublisher) Close() error {
    return nil
}
```

### 2. Create the Store

Using PostgreSQL:

```go
import (
    "database/sql"
    "github.com/sklinkert/go-outbox/postgres"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", "postgres://user:pass@localhost/mydb")
if err != nil {
    log.Fatal(err)
}

// Create the outbox table using SQL from postgres/README.md

// Enable processor lock for single-processor mode (recommended)
lockKey := int64(123456789) // Consistent across all instances
store, err := postgres.NewStore(db, "outbox_messages", lockKey)
if err != nil {
    log.Fatal(err)
}
defer store.Close()
```

### 3. Start the Processor

```go
import "github.com/sklinkert/go-outbox"

config := outbox.DefaultConfig()
config.BatchSize = 100
config.PollInterval = 1 * time.Second
config.ProcessorLockKey = lockKey // Enable single-processor mode

processor, err := outbox.NewProcessor(store, publisher, config)
if err != nil {
    log.Fatal(err)
}

if err := processor.Start(); err != nil {
    log.Fatal(err)
}

// Graceful shutdown (releases processor lock)
defer processor.Stop()
```

### 4. Insert Messages in Transactions

```go
// Start your business transaction
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

// Your business logic
_, err = tx.Exec("INSERT INTO orders (id, total) VALUES ($1, $2)", orderID, total)
if err != nil {
    return err
}

// Create outbox message
messages := []*outbox.Message{
    {
        Id:             generateUniqueId(), // Generate unique ID (e.g., UUIDv7, ULID, or timestamp-based)
        Topic:          "orders.created",
        Payload:        []byte(`{"order_id": "12345", "total": 99.99}`),
        Headers:        map[string]string{"content-type": "application/json"},
        IdempotencyKey: "order-12345-created",
        CreatedAt:      time.Now(),
    },
}

// Insert message within the same transaction
ctx = postgres.WithTx(ctx, tx)
err = store.Insert(ctx, messages)
if err != nil {
    return err
}

// Commit both order and outbox message atomically
return tx.Commit()
```

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────┐
│                       Processor                         │
│  ┌──────────┐    ┌─────────┐    ┌─────────────────┐   │
│  │  Poller  │───▶│ Batcher │───▶│ Worker Pool (n) │   │
│  └──────────┘    └─────────┘    └─────────────────┘   │
│       │               │                   │             │
└───────┼───────────────┼───────────────────┼─────────────┘
        │               │                   │
        ▼               ▼                   ▼
    ┌───────┐      ┌────────┐         ┌───────────┐
    │ Store │      │ Batch  │         │ Publisher │
    │       │      │ Chan   │         │           │
    └───────┘      └────────┘         └───────────┘
```

1. **Poller**: Fetches pending messages from the store at regular intervals
2. **Batcher**: Aggregates messages into batches (size-based + time-based flushing)
3. **Worker Pool**: Concurrent workers publish batches to the message broker
4. **Store**: Database interface for persisting and retrieving messages
5. **Publisher**: Message broker interface for publishing messages

### Message Flow

```
Business Logic ──▶ [Transaction] ──▶ Database
                         │
                         ├──▶ Business Data
                         └──▶ Outbox Message
                                    │
                                    ▼
                            [Poller fetches]
                                    │
                                    ▼
                            [Batcher accumulates]
                                    │
                                    ▼
                            [Worker publishes]
                                    │
                                    ▼
                            Message Broker (Kafka, RabbitMQ, etc.)
```

## Configuration

### Default Configuration

```go
config := outbox.DefaultConfig()
// Returns:
// PollInterval:    1 second
// BatchSize:       100
// MaxRetries:      10
// RetryBackoff:    5 seconds
// FlushTimeout:    100 milliseconds
// WorkerCount:     5
// ShutdownTimeout: 30 seconds
```

### Custom Configuration

```go
config := outbox.Config{
    PollInterval:    2 * time.Second,        // How often to check for messages
    BatchSize:       200,                     // Max messages per batch
    MaxRetries:      15,                      // Max publishing attempts
    RetryBackoff:    10 * time.Second,        // Base retry delay (exponential)
    FlushTimeout:    50 * time.Millisecond,   // Max wait before flushing partial batch
    WorkerCount:     10,                      // Concurrent publishing workers
    ShutdownTimeout: 60 * time.Second,        // Graceful shutdown timeout
    Logger:          myLogger,                // Custom logger
    MetricsHook:     myMetrics,               // Custom metrics
}
```

### Performance Tuning

**High Throughput** (Kafka-optimized):
```go
config.BatchSize = 500
config.PollInterval = 100 * time.Millisecond
config.FlushTimeout = 50 * time.Millisecond
config.WorkerCount = 10
```

**Low Latency** (minimize delay):
```go
config.BatchSize = 10
config.PollInterval = 100 * time.Millisecond
config.FlushTimeout = 10 * time.Millisecond
config.WorkerCount = 20
```

**Resource Constrained**:
```go
config.BatchSize = 50
config.PollInterval = 5 * time.Second
config.FlushTimeout = 200 * time.Millisecond
config.WorkerCount = 2
```

## Interfaces

### Store Interface

```go
type Store interface {
    // Fetch pending messages (with database locks)
    FetchPending(ctx context.Context, batchSize int) ([]*Message, error)

    // Mark messages as successfully published
    MarkSent(ctx context.Context, messageIds []string) error

    // Record failed attempts
    MarkFailed(ctx context.Context, failures []MessageFailure) error

    // Insert messages within caller's transaction
    Insert(ctx context.Context, messages []*Message) error
}
```

### Publisher Interface

```go
type Publisher interface {
    // Publish single message
    Publish(ctx context.Context, msg *Message) error

    // Close resources
    Close() error
}

type BatchPublisher interface {
    Publisher

    // Publish multiple messages (high throughput)
    PublishBatch(ctx context.Context, msgs []*Message) error
}
```

### Logger Interface

```go
type Logger interface {
    Debug(msg string, fields map[string]interface{})
    Info(msg string, fields map[string]interface{})
    Warn(msg string, fields map[string]interface{})
    Error(msg string, fields map[string]interface{})
}
```

### MetricsHook Interface

```go
type MetricsHook interface {
    OnMessagesFetched(count int)
    OnMessagesPublished(count int, duration time.Duration)
    OnMessagesFailed(count int)
    OnPollError(err error)
    OnPublishError(err error)
}
```

## PostgreSQL Implementation

The included PostgreSQL implementation provides:

- **UUIDv7 support** for time-ordered IDs
- **Advisory locks** to prevent duplicate processing
- **Partial indices** for high-performance queries
- **Soft delete** for audit trails
- **JSONB headers** for flexible metadata
- **Scheduled messages** for delayed publishing

See [postgres/README.md](postgres/README.md) for detailed documentation.

## Examples

### RabbitMQ

Complete example with RabbitMQ integration:

```bash
cd examples/rabbitmq
go run main.go
```

See [examples/rabbitmq/README.md](examples/rabbitmq/README.md) for details.

### Kafka

High-throughput example with Kafka:

```bash
cd examples/kafka
go run main.go
```

See [examples/kafka/README.md](examples/kafka/README.md) for details.

## Best Practices

### 1. Always Use Transactions

Insert outbox messages atomically with business data:

```go
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

// Business logic
tx.Exec("UPDATE inventory SET quantity = quantity - 1 WHERE product_id = $1", productID)

// Outbox message
ctx = postgres.WithTx(ctx, tx)
store.Insert(ctx, messages)

tx.Commit()
```

### 2. Use Idempotency Keys

Ensure consumers can deduplicate messages:

```go
IdempotencyKey: fmt.Sprintf("order-%s-created", orderID)
```

### 3. Monitor Failed Messages

Set up alerts for messages exceeding retry limits:

```sql
SELECT COUNT(*) FROM outbox_messages
WHERE processed_at IS NULL AND attempts >= 10
```

### 4. Archive Old Messages

Regularly clean up processed messages:

```sql
DELETE FROM outbox_messages
WHERE processed_at < NOW() - INTERVAL '30 days'
```

### 5. Use Time-Ordered IDs

Time-ordered IDs (UUIDv7, ULID, etc.) improve database performance:

```go
// Option 1: UUIDv7 (requires github.com/google/uuid v1.4.0+)
import "github.com/google/uuid"
id := uuid.Must(uuid.NewV7()).String()

// Option 2: Simple timestamp-based ID
import "fmt"
import "time"
import "math/rand"
id := fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63())
```

### 6. Batch Operations

Process multiple messages at once for higher throughput by implementing `BatchPublisher`.

### 7. Handle Partial Failures

Implement proper error handling for partial batch failures:

```go
func (p *MyPublisher) PublishBatch(ctx context.Context, msgs []*Message) error {
    for _, msg := range msgs {
        if err := p.Publish(ctx, msg); err != nil {
            // Log error but continue with other messages
            log.Printf("Failed to publish %s: %v", msg.Id, err)
            return err // Or handle individually
        }
    }
    return nil
}
```

## Observability

### Custom Logger Example

```go
type LogrusLogger struct {
    logger *logrus.Logger
}

func (l *LogrusLogger) Info(msg string, fields map[string]interface{}) {
    l.logger.WithFields(fields).Info(msg)
}
// ... implement other methods
```

### Metrics Example

```go
type PrometheusMetrics struct {
    messagesFetched   prometheus.Counter
    messagesPublished prometheus.Histogram
    messagesFailed    prometheus.Counter
}

func (m *PrometheusMetrics) OnMessagesPublished(count int, duration time.Duration) {
    m.messagesPublished.Observe(duration.Seconds())
}
// ... implement other methods
```

## Scaling

### High Availability (Recommended)

Deploy multiple processor instances with **single-processor mode** for automatic failover:

```go
// Configure the same lock key in all instances
config := outbox.DefaultConfig()
config.ProcessorLockKey = 123456789 // Consistent across all instances

// Deploy 2-3 instances
// Only one will process messages at a time
// Others act as hot standbys
```

**How it works**:
1. **Instance 1** acquires the processor lock and processes messages
2. **Instance 2** tries to acquire lock, fails, waits on standby
3. **Instance 3** also waits on standby
4. If Instance 1 crashes, Instance 2 immediately acquires lock and takes over
5. Zero downtime, automatic failover

**Benefits**:
- ✅ Strict FIFO ordering maintained
- ✅ Automatic failover on crash
- ✅ No manual intervention required
- ✅ Simple to deploy and manage

### High Throughput (Advanced)

For scenarios requiring maximum throughput, disable the processor lock:

```go
// Disable session lock
store, err := postgres.NewStore(db, "outbox_messages", 0)
config.ProcessorLockKey = 0

// Now multiple processors can run concurrently
// Each processes different messages
```

**Trade-offs**:
- ✅ Higher throughput (parallel processing)
- ✅ Better resource utilization across multiple instances
- ⚠️ Best-effort ordering (not strict FIFO)
- ⚠️ More complex to reason about

**When to use**:
- High message volume (>10k messages/sec)
- Order-independent message processing
- Advanced monitoring and operations team

### Vertical Scaling

Increase resources per instance:

```go
config.WorkerCount = 20      // More concurrent workers
config.BatchSize = 500       // Larger batches
```

## Testing

### Mock Store

```go
type MockStore struct {
    messages []*outbox.Message
}

func (s *MockStore) FetchPending(ctx context.Context, batchSize int) ([]*outbox.Message, error) {
    return s.messages, nil
}
// ... implement other methods
```

### Mock Publisher

```go
type MockPublisher struct {
    published []*outbox.Message
}

func (p *MockPublisher) Publish(ctx context.Context, msg *outbox.Message) error {
    p.published = append(p.published, msg)
    return nil
}
```

## Troubleshooting

### Messages Not Processing

1. Check processor is started: `processor.Start()` called
2. Verify database connectivity
3. Check `scheduled_at` (may be scheduled for future)
4. Review retry limits (`attempts >= max_retries`)

### High Latency

1. Decrease `PollInterval` for more frequent checks
2. Increase `WorkerCount` for more parallelism
3. Optimize database queries (check indices)
4. Increase `BatchSize` for higher throughput

### High Database Load

1. Increase `PollInterval` to reduce polling frequency
2. Archive old processed messages
3. Ensure indices are present and being used
4. Use connection pooling

### Memory Issues

1. Decrease `BatchSize` to reduce memory usage
2. Adjust `FlushTimeout` for smaller batches
3. Reduce `WorkerCount` if too many goroutines

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `go test ./...`
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- PostgreSQL advisory locks approach from [Postgres documentation](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)

## Support

- GitHub Issues: https://github.com/sklinkert/go-outbox/issues
- Documentation: See package documentation and examples

## Further Reading

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [PostgreSQL Implementation Guide](postgres/README.md)
- [RabbitMQ Example](examples/rabbitmq/README.md)
- [Kafka Example](examples/kafka/README.md)
- [Implementing the Outbox Pattern](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/)
