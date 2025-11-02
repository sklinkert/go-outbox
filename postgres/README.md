# PostgreSQL Outbox Implementation

This package provides a production-ready PostgreSQL implementation of the transactional outbox pattern.

## Features

- **Advisory Locks**: Prevents duplicate message processing across multiple instances
- **Partial Indices**: High-performance queries for pending messages
- **Soft Delete**: Messages marked as processed, not deleted (enables auditing)
- **Transactional Inserts**: Messages inserted atomically with business data
- **JSONB Headers**: Flexible metadata storage with indexing support
- **Scheduled Messages**: Delayed message publishing
- **Prepared Statements**: Optimized query performance through statement reuse

## Quick Start

### 1. Create the Database Schema

Run this SQL to create the outbox table:

```sql
-- Outbox messages table
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

-- Partial index for unprocessed messages (most common query)
-- This dramatically improves fetch performance by only indexing pending messages
CREATE INDEX idx_outbox_messages_pending
ON outbox_messages (created_at)
WHERE processed_at IS NULL;

-- Partial index for scheduled messages
CREATE INDEX idx_outbox_messages_scheduled
ON outbox_messages (scheduled_at)
WHERE processed_at IS NULL AND scheduled_at IS NOT NULL;

-- Index for topic-based queries (useful for monitoring/debugging)
CREATE INDEX idx_outbox_messages_topic
ON outbox_messages (topic);

-- Index for processed_at (useful for cleanup/archival)
CREATE INDEX idx_outbox_messages_processed_at
ON outbox_messages (processed_at)
WHERE processed_at IS NOT NULL;

-- JSONB index for headers (if you need to query by header values)
CREATE INDEX idx_outbox_messages_headers
ON outbox_messages USING GIN (headers);

-- Table and column comments
COMMENT ON TABLE outbox_messages IS 'Transactional outbox pattern for reliable message delivery';
COMMENT ON COLUMN outbox_messages.id IS 'Unique message identifier (e.g., UUIDv7 for time ordering)';
COMMENT ON COLUMN outbox_messages.topic IS 'Destination topic/queue/exchange';
COMMENT ON COLUMN outbox_messages.payload IS 'Message body (JSON, protobuf, etc.)';
COMMENT ON COLUMN outbox_messages.headers IS 'Message metadata as JSONB';
COMMENT ON COLUMN outbox_messages.idempotency_key IS 'Ensures duplicate detection by consumers';
COMMENT ON COLUMN outbox_messages.created_at IS 'Message creation timestamp';
COMMENT ON COLUMN outbox_messages.scheduled_at IS 'Delayed publishing timestamp (NULL = immediate)';
COMMENT ON COLUMN outbox_messages.attempts IS 'Number of publishing attempts';
COMMENT ON COLUMN outbox_messages.last_error IS 'Most recent error message';
COMMENT ON COLUMN outbox_messages.processed_at IS 'Successful publishing timestamp (soft delete)';
```

Or execute from your application:

```go
import (
    "database/sql"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", "postgres://user:pass@localhost/dbname?sslmode=disable")
if err != nil {
    log.Fatal(err)
}

// Copy the schema SQL from above and execute it
_, err = db.Exec(schemaSQL)
if err != nil {
    log.Fatal(err)
}
```

### 2. Initialize the Store

```go
// Method 1: With processor lock (recommended for production)
// Generate lock key: SELECT hashtext('myapp-outbox'::text)
lockKey := int64(123456789) // Use consistent value across instances
store, err := postgres.NewStore(db, "outbox_messages", lockKey)
if err != nil {
    log.Fatal(err)
}
defer store.Close() // Close prepared statements and release lock

// Method 2: Without processor lock (for development or high-throughput scenarios)
store, err := postgres.NewStore(db, "outbox_messages", 0)
if err != nil {
    log.Fatal(err)
}
defer store.Close()
```

### 3. Insert Messages within Transactions

```go
import (
    "context"
    "time"
    "github.com/sklinkert/go-outbox"
    "github.com/sklinkert/go-outbox/postgres"
)

// Start a transaction for your business logic
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

// Your business logic here...
// e.g., insert order, update inventory, etc.

// Create outbox messages
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

// Insert messages within the same transaction
ctx = postgres.WithTx(ctx, tx)
err = store.Insert(ctx, messages)
if err != nil {
    return err
}

// Commit the transaction
return tx.Commit()
```

### 4. Start the Processor

```go
import (
    "github.com/sklinkert/go-outbox"
    "github.com/sklinkert/go-outbox/postgres"
)

// Your publisher implementation (RabbitMQ, Kafka, etc.)
publisher := &MyPublisher{}

// Create processor with config including lock key
config := outbox.DefaultConfig()
config.ProcessorLockKey = lockKey // Must match store's lockKey
processor, err := outbox.NewProcessor(store, publisher, config)
if err != nil {
    log.Fatal(err)
}

// Start processing (acquires processor lock automatically)
if err := processor.Start(); err != nil {
    if errors.Is(err, outbox.ErrProcessorLockHeld{}) {
        log.Println("Another processor instance is running, this instance will be standby")
        // Retry logic here for automatic failover
    } else {
        log.Fatal(err)
    }
}

// Graceful shutdown (releases lock automatically)
defer processor.Stop()
```

## Database Schema

The table structure is optimized for high-throughput message processing:

```sql
CREATE TABLE outbox_messages (
    id TEXT PRIMARY KEY,                -- UUIDv7 recommended
    topic TEXT NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL,
    scheduled_at TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    processed_at TIMESTAMP             -- NULL = pending, NOT NULL = processed
);
```

## Indices

The implementation uses strategic indices for optimal performance:

### 1. Partial Index for Pending Messages

```sql
CREATE INDEX idx_outbox_messages_pending ON outbox_messages (created_at)
WHERE processed_at IS NULL;
```

**Why**: This is the most frequently queried condition. A partial index is much smaller and faster than a full table index.

### 2. Partial Index for Scheduled Messages

```sql
CREATE INDEX idx_outbox_messages_scheduled ON outbox_messages (scheduled_at)
WHERE processed_at IS NULL AND scheduled_at IS NOT NULL;
```

**Why**: Efficiently queries messages ready for delayed publishing.

### 3. Topic Index

```sql
CREATE INDEX idx_outbox_messages_topic ON outbox_messages (topic);
```

**Why**: Useful for monitoring, debugging, and topic-specific queries.

### 4. Processed Messages Index

```sql
CREATE INDEX idx_outbox_messages_processed_at ON outbox_messages (processed_at)
WHERE processed_at IS NOT NULL;
```

**Why**: Enables efficient archival and cleanup of old messages.

### 5. GIN Index for JSONB Headers

```sql
CREATE INDEX idx_outbox_messages_headers ON outbox_messages USING GIN (headers);
```

**Why**: Supports fast queries on header metadata.

## Locking Mechanisms

### Single-Processor Mode (Recommended)

The implementation supports **session-level advisory locks** to ensure only one processor instance runs at a time. This provides:

✅ **Strict FIFO ordering** - Messages processed in creation order
✅ **No race conditions** - Single processor eliminates concurrency issues
✅ **Automatic failover** - Lock released on crash/disconnect
✅ **Simple and safe** - Production-ready pattern

**How it works**:
```go
// Generate a consistent lock key for your application
// Method 1: Calculate in PostgreSQL
// SELECT hashtext('myapp-outbox-processor'::text); -- returns int64

// Method 2: Use a fixed number
lockKey := int64(123456789)

// Create store with lock enabled
store, err := postgres.NewStore(db, "outbox_messages", lockKey)

// Lock is acquired in processor.Start() automatically
// Only one processor instance across all servers can run at a time
```

The session-level lock (`pg_advisory_lock`) ensures that:
1. Only one processor instance can hold the lock at any time
2. Lock is automatically released when the connection closes (crash recovery)
3. Other instances wait or fail-fast depending on configuration
4. Strict FIFO message ordering is maintained

**Monitoring**:
```sql
-- Check which instance holds the lock
SELECT * FROM pg_locks
WHERE locktype = 'advisory'
  AND classid = 0;
```

### Per-Message Locking (Advanced)

When session-level locking is disabled (`lockKey = 0`), the implementation falls back to per-message advisory locks:

```sql
WHERE pg_try_advisory_xact_lock(hashtext(id))
```

**How it works**:
1. Each message ID is hashed to a numeric lock identifier
2. `pg_try_advisory_xact_lock` acquires a transaction-level lock
3. Lock is automatically released when the transaction commits/rolls back
4. Other processors skip locked messages

**Trade-offs**:
- ✅ Allows multiple concurrent processors
- ✅ Higher throughput potential
- ⚠️ Weaker ordering guarantees (best-effort FIFO)
- ⚠️ More complex to reason about

**Note**: Per-message locking is only recommended for advanced use cases where horizontal scaling is more important than strict ordering.

## Performance Considerations

### UUIDv7 vs UUID4

UUIDv7 is **strongly recommended** for the `id` column:

- **Time-ordered**: Natural clustering improves index efficiency
- **Write performance**: Reduces index fragmentation
- **Read performance**: Better cache locality for recent messages

Example UUIDv7 generation in Go:

```go
import "github.com/google/uuid"

// With google/uuid v1.4.0+
id := uuid.Must(uuid.NewV7()).String()
```

For PostgreSQL 18+:

```sql
SELECT gen_random_uuid()::TEXT; -- Native UUIDv7 support
```

### Batch Operations

All operations support batching for maximum throughput:

```go
// Fetch up to 100 messages at once
messages, err := store.FetchPending(ctx, 100)

// Mark multiple messages as sent in a single query
messageIds := []string{"id1", "id2", "id3"}
err = store.MarkSent(ctx, messageIds)

// Record multiple failures at once
failures := []outbox.MessageFailure{
    {MessageId: "id1", Error: "timeout", Attempts: 1},
    {MessageId: "id2", Error: "connection refused", Attempts: 2},
}
err = store.MarkFailed(ctx, failures)
```

### Archival Strategy

Since the implementation uses soft delete (`processed_at IS NOT NULL`), you should periodically archive old messages:

```sql
-- Archive messages older than 30 days
INSERT INTO outbox_messages_archive
SELECT * FROM outbox_messages
WHERE processed_at < NOW() - INTERVAL '30 days';

-- Delete archived messages
DELETE FROM outbox_messages
WHERE processed_at < NOW() - INTERVAL '30 days';
```

## Monitoring Queries

### Count Pending Messages

```sql
SELECT COUNT(*) FROM outbox_messages WHERE processed_at IS NULL;
```

### Messages by Topic

```sql
SELECT topic, COUNT(*)
FROM outbox_messages
WHERE processed_at IS NULL
GROUP BY topic;
```

### Failed Messages

```sql
SELECT id, topic, attempts, last_error
FROM outbox_messages
WHERE processed_at IS NULL AND attempts > 0
ORDER BY attempts DESC
LIMIT 100;
```

### Processing Throughput

```sql
SELECT
    DATE_TRUNC('minute', processed_at) AS minute,
    COUNT(*) AS messages_processed
FROM outbox_messages
WHERE processed_at > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute;
```

## Troubleshooting

### Messages Not Being Processed

1. **Check processor is running**: Verify `processor.Start()` was called
2. **Check scheduled_at**: Messages may be scheduled for future delivery
3. **Check max retries**: Messages may have exceeded retry limit
4. **Check advisory locks**: Ensure no stale locks (restart PostgreSQL if needed)

### Slow Query Performance

1. **Verify indices exist**: Run `\d outbox_messages` in psql
2. **Analyze statistics**: `ANALYZE outbox_messages;`
3. **Check index usage**: Use `EXPLAIN ANALYZE` on the fetch query
4. **Increase batch size**: Larger batches = fewer queries

### High Database Load

1. **Reduce poll interval**: Less frequent polling reduces load
2. **Use connection pooling**: Share connections across processors
3. **Archive old messages**: Large tables slow down all queries
4. **Tune PostgreSQL**: Adjust `work_mem`, `shared_buffers`, etc.

## Migration Guide

If you're migrating from an existing outbox implementation:

### From Integer IDs to UUIDv7

```sql
-- Add new column
ALTER TABLE outbox_messages ADD COLUMN id_new TEXT;

-- Generate UUIDv7 for existing rows
UPDATE outbox_messages SET id_new = gen_random_uuid()::TEXT;

-- Swap columns
ALTER TABLE outbox_messages DROP CONSTRAINT outbox_messages_pkey;
ALTER TABLE outbox_messages DROP COLUMN id;
ALTER TABLE outbox_messages RENAME COLUMN id_new TO id;
ALTER TABLE outbox_messages ADD PRIMARY KEY (id);
```

### From Hard Delete to Soft Delete

```sql
-- Add processed_at column
ALTER TABLE outbox_messages ADD COLUMN processed_at TIMESTAMP;

-- Existing processed messages won't be picked up
-- since they're already deleted
```

## Running Multiple Instances

### Single-Processor Mode (High Availability)

When using session-level advisory locks, only one processor runs at a time, but you can deploy multiple instances for automatic failover:

```go
// Configure lock key in all instances
config := outbox.DefaultConfig()
config.ProcessorLockKey = 123456789 // Same key across all instances

// Instance 1
store1, err := postgres.NewStore(db1, "outbox_messages", config.ProcessorLockKey)
processor1, err := outbox.NewProcessor(store1, publisher1, config)
processor1.Start() // Acquires lock, starts processing

// Instance 2 (standby)
store2, err := postgres.NewStore(db2, "outbox_messages", config.ProcessorLockKey)
processor2, err := outbox.NewProcessor(store2, publisher2, config)
processor2.Start() // Returns ErrProcessorLockHeld - instance waits/retries

// When Instance 1 crashes or stops, Instance 2 can acquire the lock
```

**Deployment pattern**:
1. Deploy 2-3 processor instances
2. First instance acquires lock and processes messages
3. Other instances fail-fast or retry periodically
4. On primary failure, standby automatically takes over
5. Zero manual intervention required

**Health check example**:
```go
func healthCheck(store *postgres.Store) bool {
    return store.HasProcessorLock()
}
```

### Multi-Processor Mode (High Throughput)

For high-throughput scenarios where strict ordering is not critical, disable the session lock:

```go
// Disable session lock (lockKey = 0)
store, err := postgres.NewStore(db, "outbox_messages", 0)

// Now multiple processors can run concurrently
// Each processor fetches different messages using per-message locks
```

**Trade-offs**:
- ✅ Higher throughput (parallel processing)
- ✅ Better resource utilization
- ⚠️ Best-effort ordering (not strict FIFO)
- ⚠️ More complex error scenarios

## Best Practices

1. **Always use transactions**: Insert outbox messages atomically with business data
2. **Use idempotency keys**: Prevents duplicate message processing
3. **Enable session-level locking**: Use `ProcessorLockKey` for strict ordering and safety
4. **Monitor failed messages**: Set up alerts for messages exceeding retry limits
5. **Archive regularly**: Keep the table size manageable for optimal performance
6. **Use UUIDv7**: Time-ordered IDs improve index performance
7. **Batch operations**: Process multiple messages at once for higher throughput
8. **Configure timeouts**: Set reasonable database timeouts to prevent hung queries
9. **Connection pooling**: Use `sql.DB` connection pooling for concurrent processors
10. **Deploy multiple instances**: Use 2-3 instances for automatic failover (with session lock enabled)

## PostgreSQL Version Requirements

- **Minimum**: PostgreSQL 12 (for advisory locks and JSONB)
- **Recommended**: PostgreSQL 18+ (for native UUIDv7 support)
- **Tested**: PostgreSQL 14, 15, 16, 17, 18

## Further Reading

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [PostgreSQL Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- [UUIDv7 Specification](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format)
