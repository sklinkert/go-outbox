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
    attempts INTEGER NOT NULL,
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
store, err := postgres.NewStore(db, "outbox_messages")
if err != nil {
    log.Fatal(err)
}
defer store.Close() // Close prepared statements when done
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

// Create processor with default config
config := outbox.DefaultConfig()
processor, err := outbox.NewProcessor(store, publisher, config)
if err != nil {
    log.Fatal(err)
}

// Start processing
if err := processor.Start(); err != nil {
    log.Fatal(err)
}

// Graceful shutdown
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

## Advisory Locks

The implementation uses PostgreSQL advisory locks to prevent duplicate processing:

```sql
WHERE pg_try_advisory_xact_lock(hashtext(id))
```

**How it works**:
1. Each message ID is hashed to a numeric lock identifier
2. `pg_try_advisory_xact_lock` acquires a transaction-level lock
3. Lock is automatically released when the transaction commits/rolls back
4. Other processors skip locked messages

**Benefits**:
- No row-level locking overhead
- Automatic cleanup on transaction end
- Zero contention between different messages

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

## Best Practices

1. **Always use transactions**: Insert outbox messages atomically with business data
2. **Use idempotency keys**: Prevents duplicate message processing
3. **Monitor failed messages**: Set up alerts for messages exceeding retry limits
4. **Archive regularly**: Keep the table size manageable for optimal performance
5. **Use UUIDv7**: Time-ordered IDs improve index performance
6. **Batch operations**: Process multiple messages at once for higher throughput
7. **Configure timeouts**: Set reasonable database timeouts to prevent hung queries
8. **Connection pooling**: Use `sql.DB` connection pooling for concurrent processors

## PostgreSQL Version Requirements

- **Minimum**: PostgreSQL 12 (for advisory locks and JSONB)
- **Recommended**: PostgreSQL 18+ (for native UUIDv7 support)
- **Tested**: PostgreSQL 14, 15, 16, 17, 18

## Further Reading

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [PostgreSQL Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- [UUIDv7 Specification](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format)
