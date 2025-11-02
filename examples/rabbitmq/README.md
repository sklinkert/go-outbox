# RabbitMQ Example

This example demonstrates how to integrate go-outbox with RabbitMQ for reliable message publishing.

## Features

- Transactional message insertion with business logic
- Batch publishing to RabbitMQ
- Custom logger integration
- Metrics hooks for observability
- Graceful shutdown handling

## Prerequisites

- Go 1.21+
- PostgreSQL 14+
- RabbitMQ 3.8+ (optional for this example)

## Running the Example

### 1. Start PostgreSQL

```bash
docker run --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=outbox_demo \
  -p 5432:5432 \
  -d postgres:18
```

### 2. Start RabbitMQ (Optional)

```bash
docker run --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  -d rabbitmq:3-management
```

### 3. Run the Example

```bash
cd examples/rabbitmq
go run main.go
```

### 4. With Custom Configuration

```bash
DATABASE_URL="postgres://user:pass@localhost/mydb?sslmode=disable" \
RABBITMQ_URL="amqp://guest:guest@localhost:5672/" \
go run main.go
```

## What This Example Does

1. **Creates the outbox table** with proper schema and indices
2. **Starts the outbox processor** with custom configuration
3. **Inserts example messages** within database transactions
4. **Publishes messages to RabbitMQ** using batch operations
5. **Handles graceful shutdown** on SIGINT/SIGTERM

## Code Structure

### RabbitMQPublisher

Implements both `outbox.Publisher` and `outbox.BatchPublisher` interfaces:

```go
type RabbitMQPublisher struct {
    url      string
    exchange string
}

func (p *RabbitMQPublisher) Publish(ctx context.Context, msg *outbox.Message) error {
    // Publish single message
}

func (p *RabbitMQPublisher) PublishBatch(ctx context.Context, msgs []*outbox.Message) error {
    // Publish multiple messages for higher throughput
}
```

### Transaction Pattern

Messages are inserted atomically with business data:

```go
tx, err := db.BeginTx(ctx, nil)
// ... your business logic ...

messages := []*outbox.Message{...}
ctx = postgres.WithTx(ctx, tx)
store.Insert(ctx, messages)

tx.Commit()
```

## Real-World Implementation

For production use, you should:

1. **Use the official RabbitMQ client**: `github.com/rabbitmq/amqp091-go`
2. **Implement connection pooling**: Reuse channels across publishes
3. **Enable publisher confirms**: Ensure reliable delivery
4. **Handle connection failures**: Implement reconnection logic
5. **Use proper logging**: Integrate with logrus, zap, or slog
6. **Add metrics**: Integrate with Prometheus, StatsD, etc.

### Example with Real RabbitMQ Client

```go
import (
    amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQPublisher struct {
    conn    *amqp.Connection
    channel *amqp.Channel
}

func (p *RabbitMQPublisher) PublishBatch(ctx context.Context, msgs []*outbox.Message) error {
    // Enable publisher confirms
    if err := p.channel.Confirm(false); err != nil {
        return err
    }

    confirms := p.channel.NotifyPublish(make(chan amqp.Confirmation, len(msgs)))

    for _, msg := range msgs {
        err := p.channel.PublishWithContext(
            ctx,
            "events",        // exchange
            msg.Topic,       // routing key
            false,           // mandatory
            false,           // immediate
            amqp.Publishing{
                ContentType:  msg.Headers["content-type"],
                Body:         msg.Payload,
                DeliveryMode: amqp.Persistent,
                MessageId:    msg.Id,
                Headers:      convertHeaders(msg.Headers),
            },
        )
        if err != nil {
            return err
        }
    }

    // Wait for confirms
    for i := 0; i < len(msgs); i++ {
        confirmed := <-confirms
        if !confirmed.Ack {
            return fmt.Errorf("message not confirmed")
        }
    }

    return nil
}
```

## Monitoring

The example includes a custom metrics hook that logs:

- Messages fetched per poll
- Messages published (with duration)
- Failed messages
- Poll errors
- Publish errors

In production, send these metrics to your monitoring system:

```go
type PrometheusMetrics struct {
    messagesFetched   prometheus.Counter
    messagesPublished prometheus.Histogram
    messagesFailed    prometheus.Counter
}

func (m *PrometheusMetrics) OnMessagesPublished(count int, duration time.Duration) {
    m.messagesFetched.Add(float64(count))
    m.messagesPublished.Observe(duration.Seconds())
}
```

## Testing

You can test the example without RabbitMQ since the publisher is mocked. To see actual RabbitMQ publishing:

1. Implement the real RabbitMQ client as shown above
2. Start RabbitMQ
3. Open the management UI: http://localhost:15672 (guest/guest)
4. Watch messages appear in the "events" exchange

## Troubleshooting

### Messages not being published

- Check the processor started: Look for "Outbox processor started" log
- Check database connectivity: Verify PostgreSQL is running
- Check the outbox table: `SELECT * FROM outbox_messages WHERE processed_at IS NULL;`

### High latency

- Increase `BatchSize` for higher throughput
- Increase `WorkerCount` for more parallelism
- Decrease `PollInterval` for lower latency

### Memory issues

- Decrease `BatchSize` to reduce memory usage
- Adjust `FlushTimeout` to flush smaller batches more frequently
