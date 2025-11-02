# Kafka Example

This example demonstrates how to integrate go-outbox with Apache Kafka for high-throughput message publishing.

## Features

- High-throughput batch publishing optimized for Kafka
- Multiple topic support with automatic grouping
- Transactional message insertion
- Graceful shutdown with in-flight message handling

## Prerequisites

- Go 1.21+
- PostgreSQL 14+
- Apache Kafka 2.8+ (optional for this example)

## Running the Example

### 1. Start PostgreSQL

```bash
docker run --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=outbox_demo \
  -p 5432:5432 \
  -d postgres:18
```

### 2. Start Kafka (Optional)

```bash
# Start Zookeeper
docker run --name zookeeper \
  -p 2181:2181 \
  -d zookeeper:3.8

# Start Kafka
docker run --name kafka \
  -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  --link zookeeper \
  -d confluentinc/cp-kafka:7.4.0
```

### 3. Run the Example

```bash
cd examples/kafka
go run main.go
```

### 4. With Custom Configuration

```bash
DATABASE_URL="postgres://user:pass@localhost/mydb?sslmode=disable" \
KAFKA_BROKERS="localhost:9092" \
go run main.go
```

## What This Example Does

1. **Creates the outbox table** with optimized schema
2. **Starts the outbox processor** with Kafka-optimized settings
3. **Inserts messages across multiple topics** (user.events, order.events, inventory.events)
4. **Groups messages by topic** for optimal batch publishing
5. **Publishes to Kafka** with high throughput

## Kafka-Specific Optimizations

### Batch Size

Kafka excels at batch processing. The example uses a batch size of 200:

```go
config.BatchSize = 200
```

This reduces network round trips and increases throughput.

### Topic Grouping

The publisher groups messages by topic before sending:

```go
topicMessages := make(map[string][]*outbox.Message)
for _, msg := range msgs {
    topicMessages[msg.Topic] = append(topicMessages[msg.Topic], msg)
}
```

This allows Kafka to compress and batch messages per partition more efficiently.

### Flush Timeout

Lower flush timeout (50ms) ensures messages don't wait too long:

```go
config.FlushTimeout = 50 * time.Millisecond
```

## Real-World Implementation

For production use with Kafka, you should:

1. **Use a proper Kafka client**: `github.com/segmentio/kafka-go` or `github.com/confluentinc/confluent-kafka-go`
2. **Configure compression**: Enable snappy or lz4 compression
3. **Set proper acks**: Use `acks=all` for durability
4. **Implement idempotent producer**: Enable `enable.idempotence=true`
5. **Handle retries**: Configure retry backoff and max retries
6. **Use partitioning strategy**: Route messages by key for ordering

### Example with Real Kafka Client (kafka-go)

```go
import (
    "github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
    writers map[string]*kafka.Writer
}

func NewKafkaPublisher(brokers []string) *KafkaPublisher {
    return &KafkaPublisher{
        writers: make(map[string]*kafka.Writer),
    }
}

func (p *KafkaPublisher) getWriter(topic string) *kafka.Writer {
    if writer, exists := p.writers[topic]; exists {
        return writer
    }

    writer := &kafka.Writer{
        Addr:         kafka.TCP(p.brokers...),
        Topic:        topic,
        Balancer:     &kafka.Hash{}, // Partition by key
        Compression:  kafka.Snappy,
        RequiredAcks: kafka.RequireAll,
        MaxAttempts:  10,
        BatchSize:    100,
        BatchTimeout: 10 * time.Millisecond,
    }

    p.writers[topic] = writer
    return writer
}

func (p *KafkaPublisher) PublishBatch(ctx context.Context, msgs []*outbox.Message) error {
    // Group by topic
    topicMessages := make(map[string][]kafka.Message)

    for _, msg := range msgs {
        kafkaMsg := kafka.Message{
            Key:   []byte(msg.IdempotencyKey), // Ensures ordering per key
            Value: msg.Payload,
            Headers: []kafka.Header{
                {Key: "message-id", Value: []byte(msg.Id)},
            },
        }
        topicMessages[msg.Topic] = append(topicMessages[msg.Topic], kafkaMsg)
    }

    // Publish each topic batch
    for topic, messages := range topicMessages {
        writer := p.getWriter(topic)
        err := writer.WriteMessages(ctx, messages...)
        if err != nil {
            return fmt.Errorf("failed to write to topic %s: %w", topic, err)
        }
    }

    return nil
}

func (p *KafkaPublisher) Close() error {
    for _, writer := range p.writers {
        if err := writer.Close(); err != nil {
            return err
        }
    }
    return nil
}
```

### Example with Confluent Kafka Client

```go
import (
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaPublisher struct {
    producer *kafka.Producer
}

func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers":  strings.Join(brokers, ","),
        "acks":               "all",
        "compression.type":   "snappy",
        "enable.idempotence": true,
        "max.in.flight":      5,
        "retries":            10,
    })
    if err != nil {
        return nil, err
    }

    return &KafkaPublisher{producer: producer}, nil
}

func (p *KafkaPublisher) PublishBatch(ctx context.Context, msgs []*outbox.Message) error {
    deliveryChan := make(chan kafka.Event, len(msgs))

    // Send all messages
    for _, msg := range msgs {
        err := p.producer.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{
                Topic:     &msg.Topic,
                Partition: kafka.PartitionAny,
            },
            Key:   []byte(msg.IdempotencyKey),
            Value: msg.Payload,
            Headers: []kafka.Header{
                {Key: "message-id", Value: []byte(msg.Id)},
            },
        }, deliveryChan)

        if err != nil {
            return err
        }
    }

    // Wait for delivery confirmations
    for i := 0; i < len(msgs); i++ {
        e := <-deliveryChan
        m := e.(*kafka.Message)

        if m.TopicPartition.Error != nil {
            return m.TopicPartition.Error
        }
    }

    return nil
}

func (p *KafkaPublisher) Close() error {
    p.producer.Flush(15 * 1000) // 15 seconds
    p.producer.Close()
    return nil
}
```

## Performance Tuning

### High Throughput Configuration

For maximum throughput:

```go
config := outbox.DefaultConfig()
config.BatchSize = 500           // Large batches
config.PollInterval = 100 * time.Millisecond  // Frequent polling
config.FlushTimeout = 50 * time.Millisecond   // Quick flush
config.WorkerCount = 10          // More workers
```

### Low Latency Configuration

For minimum latency:

```go
config := outbox.DefaultConfig()
config.BatchSize = 50            // Smaller batches
config.PollInterval = 100 * time.Millisecond  // Frequent polling
config.FlushTimeout = 10 * time.Millisecond   // Very quick flush
config.WorkerCount = 20          // Many workers
```

### Balanced Configuration

For general use:

```go
config := outbox.DefaultConfig()
config.BatchSize = 100
config.PollInterval = 1 * time.Second
config.FlushTimeout = 100 * time.Millisecond
config.WorkerCount = 5
```

## Monitoring

Track these Kafka-specific metrics:

- **Throughput**: Messages/second published
- **Batch size distribution**: Average messages per batch
- **Topic distribution**: Messages per topic
- **Kafka errors**: Failed sends, timeouts
- **Lag**: Time from message creation to publish

## Message Ordering

Kafka guarantees ordering within a partition. To maintain ordering:

1. **Use message keys**: Set the partition key to route related messages
2. **Single partition**: Use the same key for all messages that need ordering
3. **IdempotencyKey as key**: Use the idempotency key as the Kafka key

```go
// Ensures all order events for order-123 go to same partition
IdempotencyKey: "order-123-created"
```

## Dead Letter Queue

For messages that fail after max retries:

```go
// In your processor loop
if msg.Attempts >= config.MaxRetries {
    // Publish to DLQ topic
    dlqMsg := &outbox.Message{
        Id:     generateMessageId(),
        Topic:  msg.Topic + ".dlq",
        Payload: msg.Payload,
        Headers: map[string]string{
            "original-topic": msg.Topic,
            "failure-reason": msg.LastError,
        },
        IdempotencyKey: msg.IdempotencyKey + "-dlq",
        CreatedAt:      time.Now(),
    }
    // Insert DLQ message
}
```

## Troubleshooting

### High lag

- Increase `BatchSize` for higher throughput
- Increase `WorkerCount` for more parallelism
- Check Kafka broker performance
- Verify network connectivity

### Message duplication

- Ensure idempotent producer is enabled
- Verify idempotency key uniqueness
- Check consumer-side deduplication

### Memory pressure

- Decrease `BatchSize`
- Adjust `FlushTimeout` for smaller batches
- Monitor Go heap and GC metrics

## Testing

Test without Kafka:

```bash
go run main.go
```

The example will simulate Kafka publishing and log all operations.

Test with real Kafka:

1. Implement the real Kafka client as shown above
2. Start Kafka
3. Create topics: `kafka-topics --create --topic user.events --bootstrap-server localhost:9092`
4. Run the example
5. Consume messages: `kafka-console-consumer --topic user.events --from-beginning --bootstrap-server localhost:9092`

## Further Reading

- [Kafka Producer Documentation](https://kafka.apache.org/documentation/#producerapi)
- [kafka-go Library](https://github.com/segmentio/kafka-go)
- [Confluent Kafka Go Client](https://github.com/confluentinc/confluent-kafka-go)
- [Kafka Performance Tuning](https://kafka.apache.org/documentation/#producerconfigs)
