// Package outbox provides a production-ready implementation of the transactional outbox pattern.
//
// The outbox pattern ensures reliable message delivery by storing messages in a database
// within the same transaction as business data, then asynchronously polling and publishing
// them to message brokers. This guarantees at-least-once delivery semantics.
package outbox

import (
	"context"
	"time"
)

// Message represents an outbox message to be published.
type Message struct {
	// Id is the unique identifier for the message (e.g., UUIDv7)
	Id string

	// Topic is the destination topic/queue/exchange
	Topic string

	// Payload is the message body (JSON, protobuf, etc.)
	Payload []byte

	// Headers contains metadata for the message
	Headers map[string]string

	// IdempotencyKey ensures duplicate detection by consumers
	IdempotencyKey string

	// CreatedAt is when the message was created
	CreatedAt time.Time

	// ScheduledAt allows delayed message publishing (nil = immediate)
	ScheduledAt *time.Time

	// Attempts tracks how many times publishing was attempted
	Attempts int

	// LastError stores the most recent error message
	LastError string

	// ProcessedAt indicates when the message was successfully published
	ProcessedAt *time.Time
}

// MessageFailure represents a failed message publishing attempt.
type MessageFailure struct {
	MessageId string
	Error     string
	Attempts  int
}

// Store defines the interface for persisting and retrieving outbox messages.
// Implementations must ensure all operations are transactional and thread-safe.
type Store interface {
	// FetchPending retrieves up to batchSize unprocessed messages.
	// Messages should be ordered by creation time and respect ScheduledAt.
	// Implementation should use database locks (e.g., advisory locks) to prevent
	// concurrent processors from fetching the same messages.
	FetchPending(ctx context.Context, batchSize int) ([]*Message, error)

	// MarkSent marks messages as successfully published within a transaction.
	// This operation must be idempotent.
	MarkSent(ctx context.Context, messageIds []string) error

	// MarkFailed records failed publishing attempts with error details.
	// Implementation should update attempt count and last error message.
	MarkFailed(ctx context.Context, failures []MessageFailure) error

	// Insert adds new messages to the outbox within the caller's transaction.
	// This is typically called by business logic when creating domain events.
	Insert(ctx context.Context, messages []*Message) error
}

// ProcessorLockStore extends Store with session-level locking capabilities.
// This is an optional interface that stores can implement to support single-processor mode.
type ProcessorLockStore interface {
	Store

	// AcquireProcessorLock acquires a session-level lock (blocking).
	// Only one processor instance can hold this lock at a time.
	AcquireProcessorLock(ctx context.Context) error

	// TryAcquireProcessorLock attempts to acquire the lock without blocking.
	// Returns true if acquired, false if another instance holds it.
	TryAcquireProcessorLock(ctx context.Context) (bool, error)

	// ReleaseProcessorLock releases the session-level lock.
	ReleaseProcessorLock(ctx context.Context) error

	// HasProcessorLock returns true if this instance holds the lock.
	HasProcessorLock() bool

	// IsProcessorLockEnabled returns true if locking is enabled.
	IsProcessorLockEnabled() bool
}

// Publisher defines the interface for publishing a single message to a message broker.
type Publisher interface {
	// Publish sends a message to the configured broker.
	// Returns an error if publishing fails.
	Publish(ctx context.Context, msg *Message) error

	// Close releases any resources held by the publisher.
	Close() error
}

// BatchPublisher defines the interface for high-throughput batch publishing.
// Implementations can publish multiple messages in a single broker operation.
type BatchPublisher interface {
	Publisher

	// PublishBatch sends multiple messages in a single operation.
	// Implementations should handle partial failures by returning an error
	// that contains details about which messages failed.
	PublishBatch(ctx context.Context, msgs []*Message) error
}

// Logger defines the interface for logging within the outbox processor.
// This allows integration with any logging framework (logrus, zap, slog, etc.).
type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warn(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
}

// MetricsHook provides hooks for observability and monitoring.
type MetricsHook interface {
	// OnMessagesFetched is called after successfully fetching messages.
	OnMessagesFetched(count int)

	// OnMessagesPublished is called after successfully publishing messages.
	OnMessagesPublished(count int, duration time.Duration)

	// OnMessagesFailed is called when message publishing fails.
	OnMessagesFailed(count int)

	// OnPollError is called when polling for messages fails.
	OnPollError(err error)

	// OnPublishError is called when publishing fails.
	OnPublishError(err error)
}

// Config contains configuration for the outbox processor.
type Config struct {
	// PollInterval is how often to check for new messages
	PollInterval time.Duration

	// BatchSize is the maximum number of messages to fetch and publish at once
	BatchSize int

	// MaxRetries is the maximum number of publishing attempts per message
	MaxRetries int

	// RetryBackoff is the base duration for exponential backoff between retries
	RetryBackoff time.Duration

	// FlushTimeout is the maximum time to wait before publishing a partial batch
	FlushTimeout time.Duration

	// WorkerCount is the number of concurrent workers for publishing
	WorkerCount int

	// ShutdownTimeout is the maximum time to wait for graceful shutdown
	ShutdownTimeout time.Duration

	// ProcessorLockKey enables single-processor mode using PostgreSQL advisory locks.
	// When set to a non-zero value, only one processor instance can run at a time,
	// ensuring strict FIFO message ordering and preventing race conditions.
	// Set to 0 to disable (allows multiple concurrent processors with per-message locking).
	// Recommended: Use hashtext('your-app-name') to generate a consistent key.
	// Example: SELECT hashtext('myapp-outbox-processor'::text) -- returns an int64
	ProcessorLockKey int64

	// Logger for structured logging (optional)
	Logger Logger

	// MetricsHook for observability (optional)
	MetricsHook MetricsHook
}

// DefaultConfig returns a Config with production-ready defaults.
func DefaultConfig() Config {
	return Config{
		PollInterval:     1 * time.Second,
		BatchSize:        100,
		MaxRetries:       10,
		RetryBackoff:     5 * time.Second,
		FlushTimeout:     100 * time.Millisecond,
		WorkerCount:      5,
		ShutdownTimeout:  30 * time.Second,
		ProcessorLockKey: 0, // Disabled by default for backward compatibility
		Logger:           &noopLogger{},
		MetricsHook:      &noopMetricsHook{},
	}
}

// Validate checks if the configuration is valid.
func (c Config) Validate() error {
	if c.PollInterval <= 0 {
		return ErrInvalidConfig{Field: "PollInterval", Reason: "must be greater than 0"}
	}
	if c.BatchSize <= 0 {
		return ErrInvalidConfig{Field: "BatchSize", Reason: "must be greater than 0"}
	}
	if c.MaxRetries < 0 {
		return ErrInvalidConfig{Field: "MaxRetries", Reason: "must be non-negative"}
	}
	if c.RetryBackoff <= 0 {
		return ErrInvalidConfig{Field: "RetryBackoff", Reason: "must be greater than 0"}
	}
	if c.FlushTimeout <= 0 {
		return ErrInvalidConfig{Field: "FlushTimeout", Reason: "must be greater than 0"}
	}
	if c.WorkerCount <= 0 {
		return ErrInvalidConfig{Field: "WorkerCount", Reason: "must be greater than 0"}
	}
	if c.ShutdownTimeout <= 0 {
		return ErrInvalidConfig{Field: "ShutdownTimeout", Reason: "must be greater than 0"}
	}
	return nil
}

// noopLogger is a no-operation logger used when no logger is configured.
type noopLogger struct{}

func (l *noopLogger) Debug(msg string, fields map[string]interface{}) {}
func (l *noopLogger) Info(msg string, fields map[string]interface{})  {}
func (l *noopLogger) Warn(msg string, fields map[string]interface{})  {}
func (l *noopLogger) Error(msg string, fields map[string]interface{}) {}

// noopMetricsHook is a no-operation metrics hook used when no hook is configured.
type noopMetricsHook struct{}

func (m *noopMetricsHook) OnMessagesFetched(count int)                           {}
func (m *noopMetricsHook) OnMessagesPublished(count int, duration time.Duration) {}
func (m *noopMetricsHook) OnMessagesFailed(count int)                            {}
func (m *noopMetricsHook) OnPollError(err error)                                 {}
func (m *noopMetricsHook) OnPublishError(err error)                              {}
