package outbox

import "fmt"

// ErrInvalidConfig is returned when the configuration is invalid.
type ErrInvalidConfig struct {
	Field  string
	Reason string
}

func (e ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid config field %s: %s", e.Field, e.Reason)
}

// ErrProcessorStopped is returned when operations are attempted on a stopped processor.
type ErrProcessorStopped struct{}

func (e ErrProcessorStopped) Error() string {
	return "processor is stopped"
}

// ErrShutdownTimeout is returned when graceful shutdown exceeds the timeout.
type ErrShutdownTimeout struct {
	Timeout string
}

func (e ErrShutdownTimeout) Error() string {
	return fmt.Sprintf("shutdown timeout exceeded: %s", e.Timeout)
}

// ErrMaxRetriesExceeded is returned when a message exceeds maximum retry attempts.
type ErrMaxRetriesExceeded struct {
	MessageId  string
	Attempts   int
	MaxRetries int
}

func (e ErrMaxRetriesExceeded) Error() string {
	return fmt.Sprintf("message %s exceeded max retries: %d/%d", e.MessageId, e.Attempts, e.MaxRetries)
}

// ErrBatchPublishPartialFailure indicates some messages in a batch failed to publish.
type ErrBatchPublishPartialFailure struct {
	Total      int
	Successful int
	Failed     int
	Errors     []error
}

func (e ErrBatchPublishPartialFailure) Error() string {
	return fmt.Sprintf("batch publish partial failure: %d/%d succeeded, %d failed", e.Successful, e.Total, e.Failed)
}

// ErrStoreOperation wraps store-level errors with additional context.
type ErrStoreOperation struct {
	Operation string
	Err       error
}

func (e ErrStoreOperation) Error() string {
	return fmt.Sprintf("store operation %s failed: %v", e.Operation, e.Err)
}

func (e ErrStoreOperation) Unwrap() error {
	return e.Err
}

// ErrPublishOperation wraps publisher-level errors with additional context.
type ErrPublishOperation struct {
	MessageId string
	Topic     string
	Err       error
}

func (e ErrPublishOperation) Error() string {
	return fmt.Sprintf("publish operation failed for message %s (topic: %s): %v", e.MessageId, e.Topic, e.Err)
}

func (e ErrPublishOperation) Unwrap() error {
	return e.Err
}
