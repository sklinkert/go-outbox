package outbox

import (
	"errors"
	"testing"
)

func TestErrInvalidConfig(t *testing.T) {
	err := ErrInvalidConfig{
		Field:  "BatchSize",
		Reason: "must be greater than 0",
	}

	expected := "invalid config field BatchSize: must be greater than 0"
	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}
}

func TestErrProcessorStopped(t *testing.T) {
	err := ErrProcessorStopped{}
	expected := "processor is stopped"

	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}
}

func TestErrShutdownTimeout(t *testing.T) {
	err := ErrShutdownTimeout{
		Timeout: "30s",
	}

	expected := "shutdown timeout exceeded: 30s"
	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}
}

func TestErrMaxRetriesExceeded(t *testing.T) {
	err := ErrMaxRetriesExceeded{
		MessageId:  "msg-123",
		Attempts:   10,
		MaxRetries: 10,
	}

	if err.MessageId != "msg-123" {
		t.Errorf("expected MessageId msg-123, got %s", err.MessageId)
	}

	if err.Attempts != 10 {
		t.Errorf("expected Attempts 10, got %d", err.Attempts)
	}
}

func TestErrBatchPublishPartialFailure(t *testing.T) {
	err := ErrBatchPublishPartialFailure{
		Total:      10,
		Successful: 7,
		Failed:     3,
		Errors:     []error{errors.New("error1"), errors.New("error2")},
	}

	expected := "batch publish partial failure: 7/10 succeeded, 3 failed"
	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}
}

func TestErrStoreOperation(t *testing.T) {
	innerErr := errors.New("connection refused")
	err := ErrStoreOperation{
		Operation: "FetchPending",
		Err:       innerErr,
	}

	expected := "store operation FetchPending failed: connection refused"
	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}

	// Test Unwrap
	unwrapped := err.Unwrap()
	if unwrapped != innerErr {
		t.Errorf("expected unwrapped error to be %v, got %v", innerErr, unwrapped)
	}
}

func TestErrPublishOperation(t *testing.T) {
	innerErr := errors.New("broker unavailable")
	err := ErrPublishOperation{
		MessageId: "msg-123",
		Topic:     "test.topic",
		Err:       innerErr,
	}

	expected := "publish operation failed for message msg-123 (topic: test.topic): broker unavailable"
	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}

	// Test Unwrap
	unwrapped := err.Unwrap()
	if unwrapped != innerErr {
		t.Errorf("expected unwrapped error to be %v, got %v", innerErr, unwrapped)
	}
}
