package outbox

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.PollInterval != 1*time.Second {
		t.Errorf("expected PollInterval 1s, got %v", config.PollInterval)
	}

	if config.BatchSize != 100 {
		t.Errorf("expected BatchSize 100, got %d", config.BatchSize)
	}

	if config.MaxRetries != 10 {
		t.Errorf("expected MaxRetries 10, got %d", config.MaxRetries)
	}

	if config.WorkerCount != 5 {
		t.Errorf("expected WorkerCount 5, got %d", config.WorkerCount)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		expectErr bool
	}{
		{
			name:      "valid config",
			config:    DefaultConfig(),
			expectErr: false,
		},
		{
			name: "invalid poll interval",
			config: Config{
				PollInterval:    0,
				BatchSize:       100,
				MaxRetries:      10,
				RetryBackoff:    5 * time.Second,
				FlushTimeout:    100 * time.Millisecond,
				WorkerCount:     5,
				ShutdownTimeout: 30 * time.Second,
			},
			expectErr: true,
		},
		{
			name: "invalid batch size",
			config: Config{
				PollInterval:    1 * time.Second,
				BatchSize:       0,
				MaxRetries:      10,
				RetryBackoff:    5 * time.Second,
				FlushTimeout:    100 * time.Millisecond,
				WorkerCount:     5,
				ShutdownTimeout: 30 * time.Second,
			},
			expectErr: true,
		},
		{
			name: "invalid max retries",
			config: Config{
				PollInterval:    1 * time.Second,
				BatchSize:       100,
				MaxRetries:      -1,
				RetryBackoff:    5 * time.Second,
				FlushTimeout:    100 * time.Millisecond,
				WorkerCount:     5,
				ShutdownTimeout: 30 * time.Second,
			},
			expectErr: true,
		},
		{
			name: "invalid worker count",
			config: Config{
				PollInterval:    1 * time.Second,
				BatchSize:       100,
				MaxRetries:      10,
				RetryBackoff:    5 * time.Second,
				FlushTimeout:    100 * time.Millisecond,
				WorkerCount:     0,
				ShutdownTimeout: 30 * time.Second,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestMessage(t *testing.T) {
	now := time.Now()
	msg := &Message{
		Id:             "test-id",
		Topic:          "test.topic",
		Payload:        []byte("test payload"),
		Headers:        map[string]string{"key": "value"},
		IdempotencyKey: "test-key",
		CreatedAt:      now,
		Attempts:       0,
	}

	if msg.Id != "test-id" {
		t.Errorf("expected Id 'test-id', got %s", msg.Id)
	}

	if msg.Topic != "test.topic" {
		t.Errorf("expected Topic 'test.topic', got %s", msg.Topic)
	}

	if string(msg.Payload) != "test payload" {
		t.Errorf("expected Payload 'test payload', got %s", string(msg.Payload))
	}

	if msg.Headers["key"] != "value" {
		t.Errorf("expected header 'key' = 'value', got %s", msg.Headers["key"])
	}
}

func TestMessageFailure(t *testing.T) {
	failure := MessageFailure{
		MessageId: "test-id",
		Error:     "test error",
		Attempts:  3,
	}

	if failure.MessageId != "test-id" {
		t.Errorf("expected MessageId 'test-id', got %s", failure.MessageId)
	}

	if failure.Error != "test error" {
		t.Errorf("expected Error 'test error', got %s", failure.Error)
	}

	if failure.Attempts != 3 {
		t.Errorf("expected Attempts 3, got %d", failure.Attempts)
	}
}
