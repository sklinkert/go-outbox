package outbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockStore implements the Store interface for testing
type mockStore struct {
	messages       []*Message
	fetchedCount   int
	sentIds        []string
	failedMessages []MessageFailure
	mu             sync.Mutex
}

func newMockStore() *mockStore {
	return &mockStore{
		messages: make([]*Message, 0),
	}
}

func (s *mockStore) FetchPending(ctx context.Context, batchSize int) ([]*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fetchedCount++

	// Return messages that haven't been processed
	var pending []*Message
	for _, msg := range s.messages {
		if msg.ProcessedAt == nil && len(pending) < batchSize {
			pending = append(pending, msg)
		}
	}

	return pending, nil
}

func (s *mockStore) MarkSent(ctx context.Context, messageIds []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sentIds = append(s.sentIds, messageIds...)

	// Mark messages as processed
	now := time.Now()
	for _, msg := range s.messages {
		for _, id := range messageIds {
			if msg.Id == id {
				msg.ProcessedAt = &now
			}
		}
	}

	return nil
}

func (s *mockStore) MarkFailed(ctx context.Context, failures []MessageFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failedMessages = append(s.failedMessages, failures...)
	return nil
}

func (s *mockStore) Insert(ctx context.Context, messages []*Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages = append(s.messages, messages...)
	return nil
}

func (s *mockStore) addMessage(msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, msg)
}

// mockPublisher implements the Publisher interface for testing
type mockPublisher struct {
	published []*Message
	mu        sync.Mutex
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{
		published: make([]*Message, 0),
	}
}

func (p *mockPublisher) Publish(ctx context.Context, msg *Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.published = append(p.published, msg)
	return nil
}

func (p *mockPublisher) Close() error {
	return nil
}

func (p *mockPublisher) getPublishedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

// mockBatchPublisher implements BatchPublisher for testing
type mockBatchPublisher struct {
	mockPublisher
	batchCount int
}

func newMockBatchPublisher() *mockBatchPublisher {
	return &mockBatchPublisher{
		mockPublisher: mockPublisher{
			published: make([]*Message, 0),
		},
	}
}

func (p *mockBatchPublisher) PublishBatch(ctx context.Context, msgs []*Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.batchCount++
	p.published = append(p.published, msgs...)
	return nil
}

func TestNewProcessor(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()
	config := DefaultConfig()

	processor, err := NewProcessor(store, publisher, config)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	if processor == nil {
		t.Fatal("processor is nil")
	}

	if processor.store != store {
		t.Error("processor store not set correctly")
	}

	if processor.publisher != publisher {
		t.Error("processor publisher not set correctly")
	}
}

func TestNewProcessorInvalidConfig(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()

	config := DefaultConfig()
	config.BatchSize = 0 // Invalid

	_, err := NewProcessor(store, publisher, config)
	if err == nil {
		t.Error("expected error for invalid config")
	}
}

func TestProcessorStartStop(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()

	config := DefaultConfig()
	config.PollInterval = 100 * time.Millisecond
	config.ShutdownTimeout = 5 * time.Second

	processor, err := NewProcessor(store, publisher, config)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	// Start processor
	err = processor.Start()
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// Stop processor
	err = processor.Stop()
	if err != nil {
		t.Fatalf("failed to stop processor: %v", err)
	}
}

func TestProcessorPublishesMessages(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()

	// Add test messages
	for i := 0; i < 5; i++ {
		id := time.Now().Format(time.RFC3339Nano) + string(rune(i))
		store.addMessage(&Message{
			Id:             id,
			Topic:          "test.topic",
			Payload:        []byte("test"),
			IdempotencyKey: id,
			CreatedAt:      time.Now(),
		})
		time.Sleep(1 * time.Millisecond) // Ensure unique IDs
	}

	config := DefaultConfig()
	config.PollInterval = 100 * time.Millisecond
	config.BatchSize = 10
	config.ShutdownTimeout = 5 * time.Second

	processor, err := NewProcessor(store, publisher, config)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	err = processor.Start()
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}

	// Wait for messages to be processed
	time.Sleep(500 * time.Millisecond)

	err = processor.Stop()
	if err != nil {
		t.Fatalf("failed to stop processor: %v", err)
	}

	// Check that messages were published (at least 5)
	publishedCount := publisher.getPublishedCount()
	if publishedCount < 5 {
		t.Errorf("expected at least 5 messages published, got %d", publishedCount)
	}

	// Check that messages were marked as sent (at least 5)
	if len(store.sentIds) < 5 {
		t.Errorf("expected at least 5 messages marked as sent, got %d", len(store.sentIds))
	}
}

func TestProcessorBatchPublishing(t *testing.T) {
	store := newMockStore()
	publisher := newMockBatchPublisher()

	// Add test messages
	for i := 0; i < 10; i++ {
		id := time.Now().Format(time.RFC3339Nano) + string(rune(i))
		store.addMessage(&Message{
			Id:             id,
			Topic:          "test.topic",
			Payload:        []byte("test"),
			IdempotencyKey: id,
			CreatedAt:      time.Now(),
		})
		time.Sleep(1 * time.Millisecond) // Ensure unique IDs
	}

	config := DefaultConfig()
	config.PollInterval = 100 * time.Millisecond
	config.BatchSize = 5
	config.FlushTimeout = 50 * time.Millisecond
	config.ShutdownTimeout = 5 * time.Second

	processor, err := NewProcessor(store, publisher, config)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	err = processor.Start()
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}

	// Wait for messages to be processed
	time.Sleep(500 * time.Millisecond)

	err = processor.Stop()
	if err != nil {
		t.Fatalf("failed to stop processor: %v", err)
	}

	// Check that messages were published (at least 10)
	publishedCount := publisher.getPublishedCount()
	if publishedCount < 10 {
		t.Errorf("expected at least 10 messages published, got %d", publishedCount)
	}

	// Check that batch publishing was used
	if publisher.batchCount == 0 {
		t.Error("expected batch publishing to be used")
	}
}

func TestProcessorDoubleStart(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()
	config := DefaultConfig()
	config.ShutdownTimeout = 5 * time.Second

	processor, err := NewProcessor(store, publisher, config)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	err = processor.Start()
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}

	// Try to start again
	err = processor.Start()
	if err == nil {
		t.Error("expected error when starting processor twice")
	}

	processor.Stop()
}

func TestProcessorStopWithoutStart(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()
	config := DefaultConfig()

	processor, err := NewProcessor(store, publisher, config)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	err = processor.Stop()
	if err == nil {
		t.Error("expected error when stopping processor that was never started")
	}
}
