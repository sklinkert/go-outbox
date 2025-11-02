package outbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBatcherSizeBasedFlush(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()

	config := DefaultConfig()
	config.BatchSize = 3
	config.FlushTimeout = 10 * time.Second // Long timeout to test size-based flush

	batcher := newBatcher(publisher, store, config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Start flush loop
	wg.Add(1)
	go batcher.flushLoop(ctx, &wg)

	// Send messages
	for i := 0; i < 3; i++ {
		batcher.messageChan <- &Message{
			Id:             string(rune(i)),
			Topic:          "test",
			Payload:        []byte("test"),
			IdempotencyKey: string(rune(i)),
			CreatedAt:      time.Now(),
		}
	}

	// Wait for batch to be flushed
	time.Sleep(100 * time.Millisecond)

	// Check that batch was sent
	select {
	case batch := <-batcher.batchChan:
		if len(batch) != 3 {
			t.Errorf("expected batch size 3, got %d", len(batch))
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for batch")
	}

	cancel()
	wg.Wait()
}

func TestBatcherTimeBasedFlush(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()

	config := DefaultConfig()
	config.BatchSize = 100                      // Large batch size
	config.FlushTimeout = 50 * time.Millisecond // Short timeout to test time-based flush

	batcher := newBatcher(publisher, store, config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Start flush loop
	wg.Add(1)
	go batcher.flushLoop(ctx, &wg)

	// Send just 2 messages (less than batch size)
	for i := 0; i < 2; i++ {
		batcher.messageChan <- &Message{
			Id:             string(rune(i)),
			Topic:          "test",
			Payload:        []byte("test"),
			IdempotencyKey: string(rune(i)),
			CreatedAt:      time.Now(),
		}
	}

	// Wait for timeout-based flush
	select {
	case batch := <-batcher.batchChan:
		if len(batch) != 2 {
			t.Errorf("expected batch size 2, got %d", len(batch))
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timeout waiting for batch flush")
	}

	cancel()
	wg.Wait()
}

func TestBatcherGracefulShutdown(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()

	config := DefaultConfig()
	config.BatchSize = 100

	batcher := newBatcher(publisher, store, config)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	// Start flush loop
	wg.Add(1)
	go batcher.flushLoop(ctx, &wg)

	// Send messages
	batcher.messageChan <- &Message{
		Id:             "1",
		Topic:          "test",
		Payload:        []byte("test"),
		IdempotencyKey: "1",
		CreatedAt:      time.Now(),
	}

	time.Sleep(50 * time.Millisecond)

	// Cancel context (shutdown)
	cancel()

	// Wait for graceful shutdown
	wg.Wait()

	// Check that remaining messages were flushed
	select {
	case batch := <-batcher.batchChan:
		if len(batch) != 1 {
			t.Errorf("expected batch size 1, got %d", len(batch))
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for final batch")
	}
}

func TestBatcherEmptyBatch(t *testing.T) {
	store := newMockStore()
	publisher := newMockPublisher()

	config := DefaultConfig()
	config.BatchSize = 10
	config.FlushTimeout = 50 * time.Millisecond

	batcher := newBatcher(publisher, store, config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Start flush loop
	wg.Add(1)
	go batcher.flushLoop(ctx, &wg)

	// Wait for flush timeout without sending messages
	time.Sleep(200 * time.Millisecond)

	// Should not panic and no batch should be sent
	select {
	case <-batcher.batchChan:
		t.Error("unexpected batch sent")
	default:
		// Expected: no batch sent
	}

	cancel()
	wg.Wait()
}
