package outbox

import (
	"context"
	"sync"
	"time"
)

// batcher aggregates messages into batches for efficient publishing.
// It implements both size-based and time-based flush strategies.
type batcher struct {
	publisher Publisher
	store     Store
	config    Config

	// messageChan receives individual messages from the poller
	messageChan chan *Message

	// batchChan sends completed batches to workers for publishing
	batchChan chan []*Message

	// currentBatch holds messages waiting to be published
	currentBatch []*Message
	mu           sync.Mutex

	// lastFlush tracks when we last flushed a batch
	lastFlush time.Time
}

// newBatcher creates a new batcher instance.
func newBatcher(publisher Publisher, store Store, config Config) *batcher {
	return &batcher{
		publisher:    publisher,
		store:        store,
		config:       config,
		messageChan:  make(chan *Message, config.BatchSize*2),
		batchChan:    make(chan []*Message, config.WorkerCount),
		currentBatch: make([]*Message, 0, config.BatchSize),
		lastFlush:    time.Now(),
	}
}

// flushLoop manages the batching logic with both size and time-based flushing.
func (b *batcher) flushLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	flushTicker := time.NewTicker(b.config.FlushTimeout)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining messages before shutdown
			b.mu.Lock()
			if len(b.currentBatch) > 0 {
				b.flushBatchLocked()
			}
			b.mu.Unlock()
			close(b.batchChan)
			return

		case msg := <-b.messageChan:
			b.mu.Lock()
			b.currentBatch = append(b.currentBatch, msg)

			// Size-based flush
			if len(b.currentBatch) >= b.config.BatchSize {
				b.flushBatchLocked()
			}
			b.mu.Unlock()

		case <-flushTicker.C:
			// Time-based flush for partial batches
			b.mu.Lock()
			if len(b.currentBatch) > 0 && time.Since(b.lastFlush) >= b.config.FlushTimeout {
				b.flushBatchLocked()
			}
			b.mu.Unlock()
		}
	}
}

// flushBatchLocked sends the current batch to workers.
// Caller must hold b.mu lock.
func (b *batcher) flushBatchLocked() {
	if len(b.currentBatch) == 0 {
		return
	}

	// Create a copy to avoid race conditions
	batch := make([]*Message, len(b.currentBatch))
	copy(batch, b.currentBatch)

	// Send batch to workers (non-blocking)
	select {
	case b.batchChan <- batch:
		b.currentBatch = b.currentBatch[:0]
		b.lastFlush = time.Now()
	default:
		// If channel is full, keep messages in current batch
		// They will be flushed in the next cycle
	}
}
