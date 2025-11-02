package outbox

import (
	"context"
	"sync"
	"time"
)

// Processor orchestrates the outbox pattern by polling for messages and publishing them.
// It manages worker goroutines, graceful shutdown, and error handling.
type Processor struct {
	store     Store
	publisher Publisher
	config    Config

	poller  *poller
	batcher *batcher

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	started bool
	mu      sync.Mutex
}

// NewProcessor creates a new outbox processor with the given configuration.
func NewProcessor(store Store, publisher Publisher, config Config) (*Processor, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Processor{
		store:     store,
		publisher: publisher,
		config:    config,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Initialize poller
	p.poller = newPoller(store, config)

	// Initialize batcher for smart batch publishing
	p.batcher = newBatcher(publisher, store, config)

	return p, nil
}

// Start begins processing outbox messages.
// It spawns worker goroutines and returns immediately.
// Returns an error if the processor is already started.
func (p *Processor) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return ErrProcessorStopped{}
	}

	p.started = true

	p.config.Logger.Info("starting outbox processor", map[string]interface{}{
		"poll_interval": p.config.PollInterval.String(),
		"batch_size":    p.config.BatchSize,
		"worker_count":  p.config.WorkerCount,
	})

	// Start polling loop
	p.wg.Add(1)
	go p.pollLoop()

	// Start worker pool for publishing
	for i := 0; i < p.config.WorkerCount; i++ {
		p.wg.Add(1)
		go p.publishWorker(i)
	}

	// Start batch flusher
	p.wg.Add(1)
	go p.batcher.flushLoop(p.ctx, &p.wg)

	return nil
}

// Stop gracefully shuts down the processor.
// It waits for all in-flight messages to be processed or until shutdown timeout is reached.
func (p *Processor) Stop() error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return ErrProcessorStopped{}
	}
	p.started = false
	p.mu.Unlock()

	p.config.Logger.Info("stopping outbox processor", map[string]interface{}{
		"shutdown_timeout": p.config.ShutdownTimeout.String(),
	})

	// Signal shutdown
	p.cancel()

	// Wait for graceful shutdown with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.config.Logger.Info("outbox processor stopped gracefully", nil)
		return nil
	case <-time.After(p.config.ShutdownTimeout):
		return ErrShutdownTimeout{Timeout: p.config.ShutdownTimeout.String()}
	}
}

// pollLoop continuously polls for new messages and sends them to the message channel.
func (p *Processor) pollLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			p.config.Logger.Debug("poll loop stopping", nil)
			return
		case <-ticker.C:
			p.poll()
		}
	}
}

// poll fetches pending messages and sends them to the batcher.
func (p *Processor) poll() {
	messages, err := p.poller.fetchPending(p.ctx)
	if err != nil {
		p.config.Logger.Error("failed to fetch pending messages", map[string]interface{}{
			"error": err.Error(),
		})
		p.config.MetricsHook.OnPollError(err)
		return
	}

	if len(messages) == 0 {
		return
	}

	p.config.Logger.Debug("fetched pending messages", map[string]interface{}{
		"count": len(messages),
	})
	p.config.MetricsHook.OnMessagesFetched(len(messages))

	// Send messages to batcher
	for _, msg := range messages {
		select {
		case <-p.ctx.Done():
			return
		case p.batcher.messageChan <- msg:
		}
	}
}

// publishWorker processes messages from the batcher's output channel.
func (p *Processor) publishWorker(workerId int) {
	defer p.wg.Done()

	p.config.Logger.Debug("publish worker started", map[string]interface{}{
		"worker_id": workerId,
	})

	for {
		select {
		case <-p.ctx.Done():
			p.config.Logger.Debug("publish worker stopping", map[string]interface{}{
				"worker_id": workerId,
			})
			return
		case batch := <-p.batcher.batchChan:
			p.processBatch(batch)
		}
	}
}

// processBatch publishes a batch of messages and updates their status.
func (p *Processor) processBatch(messages []*Message) {
	if len(messages) == 0 {
		return
	}

	start := time.Now()

	// Try batch publishing if supported
	if batchPub, ok := p.publisher.(BatchPublisher); ok {
		p.publishBatch(batchPub, messages, start)
	} else {
		// Fall back to individual publishing
		p.publishIndividual(messages, start)
	}
}

// publishBatch publishes messages using BatchPublisher interface.
func (p *Processor) publishBatch(batchPub BatchPublisher, messages []*Message, start time.Time) {
	err := batchPub.PublishBatch(p.ctx, messages)

	if err == nil {
		// All messages published successfully
		p.handleSuccess(messages, start)
		return
	}

	// Handle failure - fall back to individual publishing for retry
	p.config.Logger.Warn("batch publish failed, retrying individually", map[string]interface{}{
		"error": err.Error(),
		"count": len(messages),
	})
	p.config.MetricsHook.OnPublishError(err)

	p.publishIndividual(messages, start)
}

// publishIndividual publishes messages one by one.
func (p *Processor) publishIndividual(messages []*Message, start time.Time) {
	var successIds []string
	var failures []MessageFailure

	for _, msg := range messages {
		err := p.publisher.Publish(p.ctx, msg)
		if err != nil {
			msg.Attempts++
			failures = append(failures, MessageFailure{
				MessageId: msg.Id,
				Error:     err.Error(),
				Attempts:  msg.Attempts,
			})

			p.config.Logger.Error("failed to publish message", map[string]interface{}{
				"message_id": msg.Id,
				"topic":      msg.Topic,
				"attempts":   msg.Attempts,
				"error":      err.Error(),
			})

			if msg.Attempts >= p.config.MaxRetries {
				p.config.Logger.Error("message exceeded max retries", map[string]interface{}{
					"message_id":  msg.Id,
					"attempts":    msg.Attempts,
					"max_retries": p.config.MaxRetries,
				})
			}
		} else {
			successIds = append(successIds, msg.Id)
		}
	}

	// Update message statuses
	if len(successIds) > 0 {
		if err := p.store.MarkSent(p.ctx, successIds); err != nil {
			p.config.Logger.Error("failed to mark messages as sent", map[string]interface{}{
				"error": err.Error(),
				"count": len(successIds),
			})
		} else {
			duration := time.Since(start)
			p.config.Logger.Info("messages published successfully", map[string]interface{}{
				"count":    len(successIds),
				"duration": duration.String(),
			})
			p.config.MetricsHook.OnMessagesPublished(len(successIds), duration)
		}
	}

	if len(failures) > 0 {
		if err := p.store.MarkFailed(p.ctx, failures); err != nil {
			p.config.Logger.Error("failed to mark messages as failed", map[string]interface{}{
				"error": err.Error(),
				"count": len(failures),
			})
		}
		p.config.MetricsHook.OnMessagesFailed(len(failures))
	}
}

// handleSuccess marks all messages as successfully sent.
func (p *Processor) handleSuccess(messages []*Message, start time.Time) {
	messageIds := make([]string, len(messages))
	for i, msg := range messages {
		messageIds[i] = msg.Id
	}

	if err := p.store.MarkSent(p.ctx, messageIds); err != nil {
		p.config.Logger.Error("failed to mark messages as sent", map[string]interface{}{
			"error": err.Error(),
			"count": len(messageIds),
		})
		return
	}

	duration := time.Since(start)
	p.config.Logger.Info("batch published successfully", map[string]interface{}{
		"count":    len(messageIds),
		"duration": duration.String(),
	})
	p.config.MetricsHook.OnMessagesPublished(len(messageIds), duration)
}
