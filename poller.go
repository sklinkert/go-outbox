package outbox

import (
	"context"
)

// poller is responsible for fetching pending messages from the store.
type poller struct {
	store  Store
	config Config
}

// newPoller creates a new poller instance.
func newPoller(store Store, config Config) *poller {
	return &poller{
		store:  store,
		config: config,
	}
}

// fetchPending retrieves pending messages from the store.
// It respects the configured batch size and handles errors appropriately.
func (p *poller) fetchPending(ctx context.Context) ([]*Message, error) {
	messages, err := p.store.FetchPending(ctx, p.config.BatchSize)
	if err != nil {
		return nil, ErrStoreOperation{
			Operation: "FetchPending",
			Err:       err,
		}
	}

	return messages, nil
}
