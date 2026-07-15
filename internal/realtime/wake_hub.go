package realtime

import "sync"

// WakeSubscription represents one stream waiting for changes owned by ownerKey.
// Notifications is deliberately a signal-only channel: consumers must read the
// authoritative changes from MySQL after every wake-up.
type WakeSubscription struct {
	ownerKey      string
	notifications chan struct{}
}

// Notifications returns the subscription's coalesced wake-up channel. The
// channel is closed when the subscription is unregistered or the hub is closed.
func (s *WakeSubscription) Notifications() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.notifications
}

// WakeHub fans owner-scoped, best-effort wake-ups out to SSE streams.
//
// Each subscription has a capacity of one, so repeated publications are
// coalesced until the stream consumes the pending wake-up. Wake-ups contain no
// state and are never a substitute for reading the persistent feed.
type WakeHub struct {
	mu      sync.RWMutex
	byOwner map[string]map[*WakeSubscription]struct{}
	closed  bool
}

func NewWakeHub() *WakeHub {
	return &WakeHub{byOwner: make(map[string]map[*WakeSubscription]struct{})}
}

// Register subscribes one stream to ownerKey. Registering after Close returns
// a subscription whose notification channel is already closed.
func (h *WakeHub) Register(ownerKey string) *WakeSubscription {
	subscription := &WakeSubscription{
		ownerKey:      ownerKey,
		notifications: make(chan struct{}, 1),
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		close(subscription.notifications)
		return subscription
	}

	subscriptions := h.byOwner[ownerKey]
	if subscriptions == nil {
		subscriptions = make(map[*WakeSubscription]struct{})
		h.byOwner[ownerKey] = subscriptions
	}
	subscriptions[subscription] = struct{}{}
	return subscription
}

// Publish wakes every registered stream for ownerKey without blocking. A
// subscription that already has a pending wake-up is left unchanged.
func (h *WakeHub) Publish(ownerKey string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return
	}
	for subscription := range h.byOwner[ownerKey] {
		select {
		case subscription.notifications <- struct{}{}:
		default:
		}
	}
}

// Unregister removes and closes subscription. It is safe to call more than
// once and concurrently with Publish or Close.
func (h *WakeHub) Unregister(subscription *WakeSubscription) {
	if subscription == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	subscriptions := h.byOwner[subscription.ownerKey]
	if _, exists := subscriptions[subscription]; !exists {
		return
	}
	delete(subscriptions, subscription)
	if len(subscriptions) == 0 {
		delete(h.byOwner, subscription.ownerKey)
	}
	close(subscription.notifications)
}

// Close closes every registered subscription and rejects future registrations.
// It is safe to call more than once and concurrently with Publish or
// Unregister.
func (h *WakeHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for _, subscriptions := range h.byOwner {
		for subscription := range subscriptions {
			close(subscription.notifications)
		}
	}
	h.byOwner = make(map[string]map[*WakeSubscription]struct{})
}
