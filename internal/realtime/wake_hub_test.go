package realtime

import (
	"sync"
	"testing"
	"time"
)

func TestWakeHubPublishesToEverySubscriptionForOwner(t *testing.T) {
	hub := NewWakeHub()
	first := hub.Register("owner")
	second := hub.Register("owner")

	hub.Publish("owner")

	requireWake(t, first.Notifications())
	requireWake(t, second.Notifications())
}

func TestWakeHubIsolatesOwners(t *testing.T) {
	hub := NewWakeHub()
	wanted := hub.Register("wanted")
	other := hub.Register("other")

	hub.Publish("wanted")

	requireWake(t, wanted.Notifications())
	requireNoWake(t, other.Notifications())
}

func TestWakeHubCoalescesPendingWakeUps(t *testing.T) {
	hub := NewWakeHub()
	subscription := hub.Register("owner")

	for range 100 {
		hub.Publish("owner")
	}

	if got := len(subscription.notifications); got != 1 {
		t.Fatalf("pending wake-ups = %d, want 1", got)
	}
	requireWake(t, subscription.Notifications())
	requireNoWake(t, subscription.Notifications())
}

func TestWakeHubUnregisterClosesOnlyThatSubscription(t *testing.T) {
	hub := NewWakeHub()
	removed := hub.Register("owner")
	remaining := hub.Register("owner")

	hub.Unregister(removed)
	hub.Unregister(removed)
	hub.Publish("owner")

	requireClosed(t, removed.Notifications())
	requireWake(t, remaining.Notifications())
}

func TestWakeHubCloseClosesSubscriptionsAndFutureRegistrations(t *testing.T) {
	hub := NewWakeHub()
	first := hub.Register("first")
	second := hub.Register("second")

	hub.Close()
	hub.Close()
	hub.Publish("first")
	future := hub.Register("future")

	requireClosed(t, first.Notifications())
	requireClosed(t, second.Notifications())
	requireClosed(t, future.Notifications())
}

func TestWakeHubPublishUnregisterAndCloseAreConcurrentSafe(t *testing.T) {
	hub := NewWakeHub()
	subscriptions := make([]*WakeSubscription, 64)
	for index := range subscriptions {
		subscriptions[index] = hub.Register("owner")
	}

	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for range 1_000 {
				hub.Publish("owner")
			}
		}()
	}
	for _, subscription := range subscriptions {
		wait.Add(1)
		go func(subscription *WakeSubscription) {
			defer wait.Done()
			<-start
			hub.Unregister(subscription)
		}(subscription)
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		hub.Close()
	}()

	close(start)
	wait.Wait()
	for _, subscription := range subscriptions {
		requireClosed(t, subscription.Notifications())
	}
}

func requireWake(t *testing.T, notifications <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-notifications:
		if !ok {
			t.Fatal("notification channel closed before wake-up")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wake-up")
	}
}

func requireNoWake(t *testing.T, notifications <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-notifications:
		if !ok {
			t.Fatal("notification channel unexpectedly closed")
		}
		t.Fatal("received unexpected wake-up")
	default:
	}
}

func requireClosed(t *testing.T, notifications <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-notifications:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("timed out waiting for notification channel to close")
		}
	}
}
