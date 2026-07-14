package realtime

import "testing"

func TestHubFiltersOriginAndInactivePeers(t *testing.T) {
	hub := NewHub()
	origin := NewPeer("origin", "owner")
	active := NewPeer("active", "owner")
	inactive := NewPeer("inactive", "owner")
	inactive.SetActive(false)
	hub.Register(origin)
	hub.Register(active)
	hub.Register(inactive)
	hub.Broadcast(Hint{OwnerKey: "owner", OriginConnectionID: "origin"}, []byte("hint"))
	select {
	case <-active.Send:
	default:
		t.Fatal("active peer did not receive hint")
	}
	select {
	case <-origin.Send:
		t.Fatal("origin received its own hint")
	default:
	}
	select {
	case <-inactive.Send:
		t.Fatal("inactive peer received hint")
	default:
	}
}

func TestRetryAfterSecondsRoundsUp(t *testing.T) {
	if got := RetryAfterSeconds(1001); got != "1" {
		// 1001ns is intentionally below one second.
		t.Fatalf("unexpected retry-after: %s", got)
	}
}
