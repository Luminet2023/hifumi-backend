package realtime

import "sync"

type Peer struct {
	ID       string
	OwnerKey string
	Send     chan []byte

	mu     sync.RWMutex
	active bool
}

func NewPeer(id, ownerKey string) *Peer {
	return &Peer{ID: id, OwnerKey: ownerKey, Send: make(chan []byte, 32), active: true}
}

func (p *Peer) SetActive(active bool) {
	p.mu.Lock()
	p.active = active
	p.mu.Unlock()
}

func (p *Peer) Active() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active
}

type Hub struct {
	mu      sync.RWMutex
	byOwner map[string]map[string]*Peer
}

func NewHub() *Hub {
	return &Hub{byOwner: make(map[string]map[string]*Peer)}
}

func (h *Hub) Register(peer *Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	peers := h.byOwner[peer.OwnerKey]
	if peers == nil {
		peers = make(map[string]*Peer)
		h.byOwner[peer.OwnerKey] = peers
	}
	peers[peer.ID] = peer
}

func (h *Hub) Unregister(peer *Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	peers := h.byOwner[peer.OwnerKey]
	if peers == nil {
		return
	}
	delete(peers, peer.ID)
	if len(peers) == 0 {
		delete(h.byOwner, peer.OwnerKey)
	}
}

func (h *Hub) Broadcast(hint Hint, payload []byte) {
	h.mu.RLock()
	peers := make([]*Peer, 0, len(h.byOwner[hint.OwnerKey]))
	for _, peer := range h.byOwner[hint.OwnerKey] {
		peers = append(peers, peer)
	}
	h.mu.RUnlock()
	for _, peer := range peers {
		if peer.ID == hint.OriginConnectionID || !peer.Active() {
			continue
		}
		select {
		case peer.Send <- payload:
		default:
			// A slow peer will recover through its next pull/reconnect; do not block all users.
		}
	}
}
