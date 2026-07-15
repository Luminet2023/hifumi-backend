package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"google.golang.org/protobuf/proto"
)

const (
	sseHeartbeatInterval = 15 * time.Second
	sseReconcileInterval = 30 * time.Second
	sseWriteTimeout      = 10 * time.Second
	sseFeedPageLimit     = 128
	maxJavaScriptInteger = uint64(1<<53 - 1)
)

type sseProtobufEnvelope struct {
	Version  int    `json:"version"`
	Protobuf string `json:"protobuf"`
}

type sseErrorEnvelope struct {
	Version int    `json:"version"`
	Code    string `json:"code"`
}

var errSSEWrite = errors.New("SSE write failed")

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if !s.validStateOrigin(r) {
		writeAPIError(w, http.StatusForbidden, "invalid_origin")
		return
	}
	if !acceptsEventStream(r.Header.Get("Accept")) {
		writeAPIError(w, http.StatusNotAcceptable, "event_stream_required")
		return
	}
	baselineID, cursor, ok := parseEventCursor(r)
	if !ok {
		writeAPIError(w, http.StatusBadRequest, "invalid_sync_cursor")
		return
	}
	claims, ownerKey, err := s.authenticate(r)
	if err != nil || (claims.ExpiresAt != nil && !s.now().Before(claims.ExpiresAt.Time)) {
		writeAPIError(w, http.StatusUnauthorized, "authentication_required")
		return
	}

	connectionID := newRequestID()
	if err := s.realtime.AcquireConnection(r.Context(), ownerKey, connectionID, maxConnectionsPerOwner, realtime.DefaultLeaseTTL); err != nil {
		if errors.Is(err, realtime.ErrConnectionLimit) {
			w.Header().Set("Retry-After", "5")
			writeAPIError(w, http.StatusTooManyRequests, "sync_connection_limit")
			return
		}
		writeAPIError(w, http.StatusServiceUnavailable, "redis_unavailable")
		return
	}
	defer func() {
		releaseContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.realtime.ReleaseConnection(releaseContext, ownerKey, connectionID)
	}()

	ctx, cancel := context.WithCancel(r.Context())
	stopServerCancel := context.AfterFunc(s.streamContext, cancel)
	defer func() {
		stopServerCancel()
		cancel()
	}()
	subscription := s.wakeHub.Register(ownerKey)
	defer s.wakeHub.Unregister(subscription)

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	leaseErrors := make(chan error, 1)
	go func() {
		if err := s.refreshLease(ctx, ownerKey, connectionID); err != nil {
			select {
			case leaseErrors <- err:
			default:
			}
		}
	}()

	expires := (<-chan time.Time)(nil)
	var expiryTimer *time.Timer
	if claims.ExpiresAt != nil {
		expiryTimer = time.NewTimer(claims.ExpiresAt.Time.Sub(s.now()))
		defer expiryTimer.Stop()
		expires = expiryTimer.C
	}

	s.logger.InfoContext(ctx, "sync event stream opened", "owner_key", ownerKey, "connection_id", connectionID, "baseline_id", baselineID, "cursor", cursor)
	defer s.logger.InfoContext(context.Background(), "sync event stream closed", "owner_key", ownerKey, "connection_id", connectionID)

	cursor, terminal, err := s.sendFeed(ctx, w, ownerKey, baselineID, cursor, true)
	if err != nil {
		s.streamUnavailable(w, r, connectionID, "initial_feed", err)
		return
	}
	if terminal {
		return
	}

	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()
	reconcile := time.NewTicker(sseReconcileInterval)
	defer reconcile.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-expires:
			_ = writeSSEJSON(w, "auth_required", sseErrorEnvelope{Version: 1, Code: "auth_required"})
			return
		case err := <-leaseErrors:
			s.streamUnavailable(w, r, connectionID, "lease_refresh", err)
			return
		case <-heartbeat.C:
			if err := writeSSEComment(w, "heartbeat"); err != nil {
				return
			}
		case _, ok := <-subscription.Notifications():
			if !ok {
				return
			}
			var terminal bool
			cursor, terminal, err = s.sendFeed(ctx, w, ownerKey, baselineID, cursor, false)
			if err != nil {
				s.streamUnavailable(w, r, connectionID, "hint_feed", err)
				return
			}
			if terminal {
				return
			}
		case <-reconcile.C:
			var terminal bool
			cursor, terminal, err = s.sendFeed(ctx, w, ownerKey, baselineID, cursor, false)
			if err != nil {
				s.streamUnavailable(w, r, connectionID, "reconcile_feed", err)
				return
			}
			if terminal {
				return
			}
		}
	}
}

// sendFeed 每页都通过短事务重新读取 MySQL；调用返回后不持有事务或 owner lock。
// ready 只用于一次连接的初始追赶完成 checkpoint，后续唤醒不会发送空事件。
func (s *Server) sendFeed(
	ctx context.Context,
	w http.ResponseWriter,
	ownerKey, requestedBaselineID string,
	cursor uint64,
	sendReady bool,
) (uint64, bool, error) {
	for {
		page, err := s.sync.ReadFeedPage(ctx, ownerKey, requestedBaselineID, cursor, sseFeedPageLimit)
		if err != nil {
			return cursor, false, err
		}
		if page.BaselineMismatch {
			response := feedResponse(page, cursor)
			response.BaselineMismatch = true
			if err := writeSSEProtobuf(w, "baseline_mismatch", eventID(page.BaselineID, response.GetNextCursor()), response); err != nil {
				return cursor, true, err
			}
			return cursor, true, nil
		}
		if page.ResetRequired {
			response := feedResponse(page, 0)
			response.ResetRequired = true
			if err := writeSSEProtobuf(w, "reset_required", eventID(page.BaselineID, 0), response); err != nil {
				return cursor, true, err
			}
			return cursor, true, nil
		}
		if len(page.Changes) == 0 {
			if sendReady {
				response := feedResponse(page, cursor)
				if err := writeSSEProtobuf(w, "ready", eventID(page.BaselineID, cursor), response); err != nil {
					return cursor, false, err
				}
			}
			return cursor, false, nil
		}

		response, consumed, err := boundedFeedResponse(page, cursor)
		if err != nil {
			return cursor, false, err
		}
		if err := writeSSEProtobuf(w, "changes", eventID(page.BaselineID, response.GetNextCursor()), response); err != nil {
			return cursor, false, err
		}
		cursor = response.GetNextCursor()
		if consumed < len(page.Changes) || page.HasMore {
			continue
		}
		if sendReady {
			ready := feedResponse(page, cursor)
			ready.HasMore = false
			if err := writeSSEProtobuf(w, "ready", eventID(page.BaselineID, cursor), ready); err != nil {
				return cursor, false, err
			}
		}
		return cursor, false, nil
	}
}

func feedResponse(page *syncservice.FeedPage, nextCursor uint64) *syncv1.SyncResponse {
	return &syncv1.SyncResponse{
		NextCursor:        nextCursor,
		BaselineId:        page.BaselineID,
		ServerVersion:     page.ServerVersion,
		ServerUpdatedAtMs: page.ServerUpdatedAtMs,
		ServerProgressDay: page.ServerProgressDay,
	}
}

func boundedFeedResponse(page *syncservice.FeedPage, previousCursor uint64) (*syncv1.SyncResponse, int, error) {
	response := feedResponse(page, page.NextCursor)
	response.Changes = page.Changes
	response.HasMore = page.HasMore
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		return nil, 0, err
	}
	if len(encoded) <= MaxProtobufBytes {
		return response, len(page.Changes), nil
	}

	low, high := 1, len(page.Changes)
	best := 0
	for low <= high {
		middle := low + (high-low)/2
		candidate := feedResponse(page, page.Changes[middle-1].GetCursor())
		candidate.Changes = page.Changes[:middle]
		candidate.HasMore = true
		encoded, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(candidate)
		if marshalErr != nil {
			return nil, 0, marshalErr
		}
		if len(encoded) <= MaxProtobufBytes {
			best = middle
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if best == 0 {
		return nil, 0, fmt.Errorf("single sync change after cursor %d exceeds SSE protobuf limit", previousCursor)
	}
	response = feedResponse(page, page.Changes[best-1].GetCursor())
	response.Changes = page.Changes[:best]
	response.HasMore = true
	return response, best, nil
}

func (s *Server) streamUnavailable(w http.ResponseWriter, r *http.Request, connectionID, class string, err error) {
	s.logger.WarnContext(r.Context(), "sync event stream unavailable", "request_id", requestID(r.Context()), "connection_id", connectionID, "class", class, "error", err)
	if errors.Is(err, errSSEWrite) {
		// 写 deadline 或客户端断开后不能再延长一次 deadline 尝试控制事件。
		return
	}
	_ = writeSSEJSON(w, "unavailable", sseErrorEnvelope{Version: 1, Code: "unavailable"})
}

func parseEventCursor(r *http.Request) (string, uint64, bool) {
	query := r.URL.Query()
	baselines, cursors := query["baselineId"], query["cursor"]
	if len(baselines) != 1 || len(cursors) != 1 || !baselinePattern.MatchString(baselines[0]) || cursors[0] == "" {
		return "", 0, false
	}
	cursor, err := strconv.ParseUint(cursors[0], 10, 64)
	if err != nil || cursor > maxJavaScriptInteger {
		return "", 0, false
	}
	return baselines[0], cursor, true
}

func acceptsEventStream(accept string) bool {
	for _, value := range strings.Split(accept, ",") {
		if strings.EqualFold(strings.TrimSpace(strings.SplitN(value, ";", 2)[0]), "text/event-stream") {
			return true
		}
	}
	return false
}

func eventID(baselineID string, cursor uint64) string {
	return baselineID + ":" + strconv.FormatUint(cursor, 10)
}

func writeSSEProtobuf(w http.ResponseWriter, event, id string, message proto.Message) error {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return err
	}
	if len(encoded) > MaxProtobufBytes {
		return fmt.Errorf("SSE protobuf event exceeds %d bytes", MaxProtobufBytes)
	}
	data, err := json.Marshal(sseProtobufEnvelope{Version: 1, Protobuf: base64.StdEncoding.EncodeToString(encoded)})
	if err != nil {
		return err
	}
	return writeSSE(w, event, id, data)
}

func writeSSEJSON(w http.ResponseWriter, event string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writeSSE(w, event, "", data)
}

func writeSSE(w http.ResponseWriter, event, id string, data []byte) error {
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
	if id != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
			return fmt.Errorf("%w: %v", errSSEWrite, err)
		}
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
		return fmt.Errorf("%w: %v", errSSEWrite, err)
	}
	if err := controller.Flush(); err != nil {
		return fmt.Errorf("%w: %v", errSSEWrite, err)
	}
	return nil
}

func writeSSEComment(w http.ResponseWriter, comment string) error {
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
	if _, err := fmt.Fprintf(w, ": %s\n\n", comment); err != nil {
		return fmt.Errorf("%w: %v", errSSEWrite, err)
	}
	if err := controller.Flush(); err != nil {
		return fmt.Errorf("%w: %v", errSSEWrite, err)
	}
	return nil
}
