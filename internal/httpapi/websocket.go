package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	maxConnectionsPerOwner  = 8
	sessionExpiredCloseCode = websocket.StatusCode(4001)
)

func (s *Server) webSocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		w.Header().Set("Upgrade", "websocket")
		writeAPIError(w, http.StatusUpgradeRequired, "websocket_upgrade_required")
		return
	}
	if !s.isAllowedFrontendOrigin(r.Header.Get("Origin")) {
		writeAPIError(w, http.StatusForbidden, "invalid_origin")
		return
	}
	if !s.validRefererForOrigin(r, false) {
		writeAPIError(w, http.StatusForbidden, "invalid_referer")
		return
	}
	if !hasSubprotocol(r.Header.Get("Sec-WebSocket-Protocol"), SyncWebSocketProtocol) {
		writeAPIError(w, http.StatusBadRequest, "websocket_subprotocol_required")
		return
	}
	claims, ownerKey, err := s.authenticate(r)
	if err != nil {
		writeAPIError(w, http.StatusUnauthorized, "authentication_required")
		return
	}
	connectionID := newRequestID()
	if err := s.realtime.AcquireConnection(r.Context(), ownerKey, connectionID, maxConnectionsPerOwner, realtime.DefaultLeaseTTL); err != nil {
		if errors.Is(err, realtime.ErrConnectionLimit) {
			w.Header().Set("Retry-After", "5")
			writeAPIError(w, http.StatusTooManyRequests, "websocket_connection_limit")
			return
		}
		writeAPIError(w, http.StatusServiceUnavailable, "redis_unavailable")
		return
	}
	leaseOwned := true
	defer func() {
		if leaseOwned {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = s.realtime.ReleaseConnection(ctx, ownerKey, connectionID)
		}
	}()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       []string{SyncWebSocketProtocol},
		InsecureSkipVerify: true, // Origin 已在升级前做精确比较。
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(MaxWebSocketFrame)
	peer := realtime.NewPeer(connectionID, ownerKey)
	s.hub.Register(peer)
	defer s.hub.Unregister(peer)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	writeErrors := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-peer.Send:
				if err := conn.Write(ctx, websocket.MessageText, message); err != nil {
					select {
					case writeErrors <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()

	leaseErrors := make(chan error, 1)
	go func() {
		if err := s.refreshLease(ctx, ownerKey, connectionID); err != nil {
			leaseErrors <- err
			cancel()
		}
	}()
	readContext := ctx
	if claims.ExpiresAt != nil {
		var deadlineCancel context.CancelFunc
		readContext, deadlineCancel = context.WithDeadline(ctx, claims.ExpiresAt.Time)
		defer deadlineCancel()
	}

	s.logger.InfoContext(ctx, "sync websocket opened", "owner_key", ownerKey, "connection_id", connectionID)
	s.logger.InfoContext(ctx, "legacy sync websocket upgrade succeeded", "owner_key", ownerKey, "connection_id", connectionID)
	closeStatus, closeReason := websocket.StatusNormalClosure, "connection closed"
	defer func() {
		cancel()
		_ = conn.Close(closeStatus, closeReason)
		s.logger.InfoContext(context.Background(), "sync websocket closed", "owner_key", ownerKey, "connection_id", connectionID, "code", closeStatus)
	}()

	for {
		messageType, message, err := conn.Read(readContext)
		if err != nil {
			select {
			case writeErr := <-writeErrors:
				s.logger.WarnContext(context.Background(), "sync websocket writer failed", "connection_id", connectionID, "error", writeErr)
				closeStatus, closeReason = websocket.StatusInternalError, "WebSocket write failed"
				return
			case leaseErr := <-leaseErrors:
				s.logger.WarnContext(context.Background(), "sync websocket lease failed", "connection_id", connectionID, "error", leaseErr)
				closeStatus, closeReason = websocket.StatusInternalError, "Connection lease unavailable"
				return
			default:
			}
			if claims.ExpiresAt != nil && !s.now().Before(claims.ExpiresAt.Time) {
				cancel()
				writeContext, writeCancel := context.WithTimeout(context.Background(), time.Second)
				_ = conn.Write(writeContext, websocket.MessageText, mustServerError("", "AUTH_REQUIRED", 0))
				writeCancel()
				closeStatus, closeReason = sessionExpiredCloseCode, "Authentication required"
				return
			}
			if status := websocket.CloseStatus(err); status != -1 {
				closeStatus, closeReason = status, "peer closed"
			}
			return
		}
		select {
		case err := <-writeErrors:
			s.logger.WarnContext(ctx, "sync websocket writer failed", "connection_id", connectionID, "error", err)
			closeStatus, closeReason = websocket.StatusInternalError, "WebSocket write failed"
			return
		case err := <-leaseErrors:
			s.logger.WarnContext(ctx, "sync websocket lease failed", "connection_id", connectionID, "error", err)
			closeStatus, closeReason = websocket.StatusInternalError, "Connection lease unavailable"
			return
		default:
		}
		if messageType != websocket.MessageText {
			closeStatus, closeReason = websocket.StatusUnsupportedData, "Text frames required"
			return
		}
		if string(message) == "ping" {
			if !s.enqueueWS(peer, []byte("pong")) {
				closeStatus, closeReason = websocket.StatusPolicyViolation, "Slow connection"
				return
			}
			continue
		}
		frame, err := DecodeClientFrame(message)
		if err != nil {
			var frameError *FrameError
			if errors.As(err, &frameError) {
				if frameError.CloseCode != 0 {
					closeStatus, closeReason = websocket.StatusCode(frameError.CloseCode), "Invalid sync frame"
					return
				}
				s.enqueueWS(peer, mustServerError(recoverRequestID(message), frameError.Code, 0))
				continue
			}
			closeStatus, closeReason = websocket.StatusInternalError, "Invalid sync frame"
			return
		}
		if frame.Type == "activity" {
			peer.SetActive(frame.Active)
			continue
		}
		if !s.processWebSocketRPC(ctx, peer, ownerKey, connectionID, frame) {
			closeStatus, closeReason = websocket.StatusInternalError, "Internal sync error"
			return
		}
	}
}

func (s *Server) processWebSocketRPC(ctx context.Context, peer *realtime.Peer, ownerKey, connectionID string, frame ClientFrame) bool {
	metadata := syncservice.CommandMetadata{OwnerKey: ownerKey, OriginConnectionID: connectionID}
	var exchangeRequest *syncv1.SyncRequest
	var resolveRequest *syncv1.ResolveBaselineRequest
	if frame.Type == "exchange" {
		exchangeRequest = &syncv1.SyncRequest{}
		if proto.Unmarshal(frame.Protobuf, exchangeRequest) != nil {
			s.enqueueWS(peer, mustServerError(frame.RequestID, "INVALID_ARGUMENT", 0))
			return true
		}
		if err := syncservice.ValidateExchange(ownerKey, exchangeRequest); err != nil {
			return s.enqueueOperationError(ctx, peer, ownerKey, frame.Type, frame.RequestID, err)
		}
	} else {
		resolveRequest = &syncv1.ResolveBaselineRequest{}
		if proto.Unmarshal(frame.Protobuf, resolveRequest) != nil {
			s.enqueueWS(peer, mustServerError(frame.RequestID, "INVALID_ARGUMENT", 0))
			return true
		}
		if err := syncservice.ValidateResolve(ownerKey, resolveRequest); err != nil {
			return s.enqueueOperationError(ctx, peer, ownerKey, frame.Type, frame.RequestID, err)
		}
	}
	limit, window := 8, 10*time.Second
	if frame.Type == "resolve" {
		limit, window = 3, time.Minute
	}
	retry, err := s.realtime.CheckFixedWindow(ctx, frame.Type, ownerKey, limit, window)
	if err != nil {
		s.logger.ErrorContext(ctx, "sync websocket rate limit failed",
			"operation", frame.Type,
			"owner_key", ownerKey,
			"request_id", frame.RequestID,
			"error", err,
		)
		s.enqueueWS(peer, mustServerError(frame.RequestID, "INTERNAL", 0))
		return false
	}
	if retry > 0 {
		s.enqueueWS(peer, mustServerError(frame.RequestID, "RATE_LIMITED", uint64(retry/time.Millisecond)))
		return true
	}
	var response proto.Message
	var resultType string
	if exchangeRequest != nil {
		result, err := s.sync.Exchange(ctx, metadata, exchangeRequest)
		if err != nil {
			return s.enqueueOperationError(ctx, peer, ownerKey, frame.Type, frame.RequestID, err)
		}
		response, resultType = result.Response, "exchange_result"
	} else {
		result, err := s.sync.ResolveBaseline(ctx, metadata, resolveRequest)
		if err != nil {
			return s.enqueueOperationError(ctx, peer, ownerKey, frame.Type, frame.RequestID, err)
		}
		response, resultType = result.Response, "resolve_result"
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		s.logger.ErrorContext(ctx, "sync websocket response marshal failed",
			"operation", frame.Type,
			"owner_key", ownerKey,
			"request_id", frame.RequestID,
			"error", err,
		)
		s.enqueueWS(peer, mustServerError(frame.RequestID, "INTERNAL", 0))
		return false
	}
	message, err := EncodeServerResult(resultType, frame.RequestID, encoded)
	if err != nil {
		s.logger.ErrorContext(ctx, "sync websocket response encode failed",
			"operation", frame.Type,
			"owner_key", ownerKey,
			"request_id", frame.RequestID,
			"protobuf_bytes", len(encoded),
			"error", err,
		)
		s.enqueueWS(peer, mustServerError(frame.RequestID, "INTERNAL", 0))
		return false
	}
	return s.enqueueWS(peer, message)
}

func (s *Server) enqueueOperationError(
	ctx context.Context,
	peer *realtime.Peer,
	ownerKey, operation, requestID string,
	err error,
) bool {
	var operationError *syncservice.OperationError
	if errors.As(err, &operationError) {
		s.enqueueWS(peer, mustServerError(requestID, string(operationError.Code), 0))
		return true
	}
	s.logger.ErrorContext(ctx, "sync websocket operation failed",
		"operation", operation,
		"owner_key", ownerKey,
		"request_id", requestID,
		"error", err,
	)
	s.enqueueWS(peer, mustServerError(requestID, "INTERNAL", 0))
	return false
}

func (s *Server) refreshLease(ctx context.Context, ownerKey, connectionID string) error {
	ticker := time.NewTicker(realtime.DefaultLeaseTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			refreshed, err := s.realtime.RefreshConnection(ctx, ownerKey, connectionID, realtime.DefaultLeaseTTL)
			if err == nil && !refreshed {
				err = errors.New("connection lease disappeared")
			}
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) enqueueWS(peer *realtime.Peer, message []byte) bool {
	select {
	case peer.Send <- message:
		return true
	default:
		return false
	}
}

func mustServerError(requestID, code string, retryAfterMS uint64) []byte {
	message, _ := EncodeServerError(requestID, code, retryAfterMS)
	return message
}

func recoverRequestID(message []byte) string {
	var frame struct {
		RequestID string `json:"requestId"`
	}
	if json.Unmarshal(message, &frame) == nil && requestIDPattern.MatchString(frame.RequestID) {
		return frame.RequestID
	}
	return ""
}

func hasSubprotocol(header, expected string) bool {
	for _, candidate := range strings.Split(header, ",") {
		if strings.TrimSpace(candidate) == expected {
			return true
		}
	}
	return false
}
