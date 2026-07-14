package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

const (
	SyncWebSocketProtocol = "stella-sync-v1"
	SyncWebSocketVersion  = 1
	MaxProtobufBytes      = 512 * 1024
	MaxSyncEnvelopeBytes  = (MaxProtobufBytes*4+2)/3 + 128
	MaxWebSocketFrame     = MaxSyncEnvelopeBytes + 512
)

var (
	requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{12,128}$`)
	baselinePattern  = regexp.MustCompile(`^baseline_[a-f0-9]{32}$`)
	ErrInvalidFrame  = errors.New("invalid sync websocket frame")
)

type FrameError struct {
	Code      string
	CloseCode int
	Message   string
}

func (e *FrameError) Error() string { return e.Message }

func (e *FrameError) Unwrap() error { return ErrInvalidFrame }

func frameError(message string, closeCode int, code string) error {
	if code == "" {
		code = "INVALID_ARGUMENT"
	}
	return &FrameError{Code: code, CloseCode: closeCode, Message: message}
}

type ClientFrame struct {
	Version   int
	RequestID string
	Type      string
	Protobuf  []byte
	Active    bool
}

type wireFrame struct {
	Version       int    `json:"version"`
	RequestID     string `json:"requestId,omitempty"`
	Type          string `json:"type"`
	Protobuf      string `json:"protobuf,omitempty"`
	Active        *bool  `json:"active,omitempty"`
	BaselineID    string `json:"baselineId,omitempty"`
	ServerCursor  uint64 `json:"serverCursor,omitempty"`
	ServerVersion uint64 `json:"serverVersion,omitempty"`
	Code          string `json:"code,omitempty"`
	RetryAfterMS  uint64 `json:"retryAfterMs,omitempty"`
}

func DecodeClientFrame(message []byte) (ClientFrame, error) {
	if len(message) > MaxWebSocketFrame {
		return ClientFrame{}, frameError("frame too large", 1009, "")
	}
	var frame wireFrame
	if err := json.Unmarshal(message, &frame); err != nil {
		return ClientFrame{}, frameError("invalid JSON", 1007, "")
	}
	if frame.Version != SyncWebSocketVersion {
		return ClientFrame{}, frameError("unsupported version", 0, "UNSUPPORTED_VERSION")
	}
	if frame.Type == "activity" {
		if frame.Active == nil {
			return ClientFrame{}, frameError("missing activity", 0, "")
		}
		return ClientFrame{Version: frame.Version, Type: frame.Type, Active: *frame.Active}, nil
	}
	if frame.Type != "exchange" && frame.Type != "resolve" {
		return ClientFrame{}, frameError("unsupported request type", 0, "")
	}
	if !requestIDPattern.MatchString(frame.RequestID) {
		return ClientFrame{}, frameError("invalid requestId", 0, "")
	}
	if len(frame.Protobuf) > MaxSyncEnvelopeBytes {
		return ClientFrame{}, frameError("protobuf envelope too large", 1009, "")
	}
	protobuf, err := base64.StdEncoding.Strict().DecodeString(frame.Protobuf)
	if err != nil {
		return ClientFrame{}, frameError("invalid protobuf base64", 0, "")
	}
	if len(protobuf) > MaxProtobufBytes {
		return ClientFrame{}, frameError("protobuf too large", 1009, "")
	}
	return ClientFrame{Version: frame.Version, RequestID: frame.RequestID, Type: frame.Type, Protobuf: protobuf}, nil
}

func EncodeServerResult(resultType, requestID string, protobuf []byte) ([]byte, error) {
	if resultType != "exchange_result" && resultType != "resolve_result" {
		return nil, fmt.Errorf("unsupported result type")
	}
	if !requestIDPattern.MatchString(requestID) || len(protobuf) > MaxProtobufBytes {
		return nil, fmt.Errorf("invalid result")
	}
	return json.Marshal(wireFrame{
		Version:   SyncWebSocketVersion,
		RequestID: requestID,
		Type:      resultType,
		Protobuf:  base64.StdEncoding.EncodeToString(protobuf),
	})
}

func EncodeSyncHint(baselineID string, cursor, version uint64) ([]byte, error) {
	if !baselinePattern.MatchString(baselineID) {
		return nil, fmt.Errorf("invalid baseline ID")
	}
	return json.Marshal(wireFrame{
		Version:       SyncWebSocketVersion,
		Type:          "sync_hint",
		BaselineID:    baselineID,
		ServerCursor:  cursor,
		ServerVersion: version,
	})
}

func EncodeServerError(requestID, code string, retryAfterMS uint64) ([]byte, error) {
	allowed := map[string]bool{
		"INVALID_ARGUMENT":    true,
		"UNSUPPORTED_VERSION": true,
		"RATE_LIMITED":        true,
		"FAILED_PRECONDITION": true,
		"AUTH_REQUIRED":       true,
		"INTERNAL":            true,
	}
	if !allowed[code] || (requestID != "" && !requestIDPattern.MatchString(requestID)) {
		return nil, fmt.Errorf("invalid error frame")
	}
	return json.Marshal(wireFrame{
		Version:      SyncWebSocketVersion,
		RequestID:    requestID,
		Type:         "error",
		Code:         code,
		RetryAfterMS: retryAfterMS,
	})
}
