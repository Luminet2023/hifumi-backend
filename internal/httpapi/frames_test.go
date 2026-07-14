package httpapi

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func TestDecodeClientFrame(t *testing.T) {
	payload := []byte{1, 2, 3}
	frame, err := DecodeClientFrame([]byte(`{"version":1,"requestId":"req_123456789012","type":"exchange","protobuf":"` + base64.StdEncoding.EncodeToString(payload) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != "exchange" || string(frame.Protobuf) != string(payload) {
		t.Fatalf("unexpected frame: %+v", frame)
	}
}

func TestDecodeClientFrameRejectsInvalidBase64(t *testing.T) {
	_, err := DecodeClientFrame([]byte(`{"version":1,"requestId":"req_123456789012","type":"exchange","protobuf":"!!!!"}`))
	if err == nil {
		t.Fatal("expected invalid base64")
	}
}

func TestDecodeClientFrameRejectsOversize(t *testing.T) {
	_, err := DecodeClientFrame([]byte(strings.Repeat("x", MaxWebSocketFrame+1)))
	var frameError *FrameError
	if !errors.As(err, &frameError) || frameError.CloseCode != 1009 {
		t.Fatalf("expected close 1009, got %#v", err)
	}
}

func TestDecodeClientFramePreservesProtocolErrorSemantics(t *testing.T) {
	_, err := DecodeClientFrame([]byte(`{`))
	var frameError *FrameError
	if !errors.As(err, &frameError) || frameError.CloseCode != 1007 {
		t.Fatalf("invalid JSON should close 1007, got %#v", err)
	}
	_, err = DecodeClientFrame([]byte(`{"version":2,"type":"activity","active":true}`))
	if !errors.As(err, &frameError) || frameError.CloseCode != 0 || frameError.Code != "UNSUPPORTED_VERSION" {
		t.Fatalf("unexpected version error: %#v", err)
	}
}

func TestEncodeSyncHint(t *testing.T) {
	message, err := EncodeSyncHint("baseline_0123456789abcdef0123456789abcdef", 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(message), `"type":"sync_hint"`) {
		t.Fatalf("unexpected hint: %s", message)
	}
}
