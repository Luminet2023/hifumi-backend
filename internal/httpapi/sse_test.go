package httpapi

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"github.com/Luminet2023/hifumi-backend/internal/auth"
	"github.com/Luminet2023/hifumi-backend/internal/config"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/proto"
)

const (
	sseTestBaselineA = "baseline_0123456789abcdef0123456789abcdef"
	sseTestBaselineB = "baseline_fedcba9876543210fedcba9876543210"
	sseTestOrigin    = "https://stellafortuna.luminet.cn"
	sseTestSubject   = "sse-test-subject"
)

type sseTestSync struct {
	fakeSync

	readFeed func(context.Context, string, string, uint64, int) (*syncservice.FeedPage, error)
	diff     func(context.Context, syncservice.CommandMetadata, *syncv1.DiffRequest) (*syncservice.DiffResult, error)
}

func (s *sseTestSync) ReadFeedPage(
	ctx context.Context,
	ownerKey, baselineID string,
	cursor uint64,
	limit int,
) (*syncservice.FeedPage, error) {
	if s.readFeed != nil {
		return s.readFeed(ctx, ownerKey, baselineID, cursor, limit)
	}
	return s.fakeSync.ReadFeedPage(ctx, ownerKey, baselineID, cursor, limit)
}

func (s *sseTestSync) Diff(
	ctx context.Context,
	metadata syncservice.CommandMetadata,
	request *syncv1.DiffRequest,
) (*syncservice.DiffResult, error) {
	if s.diff != nil {
		return s.diff(ctx, metadata, request)
	}
	return s.fakeSync.Diff(ctx, metadata, request)
}

type sseTestRealtime struct {
	fakeRealtime

	mu             sync.Mutex
	acquireErr     error
	acquireCalls   int
	acquireOwner   string
	acquireMaximum int
	acquireTTL     time.Duration
	rateOperations []string
}

func (r *sseTestRealtime) CheckFixedWindow(
	_ context.Context,
	operation, _ string,
	_ int,
	_ time.Duration,
) (time.Duration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rateOperations = append(r.rateOperations, operation)
	return 0, nil
}

func (r *sseTestRealtime) AcquireConnection(
	_ context.Context,
	ownerKey, _ string,
	maximum int,
	ttl time.Duration,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acquireCalls++
	r.acquireOwner = ownerKey
	r.acquireMaximum = maximum
	r.acquireTTL = ttl
	return r.acquireErr
}

func (r *sseTestRealtime) acquisition() (int, string, int, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acquireCalls, r.acquireOwner, r.acquireMaximum, r.acquireTTL
}

func (r *sseTestRealtime) operations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.rateOperations...)
}

type sseTestEvent struct {
	ID    string
	Event string
	Data  []byte
}

func newSSETestServer(
	t *testing.T,
	syncClient SyncService,
	realtimeClient RealtimeClient,
	wakeHub *realtime.WakeHub,
	claims *auth.SessionClaims,
	now func() time.Time,
) (*Server, *httptest.Server) {
	t.Helper()
	publicBase, err := url.Parse("https://api.luminet.cn/hifumi/")
	if err != nil {
		t.Fatal(err)
	}
	frontendReturn, err := url.Parse(sseTestOrigin + "/")
	if err != nil {
		t.Fatal(err)
	}
	if wakeHub == nil {
		wakeHub = realtime.NewWakeHub()
	}
	server, err := NewServer(Dependencies{
		Config: config.Config{
			PublicBaseURL: publicBase,
			FrontendOrigins: []string{
				"https://stellafortuna.hifumi.luminet.cn",
				sseTestOrigin,
			},
			FrontendReturnURL: frontendReturn,
		},
		Tokens:   &fakeTokens{claims: claims},
		OAuth:    &fakeOAuth{},
		Profiles: &fakeProfiles{},
		Sync:     syncClient,
		Realtime: realtimeClient,
		Hub:      realtime.NewHub(),
		WakeHub:  wakeHub,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:      now,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		server.CloseStreams()
		httpServer.Close()
	})
	return server, httpServer
}

func sseTestClaims(expiresAt time.Time) *auth.SessionClaims {
	return &auth.SessionClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject:   sseTestSubject,
		ExpiresAt: &jwt.NumericDate{Time: expiresAt},
	}}
}

func newSSETestRequest(t *testing.T, ctx context.Context, endpoint string) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", sseTestOrigin)
	request.Header.Set("Referer", sseTestOrigin+"/day/2026-07-13")
	request.Header.Set("Accept", "text/event-stream")
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "test-session"})
	return request
}

func openSSETestStream(
	t *testing.T,
	ctx context.Context,
	httpServer *httptest.Server,
	baselineID string,
	cursor uint64,
) *http.Response {
	t.Helper()
	endpoint := httpServer.URL + "/hifumi/v1/sync/events?baselineId=" + url.QueryEscape(baselineID) +
		"&cursor=" + strconv.FormatUint(cursor, 10)
	response, err := http.DefaultClient.Do(newSSETestRequest(t, ctx, endpoint))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func readSSETestEvent(t *testing.T, reader *bufio.Reader) sseTestEvent {
	t.Helper()
	var event sseTestEvent
	var data []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE event: %v", err)
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			if event.Event == "" && event.ID == "" && len(data) == 0 {
				continue
			}
			event.Data = []byte(strings.Join(data, "\n"))
			return event
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			event.ID = value
		case "event":
			event.Event = value
		case "data":
			data = append(data, value)
		}
	}
}

func decodeSSETestSyncResponse(t *testing.T, event sseTestEvent) *syncv1.SyncResponse {
	t.Helper()
	var envelope sseProtobufEnvelope
	if err := json.Unmarshal(event.Data, &envelope); err != nil {
		t.Fatalf("decode SSE JSON envelope %q: %v", event.Data, err)
	}
	if envelope.Version != 1 || envelope.Protobuf == "" {
		t.Fatalf("unexpected SSE protobuf envelope: %+v", envelope)
	}
	encoded, err := base64.StdEncoding.Strict().DecodeString(envelope.Protobuf)
	if err != nil {
		t.Fatalf("decode SSE protobuf base64: %v", err)
	}
	response := &syncv1.SyncResponse{}
	if err := proto.Unmarshal(encoded, response); err != nil {
		t.Fatalf("decode SSE protobuf: %v", err)
	}
	return response
}

func assertSSETestErrorEnvelope(t *testing.T, event sseTestEvent, code string) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(event.Data, &envelope); err != nil {
		t.Fatalf("decode SSE error envelope %q: %v", event.Data, err)
	}
	if len(envelope) != 2 || envelope["version"] != float64(1) || envelope["code"] != code {
		t.Fatalf("unexpected SSE error envelope: %#v", envelope)
	}
	if _, exists := envelope["error"]; exists {
		t.Fatalf("legacy error field leaked into SSE error envelope: %#v", envelope)
	}
}

func TestSSERequiresStrictBrowserRequest(t *testing.T) {
	service := &sseTestSync{}
	realtimeClient := &sseTestRealtime{}
	_, httpServer := newSSETestServer(
		t,
		service,
		realtimeClient,
		nil,
		sseTestClaims(time.Now().Add(time.Minute)),
		time.Now,
	)
	validEndpoint := httpServer.URL + "/hifumi/v1/sync/events?baselineId=" + sseTestBaselineA + "&cursor=0"

	tests := []struct {
		name       string
		endpoint   string
		mutate     func(*http.Request)
		wantStatus int
		wantError  string
	}{
		{
			name: "missing origin", mutate: func(request *http.Request) { request.Header.Del("Origin") },
			wantStatus: http.StatusForbidden, wantError: "invalid_origin",
		},
		{
			name: "foreign origin", mutate: func(request *http.Request) { request.Header.Set("Origin", "https://evil.example") },
			wantStatus: http.StatusForbidden, wantError: "invalid_origin",
		},
		{
			name: "missing referer", mutate: func(request *http.Request) { request.Header.Del("Referer") },
			wantStatus: http.StatusForbidden, wantError: "invalid_referer",
		},
		{
			name: "mismatched allowlisted referer", mutate: func(request *http.Request) {
				request.Header.Set("Referer", "https://stellafortuna.hifumi.luminet.cn/")
			},
			wantStatus: http.StatusForbidden, wantError: "invalid_referer",
		},
		{
			name: "missing cookie", mutate: func(request *http.Request) { request.Header.Del("Cookie") },
			wantStatus: http.StatusUnauthorized, wantError: "authentication_required",
		},
		{
			name: "missing accept", mutate: func(request *http.Request) { request.Header.Del("Accept") },
			wantStatus: http.StatusNotAcceptable, wantError: "event_stream_required",
		},
		{
			name: "wrong accept", mutate: func(request *http.Request) { request.Header.Set("Accept", "application/json") },
			wantStatus: http.StatusNotAcceptable, wantError: "event_stream_required",
		},
		{
			name: "missing baseline", endpoint: httpServer.URL + "/hifumi/v1/sync/events?cursor=0",
			wantStatus: http.StatusBadRequest, wantError: "invalid_sync_cursor",
		},
		{
			name: "duplicate baseline", endpoint: httpServer.URL + "/hifumi/v1/sync/events?baselineId=" + sseTestBaselineA + "&baselineId=" + sseTestBaselineA + "&cursor=0",
			wantStatus: http.StatusBadRequest, wantError: "invalid_sync_cursor",
		},
		{
			name: "missing cursor", endpoint: httpServer.URL + "/hifumi/v1/sync/events?baselineId=" + sseTestBaselineA,
			wantStatus: http.StatusBadRequest, wantError: "invalid_sync_cursor",
		},
		{
			name: "negative cursor", endpoint: httpServer.URL + "/hifumi/v1/sync/events?baselineId=" + sseTestBaselineA + "&cursor=-1",
			wantStatus: http.StatusBadRequest, wantError: "invalid_sync_cursor",
		},
		{
			name: "unsafe cursor", endpoint: httpServer.URL + "/hifumi/v1/sync/events?baselineId=" + sseTestBaselineA + "&cursor=9007199254740992",
			wantStatus: http.StatusBadRequest, wantError: "invalid_sync_cursor",
		},
		{
			name: "malformed baseline", endpoint: httpServer.URL + "/hifumi/v1/sync/events?baselineId=baseline_bad&cursor=0",
			wantStatus: http.StatusBadRequest, wantError: "invalid_sync_cursor",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			endpoint := test.endpoint
			if endpoint == "" {
				endpoint = validEndpoint
			}
			request := newSSETestRequest(t, ctx, endpoint)
			if test.mutate != nil {
				test.mutate(request)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.wantStatus || !strings.Contains(string(body), `"error":"`+test.wantError+`"`) {
				t.Fatalf("got status=%d body=%q, want status=%d error=%q", response.StatusCode, body, test.wantStatus, test.wantError)
			}
		})
	}
}

func TestSSEEmptyBootstrapSendsReadyAndStreamingHeaders(t *testing.T) {
	service := &sseTestSync{readFeed: func(
		_ context.Context,
		ownerKey, baselineID string,
		cursor uint64,
		limit int,
	) (*syncservice.FeedPage, error) {
		if ownerKey != auth.OwnerKey(sseTestSubject) || baselineID != sseTestBaselineA || cursor != 0 || limit != sseFeedPageLimit {
			return nil, fmt.Errorf("unexpected feed request owner=%q baseline=%q cursor=%d limit=%d", ownerKey, baselineID, cursor, limit)
		}
		return &syncservice.FeedPage{
			BaselineID:        baselineID,
			NextCursor:        cursor,
			HeadCursor:        cursor,
			ServerVersion:     4,
			ServerUpdatedAtMs: 1234,
			ServerProgressDay: "2026-07-13",
		}, nil
	}}
	realtimeClient := &sseTestRealtime{}
	_, httpServer := newSSETestServer(
		t,
		service,
		realtimeClient,
		nil,
		sseTestClaims(time.Now().Add(time.Minute)),
		time.Now,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	response := openSSETestStream(t, ctx, httpServer, sseTestBaselineA, 0)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("unexpected SSE status %d", response.StatusCode)
	}
	if got := response.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		cancel()
		t.Fatalf("unexpected Content-Type %q", got)
	}
	if got := response.Header.Get("Cache-Control"); got != "no-cache, no-store, no-transform" {
		cancel()
		t.Fatalf("unexpected Cache-Control %q", got)
	}
	if got := response.Header.Get("X-Accel-Buffering"); got != "no" {
		cancel()
		t.Fatalf("unexpected X-Accel-Buffering %q", got)
	}
	if got := response.Header.Get("Connection"); got != "" {
		cancel()
		t.Fatalf("SSE must not force an HTTP/1-only Connection header, got %q", got)
	}

	event := readSSETestEvent(t, bufio.NewReader(response.Body))
	if event.Event != "ready" || event.ID != sseTestBaselineA+":0" {
		cancel()
		t.Fatalf("unexpected ready event: %+v", event)
	}
	message := decodeSSETestSyncResponse(t, event)
	if message.GetNextCursor() != 0 || len(message.GetChanges()) != 0 || message.GetBaselineId() != sseTestBaselineA ||
		message.GetServerVersion() != 4 || message.GetServerProgressDay() != "2026-07-13" {
		cancel()
		t.Fatalf("unexpected ready payload: %+v", message)
	}
	cancel()
}

func TestSSEPagesChangesInCursorOrderThenSendsReady(t *testing.T) {
	var mu sync.Mutex
	var requestedCursors []uint64
	service := &sseTestSync{readFeed: func(
		_ context.Context,
		ownerKey, baselineID string,
		cursor uint64,
		limit int,
	) (*syncservice.FeedPage, error) {
		mu.Lock()
		requestedCursors = append(requestedCursors, cursor)
		mu.Unlock()
		if ownerKey != auth.OwnerKey(sseTestSubject) || baselineID != sseTestBaselineA || limit != sseFeedPageLimit {
			return nil, fmt.Errorf("unexpected feed request owner=%q baseline=%q limit=%d", ownerKey, baselineID, limit)
		}
		page := &syncservice.FeedPage{
			BaselineID:        sseTestBaselineA,
			HeadCursor:        3,
			ServerVersion:     9,
			ServerUpdatedAtMs: 999,
			ServerProgressDay: "2026-07-15",
		}
		switch cursor {
		case 0:
			page.Changes = []*syncv1.Change{{Cursor: 1, EntityKey: "stella/v1/a"}, {Cursor: 2, EntityKey: "stella/v1/b"}}
			page.NextCursor = 2
			page.HasMore = true
		case 2:
			page.Changes = []*syncv1.Change{{Cursor: 3, EntityKey: "stella/v1/c"}}
			page.NextCursor = 3
		default:
			return nil, fmt.Errorf("unexpected cursor %d", cursor)
		}
		return page, nil
	}}
	_, httpServer := newSSETestServer(
		t,
		service,
		&sseTestRealtime{},
		nil,
		sseTestClaims(time.Now().Add(time.Minute)),
		time.Now,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	response := openSSETestStream(t, ctx, httpServer, sseTestBaselineA, 0)
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)

	first := readSSETestEvent(t, reader)
	second := readSSETestEvent(t, reader)
	ready := readSSETestEvent(t, reader)
	if first.Event != "changes" || first.ID != sseTestBaselineA+":2" || second.Event != "changes" || second.ID != sseTestBaselineA+":3" ||
		ready.Event != "ready" || ready.ID != sseTestBaselineA+":3" {
		cancel()
		t.Fatalf("unexpected event sequence: first=%+v second=%+v ready=%+v", first, second, ready)
	}
	firstMessage := decodeSSETestSyncResponse(t, first)
	secondMessage := decodeSSETestSyncResponse(t, second)
	readyMessage := decodeSSETestSyncResponse(t, ready)
	if got := []uint64{firstMessage.GetChanges()[0].GetCursor(), firstMessage.GetChanges()[1].GetCursor(), secondMessage.GetChanges()[0].GetCursor()}; got[0] != 1 || got[1] != 2 || got[2] != 3 {
		cancel()
		t.Fatalf("changes are not cursor ordered: %v", got)
	}
	if firstMessage.GetNextCursor() != 2 || !firstMessage.GetHasMore() || secondMessage.GetNextCursor() != 3 || secondMessage.GetHasMore() ||
		readyMessage.GetNextCursor() != 3 || len(readyMessage.GetChanges()) != 0 {
		cancel()
		t.Fatalf("unexpected pagination payloads: first=%+v second=%+v ready=%+v", firstMessage, secondMessage, readyMessage)
	}
	mu.Lock()
	cursors := append([]uint64(nil), requestedCursors...)
	mu.Unlock()
	if fmt.Sprint(cursors) != "[0 2]" {
		cancel()
		t.Fatalf("unexpected feed cursor sequence %v", cursors)
	}
	cancel()
}

func TestSSETerminalFeedEventsCloseStream(t *testing.T) {
	tests := []struct {
		name          string
		page          *syncservice.FeedPage
		wantEvent     string
		wantID        string
		wantMismatch  bool
		wantReset     bool
		requestCursor uint64
	}{
		{
			name: "baseline mismatch",
			page: &syncservice.FeedPage{
				BaselineID: sseTestBaselineB, NextCursor: 8, HeadCursor: 8, ServerVersion: 2, BaselineMismatch: true,
			},
			wantEvent: "baseline_mismatch", wantID: sseTestBaselineB + ":5", wantMismatch: true, requestCursor: 5,
		},
		{
			name: "reset required",
			page: &syncservice.FeedPage{
				BaselineID: sseTestBaselineA, NextCursor: 9, HeadCursor: 9, ServerVersion: 3, ResetRequired: true,
			},
			wantEvent: "reset_required", wantID: sseTestBaselineA + ":0", wantReset: true, requestCursor: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &sseTestSync{readFeed: func(
				_ context.Context, _, _ string, _ uint64, _ int,
			) (*syncservice.FeedPage, error) {
				return test.page, nil
			}}
			_, httpServer := newSSETestServer(
				t,
				service,
				&sseTestRealtime{},
				nil,
				sseTestClaims(time.Now().Add(time.Minute)),
				time.Now,
			)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			response := openSSETestStream(t, ctx, httpServer, sseTestBaselineA, test.requestCursor)
			defer response.Body.Close()
			reader := bufio.NewReader(response.Body)
			event := readSSETestEvent(t, reader)
			if event.Event != test.wantEvent || event.ID != test.wantID {
				t.Fatalf("unexpected terminal event: %+v", event)
			}
			message := decodeSSETestSyncResponse(t, event)
			if message.GetBaselineMismatch() != test.wantMismatch || message.GetResetRequired() != test.wantReset {
				t.Fatalf("unexpected terminal payload: %+v", message)
			}
			remainder, err := io.ReadAll(reader)
			if err != nil || len(remainder) != 0 {
				t.Fatalf("terminal SSE stream did not close cleanly: remainder=%q err=%v", remainder, err)
			}
		})
	}
}

func TestSSEWakeHubReadsAndReturnsOwnAuthoritativeChange(t *testing.T) {
	var mu sync.Mutex
	reads := 0
	service := &sseTestSync{readFeed: func(
		_ context.Context,
		ownerKey, baselineID string,
		cursor uint64,
		_ int,
	) (*syncservice.FeedPage, error) {
		if ownerKey != auth.OwnerKey(sseTestSubject) || baselineID != sseTestBaselineA || cursor != 0 {
			return nil, fmt.Errorf("unexpected feed request owner=%q baseline=%q cursor=%d", ownerKey, baselineID, cursor)
		}
		mu.Lock()
		defer mu.Unlock()
		reads++
		page := &syncservice.FeedPage{BaselineID: sseTestBaselineA, ServerVersion: 1, HeadCursor: 1}
		if reads == 1 {
			return page, nil
		}
		page.Changes = []*syncv1.Change{{
			Cursor: 1, EntityKey: "stella/v1/day/2026-07-15", DeviceId: "device_alpha", OpId: "own_operation_001", ValueJson: []byte(`{"done":true}`),
		}}
		page.NextCursor = 1
		return page, nil
	}}
	wakeHub := realtime.NewWakeHub()
	_, httpServer := newSSETestServer(
		t,
		service,
		&sseTestRealtime{},
		wakeHub,
		sseTestClaims(time.Now().Add(time.Minute)),
		time.Now,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	response := openSSETestStream(t, ctx, httpServer, sseTestBaselineA, 0)
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	ready := readSSETestEvent(t, reader)
	if ready.Event != "ready" {
		cancel()
		t.Fatalf("unexpected initial event %+v", ready)
	}

	// Wake-ups carry no payload or origin filter. The stream must re-read MySQL
	// and return the authoritative change even when it belongs to this device.
	wakeHub.Publish(auth.OwnerKey(sseTestSubject))
	changes := readSSETestEvent(t, reader)
	message := decodeSSETestSyncResponse(t, changes)
	if changes.Event != "changes" || changes.ID != sseTestBaselineA+":1" || len(message.GetChanges()) != 1 {
		cancel()
		t.Fatalf("unexpected wake-up event=%+v payload=%+v", changes, message)
	}
	change := message.GetChanges()[0]
	if change.GetCursor() != 1 || change.GetDeviceId() != "device_alpha" || change.GetOpId() != "own_operation_001" {
		cancel()
		t.Fatalf("own authoritative change was filtered or rewritten: %+v", change)
	}
	cancel()
}

func TestSSEConnectionLeaseFailuresFailClosed(t *testing.T) {
	tests := []struct {
		name       string
		acquireErr error
		wantStatus int
		wantError  string
		wantRetry  string
	}{
		{
			name: "ninth connection", acquireErr: realtime.ErrConnectionLimit,
			wantStatus: http.StatusTooManyRequests, wantError: "sync_connection_limit", wantRetry: "5",
		},
		{
			name: "redis failure", acquireErr: errors.New("redis unavailable"),
			wantStatus: http.StatusServiceUnavailable, wantError: "redis_unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			realtimeClient := &sseTestRealtime{acquireErr: test.acquireErr}
			_, httpServer := newSSETestServer(
				t,
				&sseTestSync{},
				realtimeClient,
				nil,
				sseTestClaims(time.Now().Add(time.Minute)),
				time.Now,
			)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			response := openSSETestStream(t, ctx, httpServer, sseTestBaselineA, 0)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.wantStatus || !strings.Contains(string(body), `"error":"`+test.wantError+`"`) {
				t.Fatalf("got status=%d body=%q, want status=%d error=%q", response.StatusCode, body, test.wantStatus, test.wantError)
			}
			if got := response.Header.Get("Retry-After"); got != test.wantRetry {
				t.Fatalf("unexpected Retry-After %q", got)
			}
			calls, ownerKey, maximum, ttl := realtimeClient.acquisition()
			if calls != 1 || ownerKey != auth.OwnerKey(sseTestSubject) || maximum != 8 || ttl != realtime.DefaultLeaseTTL {
				t.Fatalf("unexpected lease request calls=%d owner=%q maximum=%d ttl=%s", calls, ownerKey, maximum, ttl)
			}
		})
	}
}

func TestSSESessionExpirySendsAuthRequiredAndCloses(t *testing.T) {
	start := time.Now()
	expiresAt := start.Add(75 * time.Millisecond)
	_, httpServer := newSSETestServer(
		t,
		&sseTestSync{},
		&sseTestRealtime{},
		nil,
		sseTestClaims(expiresAt),
		func() time.Time { return start },
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response := openSSETestStream(t, ctx, httpServer, sseTestBaselineA, 0)
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	ready := readSSETestEvent(t, reader)
	if ready.Event != "ready" {
		t.Fatalf("unexpected initial event %+v", ready)
	}
	authRequired := readSSETestEvent(t, reader)
	if authRequired.Event != "auth_required" || authRequired.ID != "" {
		t.Fatalf("unexpected expiry event %+v", authRequired)
	}
	assertSSETestErrorEnvelope(t, authRequired, "auth_required")
	remainder, err := io.ReadAll(reader)
	if err != nil || len(remainder) != 0 {
		t.Fatalf("expired SSE stream did not close cleanly: remainder=%q err=%v", remainder, err)
	}
}

func TestSSEFeedFailureSendsUnavailableCodeAndCloses(t *testing.T) {
	service := &sseTestSync{readFeed: func(
		context.Context,
		string,
		string,
		uint64,
		int,
	) (*syncservice.FeedPage, error) {
		return nil, errors.New("mysql read failed")
	}}
	_, httpServer := newSSETestServer(
		t,
		service,
		&sseTestRealtime{},
		nil,
		sseTestClaims(time.Now().Add(time.Minute)),
		time.Now,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response := openSSETestStream(t, ctx, httpServer, sseTestBaselineA, 0)
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	unavailable := readSSETestEvent(t, reader)
	if unavailable.Event != "unavailable" || unavailable.ID != "" {
		t.Fatalf("unexpected unavailable event %+v", unavailable)
	}
	assertSSETestErrorEnvelope(t, unavailable, "unavailable")
	remainder, err := io.ReadAll(reader)
	if err != nil || len(remainder) != 0 {
		t.Fatalf("unavailable SSE stream did not close cleanly: remainder=%q err=%v", remainder, err)
	}
}

func TestDiffHTTPUsesDeterministicJSONBase64Protobuf(t *testing.T) {
	requestMessage := &syncv1.DiffRequest{
		DeviceId: "device_alpha", BaselineId: sseTestBaselineA, LocalVersion: 6, LocalUpdatedAtMs: 12345, LocalProgressDay: "2026-07-15",
	}
	responseMessage := &syncv1.DiffResponse{
		Acks: []*syncv1.MutationAck{{OpId: "operation_alpha", ServerCursor: 7, Applied: true}},
		CanonicalChanges: []*syncv1.Change{{
			Cursor: 7, EntityKey: "stella/v1/day/2026-07-15", ValueJson: []byte(`{"value":1}`), DeviceId: "device_alpha", OpId: "operation_alpha",
		}},
		BaselineId: sseTestBaselineA, ServerCursor: 7, ServerVersion: 8, ServerUpdatedAtMs: 12346, ServerProgressDay: "2026-07-15",
	}
	var mu sync.Mutex
	var gotOwner string
	var gotRequest *syncv1.DiffRequest
	service := &sseTestSync{diff: func(
		_ context.Context,
		metadata syncservice.CommandMetadata,
		request *syncv1.DiffRequest,
	) (*syncservice.DiffResult, error) {
		mu.Lock()
		defer mu.Unlock()
		gotOwner = metadata.OwnerKey
		gotRequest = proto.Clone(request).(*syncv1.DiffRequest)
		return &syncservice.DiffResult{Response: responseMessage, StateChanged: true}, nil
	}}
	realtimeClient := &sseTestRealtime{}
	_, httpServer := newSSETestServer(
		t,
		service,
		realtimeClient,
		nil,
		sseTestClaims(time.Now().Add(time.Minute)),
		time.Now,
	)
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(requestMessage)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"protobuf":"` + base64.StdEncoding.EncodeToString(requestBytes) + `"}`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, httpServer.URL+"/hifumi/v1/sync/diff", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", sseTestOrigin)
	request.Header.Set("Referer", sseTestOrigin+"/")
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "test-session"})
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(responseMessage)
	if err != nil {
		t.Fatal(err)
	}
	wantBody := `{"protobuf":"` + base64.StdEncoding.EncodeToString(wantBytes) + `"}` + "\n"
	if response.StatusCode != http.StatusOK || string(responseBody) != wantBody {
		t.Fatalf("got status=%d body=%q, want status=200 body=%q", response.StatusCode, responseBody, wantBody)
	}
	if response.Header.Get("Content-Type") != "application/json; charset=utf-8" || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected Diff headers: %v", response.Header)
	}
	if operations := realtimeClient.operations(); len(operations) != 1 || operations[0] != "exchange" {
		t.Fatalf("Diff did not share the exchange rate bucket: %v", operations)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotOwner != auth.OwnerKey(sseTestSubject) || !proto.Equal(gotRequest, requestMessage) {
		t.Fatalf("unexpected Diff service call owner=%q request=%+v", gotOwner, gotRequest)
	}
}

func TestDiffHTTPMapsResponseTooLargeTo413(t *testing.T) {
	service := &sseTestSync{diff: func(
		context.Context,
		syncservice.CommandMetadata,
		*syncv1.DiffRequest,
	) (*syncservice.DiffResult, error) {
		return nil, syncservice.ErrResponseTooLarge
	}}
	_, httpServer := newSSETestServer(
		t,
		service,
		&sseTestRealtime{},
		nil,
		sseTestClaims(time.Now().Add(time.Minute)),
		time.Now,
	)
	requestMessage := &syncv1.DiffRequest{DeviceId: "device_alpha", BaselineId: sseTestBaselineA}
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(requestMessage)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"protobuf":"` + base64.StdEncoding.EncodeToString(requestBytes) + `"}`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, httpServer.URL+"/hifumi/v1/sync/diff", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", sseTestOrigin)
	request.Header.Set("Referer", sseTestOrigin+"/")
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "test-session"})
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusRequestEntityTooLarge || string(responseBody) != `{"error":"sync_response_too_large"}`+"\n" {
		t.Fatalf("unexpected oversized Diff response: status=%d body=%q", response.StatusCode, responseBody)
	}
}
