package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"github.com/Luminet2023/hifumi-backend/internal/auth"
	"github.com/Luminet2023/hifumi-backend/internal/config"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"github.com/coder/websocket"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/proto"
)

type fakeTokens struct {
	claims *auth.SessionClaims
}

func (f *fakeTokens) SignSession(auth.Profile) (string, time.Time, error) {
	return "signed.token", time.Now().Add(auth.SessionLifetime), nil
}
func (f *fakeTokens) VerifySession(string, bool) (*auth.SessionClaims, error) {
	if f.claims == nil {
		return nil, auth.ErrInvalidToken
	}
	return f.claims, nil
}
func (f *fakeTokens) VerifyOAuthState(string, string) (*auth.OAuthClaims, error) {
	return nil, auth.ErrInvalidToken
}

type fakeOAuth struct{ compat bool }

func (f *fakeOAuth) Begin(compat bool) (auth.Login, error) {
	f.compat = compat
	return auth.Login{AuthorizationURL: "https://connect.linux.do/oauth2/authorize", StateToken: "state.jwt"}, nil
}
func (*fakeOAuth) Complete(context.Context, string, *auth.OAuthClaims) (auth.Profile, error) {
	return auth.Profile{}, errors.New("not implemented")
}

type fakeProfiles struct{}

func (*fakeProfiles) UpsertProfile(_ context.Context, _ string, profile auth.Profile, _ uint64) (auth.Profile, error) {
	return profile, nil
}

type fakeSync struct{}

func (*fakeSync) Exchange(context.Context, syncservice.CommandMetadata, *syncv1.SyncRequest) (*syncservice.ExchangeResult, error) {
	return &syncservice.ExchangeResult{Response: &syncv1.SyncResponse{
		NextCursor: 3, BaselineId: "baseline_0123456789abcdef0123456789abcdef", ServerVersion: 2,
	}}, nil
}
func (*fakeSync) ResolveBaseline(context.Context, syncservice.CommandMetadata, *syncv1.ResolveBaselineRequest) (*syncservice.ResolveResult, error) {
	return &syncservice.ResolveResult{Response: &syncv1.ResolveBaselineResponse{}}, nil
}

type fakeRealtime struct {
	handoff string
}

func (*fakeRealtime) Ping(context.Context) error { return nil }
func (*fakeRealtime) CheckFixedWindow(context.Context, string, string, int, time.Duration) (time.Duration, error) {
	return 0, nil
}
func (*fakeRealtime) AcquireConnection(context.Context, string, string, int, time.Duration) error {
	return nil
}
func (*fakeRealtime) RefreshConnection(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (*fakeRealtime) ReleaseConnection(context.Context, string, string) error { return nil }
func (*fakeRealtime) PutHandoff(context.Context, string, time.Duration) (string, error) {
	return "handoff_0123456789abcdef", nil
}
func (f *fakeRealtime) ConsumeHandoff(context.Context, string) (string, error) {
	return f.handoff, nil
}

func testServer(t *testing.T, tokens *fakeTokens, oauth *fakeOAuth, redis *fakeRealtime) *Server {
	t.Helper()
	publicBase, _ := url.Parse("https://api.luminet.cn/hifumi/")
	frontendReturn, _ := url.Parse("https://stellafortuna.luminet.cn/")
	server, err := NewServer(Dependencies{
		Config: config.Config{
			PublicBaseURL: publicBase, FrontendOrigin: "https://stellafortuna.luminet.cn",
			FrontendReturnURL: frontendReturn, CompatProxySecret: strings.Repeat("c", 32),
		},
		Tokens: tokens, OAuth: oauth, Profiles: &fakeProfiles{}, Sync: &fakeSync{},
		Realtime: redis, Hub: realtime.NewHub(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Build:  BuildInfo{Version: "test", Commit: "abc", BuildTime: "now"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestSessionUnauthenticatedAndCORS(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	request := httptest.NewRequest(http.MethodGet, "/hifumi/api/v1/auth/session", nil)
	request.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"authenticated":false`) {
		t.Fatalf("unexpected response: %d %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Access-Control-Allow-Origin"); got != "https://stellafortuna.luminet.cn" {
		t.Fatalf("unexpected CORS origin %q", got)
	}
	if response.Header().Get("Access-Control-Allow-Credentials") != "true" || response.Header().Get("Vary") != "Origin" {
		t.Fatal("credentialed CORS headers are incomplete")
	}
	malicious := httptest.NewRequest(http.MethodGet, "/hifumi/api/v1/auth/session", nil)
	malicious.Header.Set("Origin", "https://evil.example")
	maliciousResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(maliciousResponse, malicious)
	if maliciousResponse.Code != http.StatusForbidden {
		t.Fatalf("malicious CORS origin returned %d", maliciousResponse.Code)
	}
}

func TestStateChangingOriginAndCookiePath(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	bad := httptest.NewRequest(http.MethodPost, "/hifumi/api/v1/auth/logout", nil)
	bad.Header.Set("Origin", "https://evil.example")
	badResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(badResponse, bad)
	if badResponse.Code != http.StatusForbidden {
		t.Fatalf("unexpected bad-origin status %d", badResponse.Code)
	}

	good := httptest.NewRequest(http.MethodPost, "/hifumi/api/v1/auth/logout", nil)
	good.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	goodResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(goodResponse, good)
	cookie := goodResponse.Header().Get("Set-Cookie")
	for _, expected := range []string{"stella_session=", "Path=/hifumi/", "HttpOnly", "Secure", "SameSite=Lax", "Max-Age=0"} {
		if !strings.Contains(cookie, expected) {
			t.Fatalf("cookie %q does not contain %q", cookie, expected)
		}
	}

	compat := httptest.NewRequest(http.MethodPost, "/hifumi/api/v1/auth/logout", nil)
	compat.Header.Set(compatSecretHeader, strings.Repeat("c", 32))
	compat.Header.Set(compatOriginHeader, "https://stellafortuna.luminet.cn")
	compatResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(compatResponse, compat)
	compatCookie := compatResponse.Header().Get("Set-Cookie")
	if !strings.Contains(compatCookie, "Path=/;") || strings.Contains(compatCookie, "Path=/hifumi/") {
		t.Fatalf("compat logout did not clear the legacy root cookie: %q", compatCookie)
	}
	compatEvil := httptest.NewRequest(http.MethodPost, "/hifumi/api/v1/auth/logout", nil)
	compatEvil.Header.Set("Origin", "https://evil.example")
	compatEvil.Header.Set(compatSecretHeader, strings.Repeat("c", 32))
	compatEvil.Header.Set(compatOriginHeader, "https://stellafortuna.luminet.cn")
	compatEvilResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(compatEvilResponse, compatEvil)
	if compatEvilResponse.Code != http.StatusForbidden {
		t.Fatalf("trusted proxy headers bypassed malicious browser origin: %d", compatEvilResponse.Code)
	}
}

func TestCompatibilityLoginAndHandoffTrustBoundary(t *testing.T) {
	oauth := &fakeOAuth{}
	redis := &fakeRealtime{handoff: "session.jwt"}
	server := testServer(t, &fakeTokens{}, oauth, redis)
	login := httptest.NewRequest(http.MethodGet, "/hifumi/api/v1/auth/login/linuxdo?compat=1", nil)
	loginResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(loginResponse, login)
	if loginResponse.Code != http.StatusFound || !oauth.compat {
		t.Fatalf("compat login was not preserved: status=%d compat=%t", loginResponse.Code, oauth.compat)
	}

	forged := httptest.NewRequest(http.MethodPost, "/hifumi/internal/compat/handoff", strings.NewReader(`{"code":"handoff_0123456789abcdef"}`))
	forged.Header.Set(compatSecretHeader, strings.Repeat("x", 32))
	forged.Header.Set(compatOriginHeader, "https://stellafortuna.luminet.cn")
	forgedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(forgedResponse, forged)
	if forgedResponse.Code != http.StatusForbidden {
		t.Fatalf("forged proxy secret returned %d", forgedResponse.Code)
	}

	trusted := httptest.NewRequest(http.MethodPost, "/hifumi/internal/compat/handoff", strings.NewReader(`{"code":"handoff_0123456789abcdef"}`))
	trusted.Header.Set(compatSecretHeader, strings.Repeat("c", 32))
	trusted.Header.Set(compatOriginHeader, "https://stellafortuna.luminet.cn")
	trustedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(trustedResponse, trusted)
	if trustedResponse.Code != http.StatusOK || !strings.Contains(trustedResponse.Body.String(), `"token":"session.jwt"`) {
		t.Fatalf("trusted handoff failed: %d %s", trustedResponse.Code, trustedResponse.Body.String())
	}
}

func TestHealthVersionAndPreflight(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	for path, expected := range map[string]string{
		"/hifumi/healthz": `"status":"ok"`,
		"/hifumi/version": `"commit":"abc"`,
	} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("%s returned %d %s", path, response.Code, response.Body.String())
		}
	}
	preflight := httptest.NewRequest(http.MethodOptions, "/hifumi/api/v1/auth/logout", nil)
	preflight.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, preflight)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Fatalf("unexpected preflight: %d", response.Code)
	}
}

func TestWebSocketHandshakePingAndRPC(t *testing.T) {
	tokens := &fakeTokens{claims: &auth.SessionClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject: "linuxdo-subject", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}}}
	server := httptest.NewServer(testServer(t, tokens, &fakeOAuth{}, &fakeRealtime{}).Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Origin": []string{"https://stellafortuna.luminet.cn"},
			"Cookie": []string{"stella_session=test-token"},
		},
		Subprotocols: []string{SyncWebSocketProtocol},
	}
	connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/hifumi/api/v1/sync/ws", options)
	if err != nil {
		t.Fatalf("dial websocket: response=%v err=%v", response, err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "test complete")
	if connection.Subprotocol() != SyncWebSocketProtocol {
		t.Fatalf("unexpected subprotocol %q", connection.Subprotocol())
	}
	if err := connection.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	messageType, message, err := connection.Read(ctx)
	if err != nil || messageType != websocket.MessageText || string(message) != "pong" {
		t.Fatalf("unexpected pong: type=%v message=%q err=%v", messageType, message, err)
	}
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(&syncv1.SyncRequest{
		DeviceId: "device_alpha", BaselineId: "baseline_0123456789abcdef0123456789abcdef",
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := `{"version":1,"requestId":"request_alpha_0001","type":"exchange","protobuf":"` + base64.StdEncoding.EncodeToString(requestBytes) + `"}`
	if err := connection.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	_, message, err = connection.Read(ctx)
	if err != nil || !strings.Contains(string(message), `"type":"exchange_result"`) {
		t.Fatalf("unexpected RPC response %q err=%v", message, err)
	}
}

func TestWebSocketRequiresUpgradeBeforeAllocatingLease(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	request := httptest.NewRequest(http.MethodGet, "/hifumi/api/v1/sync/ws", nil)
	request.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	request.Header.Set("Sec-WebSocket-Protocol", SyncWebSocketProtocol)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUpgradeRequired || response.Header().Get("Upgrade") != "websocket" {
		t.Fatalf("unexpected upgrade response: %d %v", response.Code, response.Header())
	}
}

func TestHTTPAndWebSocketReturnByteEquivalentProtobuf(t *testing.T) {
	tokens := &fakeTokens{claims: &auth.SessionClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject: "linuxdo-subject", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}}}
	server := httptest.NewServer(testServer(t, tokens, &fakeOAuth{}, &fakeRealtime{}).Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(&syncv1.SyncRequest{
		DeviceId: "device_alpha", BaselineId: "baseline_0123456789abcdef0123456789abcdef",
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := `{"protobuf":"` + base64.StdEncoding.EncodeToString(requestBytes) + `"}`
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/hifumi/api/v1/sync/exchange", strings.NewReader(envelope))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	httpRequest.Header.Set("Cookie", "stella_session=test-token")
	httpResponse, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer httpResponse.Body.Close()
	var httpEnvelope struct {
		Protobuf string `json:"protobuf"`
	}
	if err := json.NewDecoder(httpResponse.Body).Decode(&httpEnvelope); err != nil {
		t.Fatal(err)
	}

	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/hifumi/api/v1/sync/ws", &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Origin": []string{"https://stellafortuna.luminet.cn"},
			"Cookie": []string{"stella_session=test-token"},
		},
		Subprotocols: []string{SyncWebSocketProtocol},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "test complete")
	frame := `{"version":1,"requestId":"request_equal_0001","type":"exchange","protobuf":"` + base64.StdEncoding.EncodeToString(requestBytes) + `"}`
	if err := connection.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	_, message, err := connection.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var websocketEnvelope wireFrame
	if err := json.Unmarshal(message, &websocketEnvelope); err != nil {
		t.Fatal(err)
	}
	if httpEnvelope.Protobuf == "" || websocketEnvelope.Protobuf != httpEnvelope.Protobuf {
		t.Fatalf("transport bytes differ: http=%q websocket=%q", httpEnvelope.Protobuf, websocketEnvelope.Protobuf)
	}
}
