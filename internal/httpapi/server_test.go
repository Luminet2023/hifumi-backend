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
	claims      *auth.SessionClaims
	oauthClaims *auth.OAuthClaims
	signErr     error
}

func (f *fakeTokens) SignSession(auth.Profile) (string, time.Time, error) {
	if f.signErr != nil {
		return "", time.Time{}, f.signErr
	}
	return "signed.token", time.Now().Add(auth.SessionLifetime), nil
}
func (f *fakeTokens) VerifySession(string) (*auth.SessionClaims, error) {
	if f.claims == nil {
		return nil, auth.ErrInvalidToken
	}
	return f.claims, nil
}
func (f *fakeTokens) VerifyOAuthState(string, string) (*auth.OAuthClaims, error) {
	if f.oauthClaims == nil {
		return nil, auth.ErrInvalidToken
	}
	return f.oauthClaims, nil
}

type fakeOAuth struct {
	returnTo string
	profile  *auth.Profile
	panic    bool
}

func (f *fakeOAuth) Begin(returnTo string) (auth.Login, error) {
	if f.panic {
		panic("sensitive panic detail")
	}
	f.returnTo = returnTo
	return auth.Login{AuthorizationURL: "https://connect.linux.do/oauth2/authorize", StateToken: "state.jwt"}, nil
}
func (f *fakeOAuth) Complete(context.Context, string, *auth.OAuthClaims) (auth.Profile, error) {
	if f.profile != nil {
		return *f.profile, nil
	}
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
func (*fakeSync) Diff(_ context.Context, _ syncservice.CommandMetadata, request *syncv1.DiffRequest) (*syncservice.DiffResult, error) {
	return &syncservice.DiffResult{Response: &syncv1.DiffResponse{
		BaselineId: request.GetBaselineId(), ServerCursor: 3, ServerVersion: 2,
	}}, nil
}
func (*fakeSync) ReadFeedPage(_ context.Context, _ string, baselineID string, cursor uint64, _ int) (*syncservice.FeedPage, error) {
	return &syncservice.FeedPage{
		NextCursor: cursor, HeadCursor: cursor, BaselineID: baselineID, ServerProgressDay: "2026-07-13",
	}, nil
}
func (*fakeSync) ResolveBaseline(context.Context, syncservice.CommandMetadata, *syncv1.ResolveBaselineRequest) (*syncservice.ResolveResult, error) {
	return &syncservice.ResolveResult{Response: &syncv1.ResolveBaselineResponse{}}, nil
}

type fakeRealtime struct{}

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

func testServer(t *testing.T, tokens *fakeTokens, oauth *fakeOAuth, redis *fakeRealtime) *Server {
	return testServerWithLogger(t, tokens, oauth, redis, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func testServerWithLogger(t *testing.T, tokens *fakeTokens, oauth *fakeOAuth, redis *fakeRealtime, logger *slog.Logger) *Server {
	t.Helper()
	publicBase, _ := url.Parse("https://api.luminet.cn/hifumi/")
	frontendReturn, _ := url.Parse("https://stellafortuna.luminet.cn/")
	server, err := NewServer(Dependencies{
		Config: config.Config{
			PublicBaseURL:     publicBase,
			FrontendOrigins:   []string{"https://stellafortuna.hifumi.luminet.cn", "https://stellafortuna.luminet.cn"},
			FrontendReturnURL: frontendReturn,
		},
		Tokens: tokens, OAuth: oauth, Profiles: &fakeProfiles{}, Sync: &fakeSync{},
		Realtime: redis, Hub: realtime.NewHub(), WakeHub: realtime.NewWakeHub(),
		Logger: logger,
		Build:  BuildInfo{Version: "test", Commit: "abc", BuildTime: "now"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestSessionUnauthenticatedAndCORS(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	request := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/session", nil)
	request.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	request.Header.Set("Referer", "https://stellafortuna.luminet.cn/")
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
	secondary := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/session", nil)
	secondary.Header.Set("Origin", "https://stellafortuna.hifumi.luminet.cn")
	secondary.Header.Set("Referer", "https://stellafortuna.hifumi.luminet.cn/day/2026-07-13")
	secondaryResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(secondaryResponse, secondary)
	if secondaryResponse.Code != http.StatusOK || secondaryResponse.Header().Get("Access-Control-Allow-Origin") != "https://stellafortuna.hifumi.luminet.cn" {
		t.Fatalf("secondary frontend origin was rejected: %d %v", secondaryResponse.Code, secondaryResponse.Header())
	}
	malicious := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/session", nil)
	malicious.Header.Set("Origin", "https://evil.example")
	maliciousResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(maliciousResponse, malicious)
	if maliciousResponse.Code != http.StatusForbidden {
		t.Fatalf("malicious CORS origin returned %d", maliciousResponse.Code)
	}
	badReferer := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/session", nil)
	badReferer.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	badReferer.Header.Set("Referer", "https://evil.example/embedded")
	badRefererResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(badRefererResponse, badReferer)
	if badRefererResponse.Code != http.StatusForbidden || !strings.Contains(badRefererResponse.Body.String(), "invalid_referer") {
		t.Fatalf("malicious Referer returned %d %s", badRefererResponse.Code, badRefererResponse.Body.String())
	}
	missingReferer := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/session", nil)
	missingReferer.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	missingRefererResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingRefererResponse, missingReferer)
	if missingRefererResponse.Code != http.StatusForbidden || !strings.Contains(missingRefererResponse.Body.String(), "invalid_referer") {
		t.Fatalf("missing Referer returned %d %s", missingRefererResponse.Code, missingRefererResponse.Body.String())
	}
	mismatchedReferer := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/session", nil)
	mismatchedReferer.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	mismatchedReferer.Header.Set("Referer", "https://stellafortuna.hifumi.luminet.cn/")
	mismatchedRefererResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(mismatchedRefererResponse, mismatchedReferer)
	if mismatchedRefererResponse.Code != http.StatusForbidden || !strings.Contains(mismatchedRefererResponse.Body.String(), "invalid_referer") {
		t.Fatalf("cross-allowlist Referer returned %d %s", mismatchedRefererResponse.Code, mismatchedRefererResponse.Body.String())
	}
}

func TestStateChangingOriginAndCookiePath(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	bad := httptest.NewRequest(http.MethodPost, "/hifumi/v1/auth/logout", nil)
	bad.Header.Set("Origin", "https://evil.example")
	badResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(badResponse, bad)
	if badResponse.Code != http.StatusForbidden {
		t.Fatalf("unexpected bad-origin status %d", badResponse.Code)
	}

	good := httptest.NewRequest(http.MethodPost, "/hifumi/v1/auth/logout", nil)
	good.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	good.Header.Set("Referer", "https://stellafortuna.luminet.cn/settings")
	goodResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(goodResponse, good)
	cookie := goodResponse.Header().Get("Set-Cookie")
	for _, expected := range []string{"stella_session=", "Path=/hifumi/", "HttpOnly", "Secure", "SameSite=Lax", "Max-Age=0"} {
		if !strings.Contains(cookie, expected) {
			t.Fatalf("cookie %q does not contain %q", cookie, expected)
		}
	}

	noOrigin := httptest.NewRequest(http.MethodPost, "/hifumi/v1/auth/logout", nil)
	noOriginResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(noOriginResponse, noOrigin)
	if noOriginResponse.Code != http.StatusForbidden {
		t.Fatalf("state-changing request without Origin returned %d", noOriginResponse.Code)
	}
	missingReferer := httptest.NewRequest(http.MethodPost, "/hifumi/v1/auth/logout", nil)
	missingReferer.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	missingRefererResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingRefererResponse, missingReferer)
	if missingRefererResponse.Code != http.StatusForbidden || !strings.Contains(missingRefererResponse.Body.String(), "invalid_referer") {
		t.Fatalf("state-changing request without Referer returned %d %s", missingRefererResponse.Code, missingRefererResponse.Body.String())
	}
	badReferer := httptest.NewRequest(http.MethodPost, "/hifumi/v1/auth/logout", nil)
	badReferer.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	badReferer.Header.Set("Referer", "https://evil.example/")
	badRefererResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(badRefererResponse, badReferer)
	if badRefererResponse.Code != http.StatusForbidden || !strings.Contains(badRefererResponse.Body.String(), "invalid_referer") {
		t.Fatalf("unexpected bad-referer response: %d %s", badRefererResponse.Code, badRefererResponse.Body.String())
	}
}

func TestInternalCompatibilityHandoffIsNotRouted(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	request := httptest.NewRequest(http.MethodPost, "/hifumi/internal/compat/handoff", strings.NewReader(`{"code":"retired"}`))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("retired compatibility handoff returned %d", response.Code)
	}
}

func TestOAuthReturnToFollowsAllowedRefererAndRejectsOpenRedirect(t *testing.T) {
	oauth := &fakeOAuth{}
	server := testServer(t, &fakeTokens{}, oauth, &fakeRealtime{})

	login := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/login/linuxdo", nil)
	login.Header.Set("Referer", "https://stellafortuna.hifumi.luminet.cn/day/2026-07-13")
	loginResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(loginResponse, login)
	if loginResponse.Code != http.StatusFound || oauth.returnTo != "https://stellafortuna.hifumi.luminet.cn/" {
		t.Fatalf("allowed Referer selected returnTo=%q status=%d", oauth.returnTo, loginResponse.Code)
	}

	invalidLogin := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/login/linuxdo", nil)
	invalidLogin.Header.Set("Referer", "https://evil.example/phishing")
	invalidLoginResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(invalidLoginResponse, invalidLogin)
	if invalidLoginResponse.Code != http.StatusFound || oauth.returnTo != "https://stellafortuna.luminet.cn/" {
		t.Fatalf("invalid Referer did not fall back: returnTo=%q status=%d", oauth.returnTo, invalidLoginResponse.Code)
	}

	callbackLocation := func(returnTo string) string {
		t.Helper()
		tokens := &fakeTokens{oauthClaims: &auth.OAuthClaims{ReturnTo: returnTo}}
		provider := &fakeOAuth{profile: &auth.Profile{Subject: "42", Username: "hifumi"}}
		callbackServer := testServer(t, tokens, provider, &fakeRealtime{})
		callback := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/callback?code=test&state=test", nil)
		callback.AddCookie(&http.Cookie{Name: oauthCookieName, Value: "signed-state"})
		response := httptest.NewRecorder()
		callbackServer.Handler().ServeHTTP(response, callback)
		if response.Code != http.StatusFound {
			t.Fatalf("callback returned %d %s", response.Code, response.Body.String())
		}
		return response.Header().Get("Location")
	}
	if got := callbackLocation("https://stellafortuna.hifumi.luminet.cn/"); got != "https://stellafortuna.hifumi.luminet.cn/" {
		t.Fatalf("allowed signed returnTo became %q", got)
	}
	if got := callbackLocation("https://evil.example/steal"); got != "https://stellafortuna.luminet.cn/" {
		t.Fatalf("untrusted signed returnTo became %q", got)
	}

	callbackFailureLocation := func(tokens *fakeTokens, provider *fakeOAuth) string {
		t.Helper()
		callbackServer := testServer(t, tokens, provider, &fakeRealtime{})
		callback := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/callback?code=test&state=test", nil)
		callback.AddCookie(&http.Cookie{Name: oauthCookieName, Value: "signed-state"})
		response := httptest.NewRecorder()
		callbackServer.Handler().ServeHTTP(response, callback)
		if response.Code != http.StatusFound {
			t.Fatalf("failed callback returned %d %s", response.Code, response.Body.String())
		}
		return response.Header().Get("Location")
	}
	secondaryClaims := &auth.OAuthClaims{ReturnTo: "https://stellafortuna.hifumi.luminet.cn/"}
	for name, location := range map[string]string{
		"upstream": callbackFailureLocation(&fakeTokens{oauthClaims: secondaryClaims}, &fakeOAuth{}),
		"sign": callbackFailureLocation(
			&fakeTokens{oauthClaims: secondaryClaims, signErr: errors.New("sign failed")},
			&fakeOAuth{profile: &auth.Profile{Subject: "42"}},
		),
	} {
		if !strings.HasPrefix(location, "https://stellafortuna.hifumi.luminet.cn/day/2026-07-13?") || !strings.Contains(location, "auth_error=oauth_failed") {
			t.Fatalf("%s failure did not preserve initiating origin: %q", name, location)
		}
	}

	unverified := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	unverifiedRequest := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/callback?code=test&state=test", nil)
	unverifiedRequest.AddCookie(&http.Cookie{Name: oauthCookieName, Value: "invalid-state"})
	unverifiedResponse := httptest.NewRecorder()
	unverified.Handler().ServeHTTP(unverifiedResponse, unverifiedRequest)
	if got := unverifiedResponse.Header().Get("Location"); !strings.HasPrefix(got, "https://stellafortuna.luminet.cn/day/2026-07-13?") {
		t.Fatalf("unverified state did not fall back to primary origin: %q", got)
	}
}

func TestGinRecoveryHidesPanicAndLogsStatus(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	server := testServerWithLogger(t, &fakeTokens{}, &fakeOAuth{panic: true}, &fakeRealtime{}, logger)
	request := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/login/linuxdo", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), `"error":"internal_error"`) {
		t.Fatalf("unexpected panic response: %d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "sensitive panic detail") {
		t.Fatalf("panic detail leaked to client: %s", response.Body.String())
	}
	if !strings.Contains(logs.String(), "class=panic") || !strings.Contains(logs.String(), "status=500") {
		t.Fatalf("panic/status missing from structured logs: %s", logs.String())
	}
}

func TestHealthVersionAndPreflight(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	for path, expected := range map[string]string{
		"/hifumi/healthz": `"status":"ok"`,
		"/hifumi/version": `"commit":"abc"`,
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Referer", "https://monitoring.example/")
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("%s returned %d %s", path, response.Code, response.Body.String())
		}
	}
	preflight := httptest.NewRequest(http.MethodOptions, "/hifumi/v1/auth/logout", nil)
	preflight.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, preflight)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Fatalf("unexpected preflight: %d", response.Code)
	}
	callback := httptest.NewRequest(http.MethodGet, "/hifumi/v1/auth/callback?code=test&state=test", nil)
	callbackResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(callbackResponse, callback)
	if callbackResponse.Code != http.StatusFound {
		t.Fatalf("OAuth callback without Referer returned %d", callbackResponse.Code)
	}
	wrongMethod := httptest.NewRequest(http.MethodPost, "/hifumi/healthz", nil)
	wrongMethodResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(wrongMethodResponse, wrongMethod)
	if wrongMethodResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method returned %d", wrongMethodResponse.Code)
	}
}

func TestLegacyPublicAPIPrefixIsNotRouted(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	request := httptest.NewRequest(http.MethodGet, "/hifumi/api/v1/auth/session", nil)
	request.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("legacy public API prefix returned %d", response.Code)
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
	connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/hifumi/v1/sync/ws", options)
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
	request := httptest.NewRequest(http.MethodGet, "/hifumi/v1/sync/ws", nil)
	request.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	request.Header.Set("Sec-WebSocket-Protocol", SyncWebSocketProtocol)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUpgradeRequired || response.Header().Get("Upgrade") != "websocket" {
		t.Fatalf("unexpected upgrade response: %d %v", response.Code, response.Header())
	}
}

func TestWebSocketRejectsForeignReferer(t *testing.T) {
	server := testServer(t, &fakeTokens{}, &fakeOAuth{}, &fakeRealtime{})
	for _, referer := range []string{"https://evil.example/", "https://stellafortuna.hifumi.luminet.cn/"} {
		request := httptest.NewRequest(http.MethodGet, "/hifumi/v1/sync/ws", nil)
		request.Header.Set("Connection", "Upgrade")
		request.Header.Set("Upgrade", "websocket")
		request.Header.Set("Origin", "https://stellafortuna.luminet.cn")
		request.Header.Set("Referer", referer)
		request.Header.Set("Sec-WebSocket-Protocol", SyncWebSocketProtocol)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), "invalid_referer") {
			t.Fatalf("unexpected Referer %q response: %d %s", referer, response.Code, response.Body.String())
		}
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
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/hifumi/v1/sync/exchange", strings.NewReader(envelope))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Origin", "https://stellafortuna.luminet.cn")
	httpRequest.Header.Set("Referer", "https://stellafortuna.luminet.cn/")
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

	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/hifumi/v1/sync/ws", &websocket.DialOptions{
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
