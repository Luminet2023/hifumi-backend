package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"github.com/Luminet2023/hifumi-backend/internal/auth"
	"github.com/Luminet2023/hifumi-backend/internal/config"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"google.golang.org/protobuf/proto"
)

const (
	sessionCookieName  = "stella_session"
	oauthCookieName    = "stella_oauth"
	cookiePath         = "/hifumi/"
	compatSecretHeader = "X-Compat-Proxy-Secret"
	compatOriginHeader = "X-Compat-External-Origin"
)

type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"buildTime"`
}

type TokenManager interface {
	SignSession(auth.Profile) (string, time.Time, error)
	VerifySession(string, bool) (*auth.SessionClaims, error)
	VerifyOAuthState(string, string) (*auth.OAuthClaims, error)
}

type OAuthProvider interface {
	Begin(bool) (auth.Login, error)
	Complete(context.Context, string, *auth.OAuthClaims) (auth.Profile, error)
}

type ProfileStore interface {
	UpsertProfile(context.Context, string, auth.Profile, uint64) (auth.Profile, error)
}

type SyncService interface {
	Exchange(context.Context, syncservice.CommandMetadata, *syncv1.SyncRequest) (*syncservice.ExchangeResult, error)
	ResolveBaseline(context.Context, syncservice.CommandMetadata, *syncv1.ResolveBaselineRequest) (*syncservice.ResolveResult, error)
}

type RealtimeClient interface {
	Ping(context.Context) error
	CheckFixedWindow(context.Context, string, string, int, time.Duration) (time.Duration, error)
	AcquireConnection(context.Context, string, string, int, time.Duration) error
	RefreshConnection(context.Context, string, string, time.Duration) (bool, error)
	ReleaseConnection(context.Context, string, string) error
	PutHandoff(context.Context, string, time.Duration) (string, error)
	ConsumeHandoff(context.Context, string) (string, error)
}

type Dependencies struct {
	Config      config.Config
	Tokens      TokenManager
	OAuth       OAuthProvider
	Profiles    ProfileStore
	Sync        SyncService
	Realtime    RealtimeClient
	Hub         *realtime.Hub
	DB          *sql.DB
	CheckSchema func(context.Context) error
	Logger      *slog.Logger
	Build       BuildInfo
	Now         func() time.Time
}

type Server struct {
	cfg               config.Config
	tokens            TokenManager
	oauth             OAuthProvider
	profiles          ProfileStore
	sync              SyncService
	realtime          RealtimeClient
	hub               *realtime.Hub
	db                *sql.DB
	checkSchema       func(context.Context) error
	logger            *slog.Logger
	build             BuildInfo
	now               func() time.Time
	mux               *http.ServeMux
	trustedProxyCIDRs []*net.IPNet
}

func NewServer(deps Dependencies) (*Server, error) {
	if deps.Tokens == nil || deps.OAuth == nil || deps.Profiles == nil || deps.Sync == nil || deps.Realtime == nil || deps.Hub == nil {
		return nil, fmt.Errorf("httpapi dependencies are incomplete")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	server := &Server{
		cfg: deps.Config, tokens: deps.Tokens, oauth: deps.OAuth, profiles: deps.Profiles,
		sync: deps.Sync, realtime: deps.Realtime, hub: deps.Hub, db: deps.DB,
		checkSchema: deps.CheckSchema, logger: deps.Logger, build: deps.Build, now: deps.Now,
		mux: http.NewServeMux(),
	}
	for _, raw := range deps.Config.TrustedProxyCIDRs {
		_, network, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", raw, err)
		}
		server.trustedProxyCIDRs = append(server.trustedProxyCIDRs, network)
	}
	server.routes()
	return server, nil
}

func (s *Server) Handler() http.Handler {
	return s.requestLog(s.securityHeaders(s.cors(s.mux)))
}

func (s *Server) routes() {
	path := s.cfg.PublicPath
	s.mux.HandleFunc("GET "+path("healthz"), s.health)
	s.mux.HandleFunc("GET "+path("readyz"), s.ready)
	s.mux.HandleFunc("GET "+path("version"), s.version)
	s.mux.HandleFunc("GET "+path("api/v1/auth/login/linuxdo"), s.login)
	s.mux.HandleFunc("GET "+path("api/v1/auth/callback"), s.callback)
	s.mux.HandleFunc("GET "+path("api/v1/auth/session"), s.session)
	s.mux.HandleFunc("POST "+path("api/v1/auth/logout"), s.logout)
	s.mux.HandleFunc("POST "+path("api/v1/sync/exchange"), s.exchange)
	s.mux.HandleFunc("POST "+path("api/v1/sync/resolve"), s.resolve)
	s.mux.HandleFunc("GET "+path("api/v1/sync/ws"), s.webSocket)
	s.mux.HandleFunc("POST "+path("internal/compat/handoff"), s.handoff)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.db == nil || s.db.PingContext(ctx) != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "mysql_unavailable")
		return
	}
	if err := s.realtime.Ping(ctx); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "redis_unavailable")
		return
	}
	if s.checkSchema == nil || s.checkSchema(ctx) != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "schema_not_ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.build)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	// compat 仅决定 OAuth 完成后是否生成一次性 handoff；它不放宽任何
	// Session 验证或内部接口权限，因此旧 Worker 可以用浏览器重定向进入。
	login, err := s.oauth.Begin(r.URL.Query().Get("compat") == "1")
	if err != nil {
		s.internalError(w, r, "oauth_begin", err)
		return
	}
	setCookie(w, oauthCookieName, login.StateToken, int(auth.OAuthLifetime/time.Second))
	w.Header().Set("Location", login.AuthorizationURL)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusFound)
}

func (s *Server) callback(w http.ResponseWriter, r *http.Request) {
	fail := func(code string, err error) {
		if err != nil {
			s.logger.WarnContext(r.Context(), "oauth callback failed", "request_id", requestID(r.Context()), "class", code, "error", err)
		}
		target := *s.cfg.FrontendReturnURL
		target.Path = "/day/2026-07-13"
		query := target.Query()
		query.Set("auth_error", "oauth_failed")
		target.RawQuery = query.Encode()
		http.Redirect(w, r, target.String(), http.StatusFound)
	}
	if r.URL.Query().Get("error") != "" {
		fail("oauth_denied", nil)
		return
	}
	code, state := r.URL.Query().Get("code"), r.URL.Query().Get("state")
	if code == "" || state == "" {
		fail("missing_oauth_parameters", nil)
		return
	}
	cookie, err := r.Cookie(oauthCookieName)
	if err != nil {
		fail("missing_oauth_cookie", err)
		return
	}
	claims, err := s.tokens.VerifyOAuthState(cookie.Value, state)
	if err != nil {
		fail("invalid_oauth_state", err)
		return
	}
	profile, err := s.oauth.Complete(r.Context(), code, claims)
	if err != nil {
		fail("oauth_upstream", err)
		return
	}
	profile, err = s.profiles.UpsertProfile(r.Context(), auth.OwnerKey(profile.Subject), profile, uint64(s.now().UnixMilli()))
	if err != nil {
		fail("profile_store", err)
		return
	}
	token, expires, err := s.tokens.SignSession(profile)
	if err != nil {
		fail("session_sign", err)
		return
	}
	_ = expires
	setCookie(w, sessionCookieName, token, int(auth.SessionLifetime/time.Second))
	clearCookie(w, oauthCookieName)
	w.Header().Set("Cache-Control", "no-store")
	if claims.Compat {
		code, err := s.realtime.PutHandoff(r.Context(), token, time.Minute)
		if err != nil {
			fail("handoff_store", err)
			return
		}
		target, _ := url.Parse(s.cfg.FrontendOrigin + "/api/v1/auth/handoff")
		query := target.Query()
		query.Set("code", code)
		target.RawQuery = query.Encode()
		w.Header().Set("Location", target.String())
	} else {
		w.Header().Set("Location", s.cfg.FrontendReturnURL.String())
	}
	w.WriteHeader(http.StatusFound)
}

func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	claims, _, err := s.authenticate(r)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false, "user": nil})
		return
	}
	profile := auth.Profile{
		Subject: claims.Subject, Username: claims.Username, DisplayName: claims.Name,
		AvatarURL: claims.AvatarURL, Email: claims.Email,
	}
	lastLogin := uint64(0)
	if claims.IssuedAt != nil && claims.IssuedAt.Unix() > 0 {
		lastLogin = uint64(claims.IssuedAt.UnixMilli())
	}
	profile, err = s.profiles.UpsertProfile(r.Context(), auth.OwnerKey(profile.Subject), profile, lastLogin)
	if err != nil {
		s.internalError(w, r, "profile_store", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"user": map[string]string{
			"id": profile.Subject, "username": profile.Username, "name": profile.DisplayName,
			"avatarUrl": profile.AvatarURL, "email": profile.Email,
		},
	})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if !s.validStateOrigin(r) {
		writeAPIError(w, http.StatusForbidden, "invalid_origin")
		return
	}
	if s.isTrustedCompat(r) {
		clearCookieAtPath(w, sessionCookieName, "/")
	} else {
		clearCookie(w, sessionCookieName)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"authenticated": false})
}

func (s *Server) exchange(w http.ResponseWriter, r *http.Request) {
	s.handleSyncHTTP(w, r, "exchange")
}

func (s *Server) resolve(w http.ResponseWriter, r *http.Request) {
	s.handleSyncHTTP(w, r, "resolve")
}

func (s *Server) handleSyncHTTP(w http.ResponseWriter, r *http.Request, operation string) {
	if !s.validStateOrigin(r) {
		writeAPIError(w, http.StatusForbidden, "invalid_origin")
		return
	}
	_, ownerKey, err := s.authenticate(r)
	if err != nil {
		writeAPIError(w, http.StatusUnauthorized, "authentication_required")
		return
	}
	body, err := readProtobufEnvelope(r)
	if err != nil {
		writeEnvelopeError(w, err)
		return
	}
	started := time.Now()
	var exchangeRequest *syncv1.SyncRequest
	var resolveRequest *syncv1.ResolveBaselineRequest
	if operation == "exchange" {
		request := &syncv1.SyncRequest{}
		if err := proto.Unmarshal(body, request); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_sync_envelope")
			return
		}
		if err := syncservice.ValidateExchange(ownerKey, request); err != nil {
			s.writeSyncError(w, r, err)
			return
		}
		exchangeRequest = request
	} else {
		request := &syncv1.ResolveBaselineRequest{}
		if err := proto.Unmarshal(body, request); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_sync_envelope")
			return
		}
		if err := syncservice.ValidateResolve(ownerKey, request); err != nil {
			s.writeSyncError(w, r, err)
			return
		}
		resolveRequest = request
	}
	limit, window := 8, 10*time.Second
	if operation == "resolve" {
		limit, window = 3, time.Minute
	}
	retry, err := s.realtime.CheckFixedWindow(r.Context(), operation, ownerKey, limit, window)
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "redis_unavailable")
		return
	}
	if retry > 0 {
		w.Header().Set("Retry-After", realtime.RetryAfterSeconds(retry))
		writeAPIError(w, http.StatusTooManyRequests, "sync_rate_limited")
		return
	}
	var response proto.Message
	var changed bool
	if exchangeRequest != nil {
		result, err := s.sync.Exchange(r.Context(), syncservice.CommandMetadata{OwnerKey: ownerKey}, exchangeRequest)
		if err != nil {
			s.writeSyncError(w, r, err)
			return
		}
		response, changed = result.Response, result.StateChanged
	} else {
		result, err := s.sync.ResolveBaseline(r.Context(), syncservice.CommandMetadata{OwnerKey: ownerKey}, resolveRequest)
		if err != nil {
			s.writeSyncError(w, r, err)
			return
		}
		response, changed = result.Response, result.StateChanged
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		s.internalError(w, r, "protobuf_marshal", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]string{"protobuf": base64.StdEncoding.EncodeToString(encoded)})
	s.logger.InfoContext(r.Context(), "sync request completed", "request_id", requestID(r.Context()), "operation", operation, "state_changed", changed, "duration_ms", time.Since(started).Milliseconds())
}

func (s *Server) handoff(w http.ResponseWriter, r *http.Request) {
	if !s.isTrustedCompat(r) {
		writeAPIError(w, http.StatusForbidden, "compat_proxy_required")
		return
	}
	var payload struct {
		Code string `json:"code"`
	}
	if err := decodeLimitedJSON(r.Body, &payload, 1024); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_handoff_request")
		return
	}
	token, err := s.realtime.ConsumeHandoff(r.Context(), payload.Code)
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "redis_unavailable")
		return
	}
	if token == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_handoff_code")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "maxAge": int(auth.SessionLifetime / time.Second)})
}

func (s *Server) authenticate(r *http.Request) (*auth.SessionClaims, string, error) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return nil, "", auth.ErrInvalidToken
	}
	claims, err := s.tokens.VerifySession(cookie.Value, s.isTrustedCompat(r))
	if err != nil {
		return nil, "", err
	}
	return claims, auth.OwnerKey(claims.Subject), nil
}

func (s *Server) isTrustedCompat(r *http.Request) bool {
	expected, provided := []byte(s.cfg.CompatProxySecret), []byte(r.Header.Get(compatSecretHeader))
	return len(expected) > 0 && len(expected) == len(provided) && subtle.ConstantTimeCompare(expected, provided) == 1 &&
		r.Header.Get(compatOriginHeader) == s.cfg.FrontendOrigin
}

func (s *Server) validStateOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	return origin == s.cfg.FrontendOrigin || (origin == "" && s.isTrustedCompat(r))
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		isAPI := strings.HasPrefix(r.URL.Path, s.cfg.PublicPath("api/")) || strings.HasPrefix(r.URL.Path, s.cfg.PublicPath("internal/"))
		if isAPI && origin != "" && origin != s.cfg.FrontendOrigin {
			writeAPIError(w, http.StatusForbidden, "invalid_origin")
			return
		}
		if origin == s.cfg.FrontendOrigin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions && strings.HasPrefix(r.URL.Path, s.cfg.PublicPath("api/")) {
			if origin != s.cfg.FrontendOrigin {
				writeAPIError(w, http.StatusForbidden, "invalid_origin")
				return
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

type contextKey string

const requestIDKey contextKey = "request_id"

func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newRequestID()
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		w.Header().Set("X-Request-Id", id)
		started := time.Now()
		next.ServeHTTP(w, r.WithContext(ctx))
		s.logger.InfoContext(ctx, "http request", "request_id", id, "method", r.Method, "path", r.URL.Path, "client_ip", s.clientIP(r), "duration_ms", time.Since(started).Milliseconds())
	})
}

func (s *Server) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	remoteIP := net.ParseIP(host)
	trusted := false
	for _, network := range s.trustedProxyCIDRs {
		if remoteIP != nil && network.Contains(remoteIP) {
			trusted = true
			break
		}
	}
	if trusted {
		for _, candidate := range strings.Split(r.Header.Get("X-Forwarded-For"), ",") {
			candidate = strings.TrimSpace(candidate)
			if net.ParseIP(candidate) != nil {
				return candidate
			}
		}
	}
	return host
}

func requestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

func newRequestID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(value)
}

func (s *Server) writeSyncError(w http.ResponseWriter, r *http.Request, err error) {
	var operationError *syncservice.OperationError
	if errors.As(err, &operationError) {
		writeAPIError(w, http.StatusBadRequest, strings.ToLower(string(operationError.Code)))
		return
	}
	s.internalError(w, r, "sync_store", err)
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, class string, err error) {
	s.logger.ErrorContext(r.Context(), "request failed", "request_id", requestID(r.Context()), "class", class, "error", err)
	writeAPIError(w, http.StatusInternalServerError, "internal_error")
}

var (
	errEnvelopeMediaType = errors.New("json base64 protobuf required")
	errEnvelopeTooLarge  = errors.New("sync payload too large")
	errEnvelopeInvalid   = errors.New("invalid sync envelope")
)

func readProtobufEnvelope(r *http.Request) ([]byte, error) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, errEnvelopeMediaType
	}
	if r.ContentLength > MaxSyncEnvelopeBytes {
		return nil, errEnvelopeTooLarge
	}
	var envelope struct {
		Protobuf string `json:"protobuf"`
	}
	if err := decodeLimitedJSON(r.Body, &envelope, MaxSyncEnvelopeBytes); err != nil || envelope.Protobuf == "" {
		if errors.Is(err, errEnvelopeTooLarge) {
			return nil, err
		}
		return nil, errEnvelopeInvalid
	}
	if len(envelope.Protobuf) > MaxSyncEnvelopeBytes {
		return nil, errEnvelopeTooLarge
	}
	body, err := base64.StdEncoding.Strict().DecodeString(envelope.Protobuf)
	if err != nil {
		return nil, errEnvelopeInvalid
	}
	if len(body) > MaxProtobufBytes {
		return nil, errEnvelopeTooLarge
	}
	return body, nil
}

func decodeLimitedJSON(body io.Reader, target any, limit int64) error {
	limited := io.LimitReader(body, limit+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return errEnvelopeTooLarge
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errEnvelopeInvalid
	}
	return nil
}

func writeEnvelopeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errEnvelopeMediaType):
		writeAPIError(w, http.StatusUnsupportedMediaType, "json_base64_protobuf_required")
	case errors.Is(err, errEnvelopeTooLarge):
		writeAPIError(w, http.StatusRequestEntityTooLarge, "sync_payload_too_large")
	default:
		writeAPIError(w, http.StatusBadRequest, "invalid_sync_envelope")
	}
}

func setCookie(w http.ResponseWriter, name, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: value, Path: cookiePath, MaxAge: maxAge, HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode})
}

func clearCookie(w http.ResponseWriter, name string) {
	setCookie(w, name, "", -1)
}

func clearCookieAtPath(w http.ResponseWriter, name, path string) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: "", Path: path, MaxAge: -1, HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode})
}

func writeAPIError(w http.ResponseWriter, status int, code string) {
	writeJSONStatus(w, status, map[string]string{"error": code})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	writeJSONStatus(w, status, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
