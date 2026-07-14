package httpapi

import (
	"context"
	"crypto/rand"
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
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"github.com/Luminet2023/hifumi-backend/internal/auth"
	"github.com/Luminet2023/hifumi-backend/internal/config"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"
)

const (
	sessionCookieName = "stella_session"
	oauthCookieName   = "stella_oauth"
	cookiePath        = "/hifumi/"
)

type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"buildTime"`
}

type TokenManager interface {
	SignSession(auth.Profile) (string, time.Time, error)
	VerifySession(string) (*auth.SessionClaims, error)
	VerifyOAuthState(string, string) (*auth.OAuthClaims, error)
}

type OAuthProvider interface {
	Begin(string) (auth.Login, error)
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
	router            *gin.Engine
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
	}
	for _, raw := range deps.Config.TrustedProxyCIDRs {
		_, network, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", raw, err)
		}
		server.trustedProxyCIDRs = append(server.trustedProxyCIDRs, network)
	}
	// 本服务自行输出结构化请求日志，关闭 Gin 的路由调试输出。
	gin.SetMode(gin.ReleaseMode)
	server.router = gin.New()
	server.router.RedirectTrailingSlash = false
	server.router.RedirectFixedPath = false
	server.router.HandleMethodNotAllowed = true
	if err := server.router.SetTrustedProxies(nil); err != nil {
		return nil, fmt.Errorf("disable Gin trusted proxies: %w", err)
	}
	server.router.Use(server.requestLog(), server.recovery(), server.securityHeaders(), server.cors())
	server.routes()
	return server, nil
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) routes() {
	path := s.cfg.PublicPath
	s.router.GET(path("healthz"), ginHandler(s.health))
	s.router.GET(path("readyz"), ginHandler(s.ready))
	s.router.GET(path("version"), ginHandler(s.version))
	s.router.GET(path("v1/auth/login/linuxdo"), ginHandler(s.login))
	s.router.GET(path("v1/auth/callback"), ginHandler(s.callback))
	s.router.GET(path("v1/auth/session"), ginHandler(s.session))
	s.router.POST(path("v1/auth/logout"), ginHandler(s.logout))
	s.router.POST(path("v1/sync/exchange"), ginHandler(s.exchange))
	s.router.POST(path("v1/sync/resolve"), ginHandler(s.resolve))
	s.router.GET(path("v1/sync/ws"), ginHandler(s.webSocket))
	// CORS middleware 会在进入该处前完成 allowlist 校验并返回 204。
	s.router.OPTIONS(path("v1/*path"), func(c *gin.Context) { c.Status(http.StatusNoContent) })
}

func ginHandler(handler http.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		handler(c.Writer, c.Request)
	}
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
	returnTo := s.cfg.FrontendReturnURL.String()
	if origin, ok := s.allowedRefererOrigin(r); ok {
		returnTo = origin + "/"
	}
	login, err := s.oauth.Begin(returnTo)
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
	failureReturnTo := s.cfg.FrontendReturnURL.String()
	fail := func(code string, err error) {
		if err != nil {
			s.logger.WarnContext(r.Context(), "oauth callback failed", "request_id", requestID(r.Context()), "class", code, "error", err)
		}
		target, _ := url.Parse(s.validatedFrontendReturnURL(failureReturnTo))
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
	failureReturnTo = claims.ReturnTo
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
	w.Header().Set("Location", s.validatedFrontendReturnURL(claims.ReturnTo))
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
	clearCookie(w, sessionCookieName)
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

func (s *Server) authenticate(r *http.Request) (*auth.SessionClaims, string, error) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return nil, "", auth.ErrInvalidToken
	}
	claims, err := s.tokens.VerifySession(cookie.Value)
	if err != nil {
		return nil, "", err
	}
	return claims, auth.OwnerKey(claims.Subject), nil
}

func (s *Server) validStateOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	return s.isAllowedFrontendOrigin(origin) && s.validRefererForOrigin(r, true)
}

func (s *Server) isAllowedFrontendOrigin(origin string) bool {
	for _, allowed := range s.cfg.FrontendOrigins {
		if origin == allowed {
			return true
		}
	}
	return false
}

func (s *Server) validRefererForOrigin(r *http.Request, required bool) bool {
	raw := strings.TrimSpace(r.Header.Get("Referer"))
	if raw == "" {
		return !required
	}
	refererOrigin, ok := s.allowedRefererOrigin(r)
	return ok && refererOrigin == r.Header.Get("Origin")
}

func (s *Server) allowedRefererOrigin(r *http.Request) (string, bool) {
	raw := strings.TrimSpace(r.Header.Get("Referer"))
	if raw == "" {
		return "", false
	}
	referer, err := url.Parse(raw)
	if err != nil || referer.Scheme != "https" || referer.Host == "" || referer.User != nil {
		return "", false
	}
	origin := referer.Scheme + "://" + referer.Host
	return origin, s.isAllowedFrontendOrigin(origin)
}

func (s *Server) validatedFrontendReturnURL(raw string) string {
	target, err := url.Parse(strings.TrimSpace(raw))
	if err == nil && target.Scheme == "https" && target.Host != "" && target.User == nil &&
		s.isAllowedFrontendOrigin(target.Scheme+"://"+target.Host) {
		return target.String()
	}
	return s.cfg.FrontendReturnURL.String()
}

func (s *Server) requiresBrowserSource(path string) bool {
	return path == s.cfg.PublicPath("v1/auth/session") ||
		path == s.cfg.PublicPath("v1/auth/logout") ||
		path == s.cfg.PublicPath("v1/sync/exchange") ||
		path == s.cfg.PublicPath("v1/sync/resolve")
}

func (s *Server) cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		r, w := c.Request, c.Writer
		origin := r.Header.Get("Origin")
		isAPI := strings.HasPrefix(r.URL.Path, s.cfg.PublicPath("v1/"))
		requiresSource := s.requiresBrowserSource(r.URL.Path) && r.Method != http.MethodOptions
		if (requiresSource && !s.isAllowedFrontendOrigin(origin)) || (isAPI && origin != "" && !s.isAllowedFrontendOrigin(origin)) {
			writeAPIError(w, http.StatusForbidden, "invalid_origin")
			c.Abort()
			return
		}
		if requiresSource && !s.validRefererForOrigin(r, true) {
			writeAPIError(w, http.StatusForbidden, "invalid_referer")
			c.Abort()
			return
		}
		if r.URL.Path == s.cfg.PublicPath("v1/sync/ws") && !s.validRefererForOrigin(r, false) {
			writeAPIError(w, http.StatusForbidden, "invalid_referer")
			c.Abort()
			return
		}
		if s.isAllowedFrontendOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions && strings.HasPrefix(r.URL.Path, s.cfg.PublicPath("v1/")) {
			if !s.isAllowedFrontendOrigin(origin) {
				writeAPIError(w, http.StatusForbidden, "invalid_origin")
				c.Abort()
				return
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			c.Abort()
			return
		}
		c.Next()
	}
}

func (s *Server) securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Cache-Control", "no-store")
		c.Next()
	}
}

func (s *Server) recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.ErrorContext(c.Request.Context(), "http panic recovered",
					"request_id", requestID(c.Request.Context()), "class", "panic", "error", fmt.Sprint(recovered),
					"stack", string(debug.Stack()))
				if !c.Writer.Written() {
					writeAPIError(c.Writer, http.StatusInternalServerError, "internal_error")
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

type contextKey string

const requestIDKey contextKey = "request_id"

func (s *Server) requestLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		r := c.Request
		id := newRequestID()
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		c.Header("X-Request-Id", id)
		started := time.Now()
		c.Request = r.WithContext(ctx)
		c.Next()
		s.logger.InfoContext(ctx, "http request", "request_id", id, "method", r.Method, "path", r.URL.Path,
			"status", c.Writer.Status(), "client_ip", s.clientIP(r), "duration_ms", time.Since(started).Milliseconds())
	}
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
