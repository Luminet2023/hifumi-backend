package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Luminet2023/hifumi-backend/internal/auth"
	"github.com/Luminet2023/hifumi-backend/internal/config"
	"github.com/Luminet2023/hifumi-backend/internal/httpapi"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	mysqlstore "github.com/Luminet2023/hifumi-backend/internal/store/mysql"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"github.com/Luminet2023/hifumi-backend/migrations"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("study-list-api stopped", "error", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	command := "serve"
	if len(arguments) > 0 {
		command = arguments[0]
	}
	switch command {
	case "serve":
		return serve()
	case "migrate":
		return migrate()
	case "healthcheck":
		return healthcheck()
	default:
		return fmt.Errorf("unknown command %q; expected serve, migrate, or healthcheck", command)
	}
}

func serve() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.ValidateServe(); err != nil {
		return err
	}
	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)
	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startupContext, cancel := context.WithTimeout(rootContext, 10*time.Second)
	defer cancel()
	db, err := mysqlstore.Open(startupContext, cfg.MySQLDSN)
	if err != nil {
		return fmt.Errorf("connect MySQL: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := migrations.Check(startupContext, db); err != nil {
		return fmt.Errorf("schema check: %w (run migrate first)", err)
	}
	redisClient, err := realtime.New(cfg.RedisURL, cfg.RedisKeyPrefix)
	if err != nil {
		return err
	}
	defer redisClient.Close()
	if err := redisClient.Ping(startupContext); err != nil {
		return fmt.Errorf("connect Redis: %w", err)
	}
	tokens, err := auth.NewManager(cfg.SessionSecret, cfg.PublicIssuer(), cfg.SessionAudience)
	if err != nil {
		return err
	}
	oauthHTTPClient := &http.Client{
		Timeout: 12 * time.Second,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 8 * time.Second,
			IdleConnTimeout:       60 * time.Second,
		},
	}
	provider := auth.NewProvider(
		cfg.LinuxDOClientID, cfg.LinuxDOSecret, cfg.PublicURL("v1/auth/callback"),
		cfg.FrontendReturnURL.String(), tokens, oauthHTTPClient,
	)
	store := mysqlstore.New(db)
	hub := realtime.NewHub()
	api, err := httpapi.NewServer(httpapi.Dependencies{
		Config: cfg, Tokens: tokens, OAuth: provider, Profiles: store,
		Sync: syncservice.NewService(store), Realtime: redisClient, Hub: hub, DB: db,
		CheckSchema: func(ctx context.Context) error { return migrations.Check(ctx, db) },
		Logger:      logger,
		Build:       httpapi.BuildInfo{Version: version, Commit: commit, BuildTime: buildTime},
	})
	if err != nil {
		return err
	}

	go runHintSubscriber(rootContext, redisClient, hub, logger)
	go func() {
		if err := mysqlstore.RunOutbox(rootContext, db, redisClient, logger); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("realtime outbox stopped", "error", err)
			stop()
		}
	}()
	requestContext, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	httpServer := &http.Server{
		Addr: cfg.HTTPAddr, Handler: api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
		BaseContext:       func(net.Listener) context.Context { return requestContext },
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr, "version", version, "commit", commit)
		serverErrors <- httpServer.ListenAndServe()
	}()
	select {
	case <-rootContext.Done():
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	err = httpServer.Shutdown(shutdownContext)
	cancelRequests()
	return err
}

func migrate() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.ValidateMigrate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := mysqlstore.Open(ctx, cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrations.Up(ctx, db); err != nil {
		return err
	}
	slog.Info("database schema is current", "version", migrations.CurrentVersion)
	return nil
}

func healthcheck() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("HTTP_ADDR: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	target := "http://" + net.JoinHostPort(host, port) + cfg.PublicPath("healthz")
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(target)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health endpoint returned %s", response.Status)
	}
	return nil
}

func runHintSubscriber(ctx context.Context, client *realtime.Client, hub *realtime.Hub, logger *slog.Logger) {
	for ctx.Err() == nil {
		err := client.SubscribeHints(ctx, func(hint realtime.Hint) {
			payload, encodeErr := httpapi.EncodeSyncHint(hint.BaselineID, hint.ServerCursor, hint.ServerVersion)
			if encodeErr == nil {
				hub.Broadcast(hint, payload)
			}
		})
		if ctx.Err() != nil {
			return
		}
		logger.WarnContext(ctx, "redis hint subscriber reconnecting", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func newLogger(level string) *slog.Logger {
	var parsed slog.Level
	switch strings.ToLower(level) {
	case "debug":
		parsed = slog.LevelDebug
	case "warn":
		parsed = slog.LevelWarn
	case "error":
		parsed = slog.LevelError
	default:
		parsed = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parsed}))
}
