package web

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/alexedwards/scs/v2"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/syncer"
	"github.com/steipete/discrawl/internal/web/auth"
	"github.com/steipete/discrawl/internal/web/ratelimit"
	"github.com/steipete/discrawl/internal/web/sse"
	"golang.org/x/oauth2"
)

// Server holds the HTTP server state.
type Server struct {
	cfg            config.Config
	router         chi.Router
	registry       *store.Registry
	logger         *slog.Logger
	sessionManager *scs.SessionManager
	oauthCfg       *oauth2.Config
	sseBroker      *sse.Broker
	rateLimiter    *ratelimit.PerUserLimiter
	syncer         *syncer.Syncer
}

// NewServer creates a new Server.
func NewServer(cfg config.Config, registry *store.Registry, logger *slog.Logger) *Server {
	broker := sse.NewBroker()
	// 10 requests per second per user, burst of 20.
	limiter := ratelimit.NewPerUserLimiter(10.0, 20)
	s := &Server{
		cfg:         cfg,
		registry:    registry,
		logger:      logger,
		sseBroker:   broker,
		rateLimiter: limiter,
	}

	// Initialise session manager backed by meta.db SQLite store.
	sm := scs.New()
	sm.Lifetime = 30 * 24 * time.Hour
	sm.Cookie.HttpOnly = true
	sm.Cookie.SameSite = http.SameSiteLaxMode
	if cfg.Web.SessionSecret != "" {
		sm.Cookie.Secure = true
	}
	if registry != nil && registry.Meta() != nil {
		sqliteStore := auth.NewSQLiteStore(registry.Meta().DB(), 10*time.Minute)
		sm.Store = sqliteStore
	}
	s.sessionManager = sm

	// Initialise OAuth2 config.
	clientID := cfg.Web.OAuthClientID
	if clientID == "" {
		clientID = os.Getenv(cfg.Web.OAuthClientIDEnv)
	}
	clientSecret := os.Getenv(cfg.Web.OAuthSecretEnv)
	redirectURI := cfg.Web.OAuthRedirectURI
	if redirectURI == "" {
		redirectURI = fmt.Sprintf("http://%s:%d/auth/callback", cfg.Web.Host, cfg.Web.Port)
	}
	s.oauthCfg = auth.NewOAuth2Config(auth.OAuthConfig{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURI:  redirectURI,
	})

	s.router = s.buildRouter()
	return s
}

// SetSyncer attaches a syncer and wires it to publish SSE events.
func (s *Server) SetSyncer(sync *syncer.Syncer) {
	s.syncer = sync
	if sync != nil && s.sseBroker != nil {
		sync.SetEventHook(NewSyncerSSEHook(s.sseBroker))
	}
}

func (s *Server) buildRouter() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(RequestLogger(s.logger))
	r.Use(middleware.Recoverer)
	r.Use(s.sessionManager.LoadAndSave)
	s.routes(r)
	return r
}

// StartTail starts the syncer tail in a background goroutine.
// Returns an error if syncer is not set.
func (s *Server) StartTail(ctx context.Context, guildIDs []string, repairEvery time.Duration) error {
	if s.syncer == nil {
		return fmt.Errorf("syncer not configured")
	}
	go func() {
		if err := s.syncer.RunTail(ctx, guildIDs, repairEvery); err != nil {
			s.logger.Error("tail stopped", "err", err)
		}
	}()
	s.logger.Info("syncer tail started", "guilds", len(guildIDs), "repair_every", repairEvery)
	return nil
}

// ListenAndServe starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context, host string, port int) error {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // disabled; SSE needs long-lived writes. Per-route timeouts are used instead.
		IdleTimeout:  120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("web server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.logger.Info("shutting down web server")
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}
