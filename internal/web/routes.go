package web

import (
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/steipete/discrawl/internal/web/auth"
	"github.com/steipete/discrawl/internal/web/handlers"
	"github.com/steipete/discrawl/internal/web/static"
	"github.com/steipete/discrawl/internal/web/templates/layout"
)

func (s *Server) routes(r chi.Router) {
	r.Get("/healthz", s.handleHealthz)

	// Home page.
	r.Get("/", s.handleHome)

	// Static assets.
	r.Handle("/static/*", http.StripPrefix("/static", http.FileServer(http.FS(static.Assets))))

	// Auth routes.
	r.Route("/auth", func(r chi.Router) {
		r.Get("/login", auth.HandleLogin(s.sessionManager, s.oauthCfg))
		r.Get("/callback", auth.HandleCallback(s.sessionManager, s.oauthCfg, s.registry.Meta(), s.cfg.Web.SessionSecret))
		r.Get("/logout", auth.HandleLogout(s.sessionManager))
	})

	// App routes (require auth). Apply timeout here (not globally) to avoid killing SSE.
	r.Route("/app", func(r chi.Router) {
		r.Use(middleware.Timeout(30 * time.Second))
		r.Use(auth.RequireAuth(s.sessionManager))

		r.Get("/guilds", handlers.HandleGuildSelector(s.registry.Meta()))

		r.Route("/g/{guildID}", func(r chi.Router) {
			r.Use(TenantResolver(s.registry))

			r.Get("/", handlers.HandleGuildDashboard(s.registry))
			r.Get("/channels", handlers.HandleChannelSidebar(s.registry))
			r.Get("/members", handlers.HandleMemberList(s.registry))
			r.Get("/members/{userID}", handlers.HandleMemberProfile())

			// Rate-limited endpoints.
			r.With(s.rateLimiter.Middleware).Get("/search", handlers.HandleSearch(s.registry))
			r.Get("/analytics", handlers.HandleAnalyticsDashboard())

			r.Route("/c/{channelID}", func(r chi.Router) {
				r.Get("/", handlers.HandleMessageViewer(s.registry))
				r.Get("/messages", handlers.HandleMessageList(s.registry))
			})
		})
	})

	// API routes.
	r.Route("/api/v1", func(r chi.Router) {
		r.Route("/g/{guildID}", func(r chi.Router) {
			r.Use(auth.RequireAuth(s.sessionManager))
			r.Use(TenantResolver(s.registry))

			// Regular API endpoints with timeout.
			r.Group(func(r chi.Router) {
				r.Use(middleware.Timeout(30 * time.Second))

				// Analytics stats endpoints.
				r.Get("/stats/message-volume", handlers.HandleMessageVolume())
				r.Get("/stats/activity-heatmap", handlers.HandleActivityHeatmap())
				r.Get("/stats/top-members", handlers.HandleTopMembers())
				r.Get("/stats/channel-activity", handlers.HandleChannelActivity())
				r.Get("/stats/overview", handlers.HandleOverviewStats())

				// Export (rate-limited).
				r.With(s.rateLimiter.Middleware).Get("/export/messages", handlers.HandleExportMessages())
			})

			// Live SSE stream (no timeout -- long-lived connection).
			r.Get("/live", s.sseBroker.ServeHTTP)
		})
	})
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	// Dev mode: skip login, go straight to guild list.
	if os.Getenv("DISCRAWL_DEV") == "1" {
		http.Redirect(w, r, "/app/guilds", http.StatusSeeOther)
		return
	}
	userID := s.sessionManager.GetString(r.Context(), "user_id")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = layout.Home(userID != "").Render(r.Context(), w)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func comingSoon(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("coming soon: " + name))
	}
}
