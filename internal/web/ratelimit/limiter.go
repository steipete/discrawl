package ratelimit

import (
	"net/http"
	"sync"
	"time"

	"github.com/steipete/discrawl/internal/web/webctx"
	"golang.org/x/time/rate"
)

// PerUserLimiter holds rate limiters per user ID.
type PerUserLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
	r        rate.Limit   // requests per second
	b        int          // burst capacity
	cleanup  *time.Ticker // cleanup old entries periodically
}

// NewPerUserLimiter creates a new per-user rate limiter.
// r is the rate (requests per second), b is the burst size.
func NewPerUserLimiter(r float64, b int) *PerUserLimiter {
	limiter := &PerUserLimiter{
		limiters: make(map[string]*rate.Limiter),
		r:        rate.Limit(r),
		b:        b,
		cleanup:  time.NewTicker(5 * time.Minute),
	}
	go limiter.cleanupLoop()
	return limiter
}

// getLimiter returns the rate limiter for a given user ID, creating it if necessary.
func (l *PerUserLimiter) getLimiter(userID string) *rate.Limiter {
	l.mu.RLock()
	limiter, exists := l.limiters[userID]
	l.mu.RUnlock()
	if exists {
		return limiter
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	// Double-check after acquiring write lock.
	if limiter, exists := l.limiters[userID]; exists {
		return limiter
	}
	limiter = rate.NewLimiter(l.r, l.b)
	l.limiters[userID] = limiter
	return limiter
}

// cleanupLoop periodically removes stale limiters.
func (l *PerUserLimiter) cleanupLoop() {
	for range l.cleanup.C {
		l.mu.Lock()
		// Remove limiters that haven't been used recently (simple heuristic: allow all).
		// In a production system, track last access time per limiter.
		if len(l.limiters) > 1000 {
			l.limiters = make(map[string]*rate.Limiter)
		}
		l.mu.Unlock()
	}
}

// Middleware returns a middleware that rate-limits requests per user.
func (l *PerUserLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := webctx.GetUserID(r.Context())
		if userID == "" {
			// No user context — skip rate limiting (shouldn't happen on auth-protected routes).
			next.ServeHTTP(w, r)
			return
		}

		limiter := l.getLimiter(userID)
		if !limiter.Allow() {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Stop stops the cleanup goroutine.
func (l *PerUserLimiter) Stop() {
	l.cleanup.Stop()
}
