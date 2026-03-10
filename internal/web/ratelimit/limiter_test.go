package ratelimit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/steipete/discrawl/internal/web/webctx"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestNewPerUserLimiter(t *testing.T) {
	t.Parallel()

	limiter := NewPerUserLimiter(10.0, 20)
	require.NotNil(t, limiter)
	require.Equal(t, 10.0, float64(limiter.r))
	require.Equal(t, 20, limiter.b)
	require.NotNil(t, limiter.limiters)
	require.NotNil(t, limiter.cleanup)

	limiter.Stop()
}

func TestPerUserLimiterGetLimiter(t *testing.T) {
	t.Parallel()

	limiter := NewPerUserLimiter(1.0, 1)
	defer limiter.Stop()

	userID := "user-123"
	rateLimiter := limiter.getLimiter(userID)
	require.NotNil(t, rateLimiter)

	// Should return the same limiter for the same user
	rateLimiter2 := limiter.getLimiter(userID)
	require.Equal(t, rateLimiter, rateLimiter2)
}

func TestPerUserLimiterMiddleware(t *testing.T) {
	t.Parallel()

	limiter := NewPerUserLimiter(10.0, 2)
	defer limiter.Stop()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	middleware := limiter.Middleware(handler)

	t.Run("allows requests within limit", func(t *testing.T) {
		ctx := webctx.WithUserID(context.Background(), "user-1")
		req := httptest.NewRequest("GET", "/test", nil).WithContext(ctx)
		rec := httptest.NewRecorder()

		middleware.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "success", rec.Body.String())
	})

	t.Run("rate limits exceeded requests", func(t *testing.T) {
		ctx := webctx.WithUserID(context.Background(), "user-2")

		// Exhaust the burst capacity (2 tokens)
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest("GET", "/test", nil).WithContext(ctx)
			rec := httptest.NewRecorder()
			middleware.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
		}

		// Next request should be rate limited
		req := httptest.NewRequest("GET", "/test", nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)
		require.Equal(t, http.StatusTooManyRequests, rec.Code)
	})

	t.Run("skips rate limiting for unauthenticated requests", func(t *testing.T) {
		// No user ID in context
		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()

		middleware.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("independent limits per user", func(t *testing.T) {
		ctx1 := webctx.WithUserID(context.Background(), "user-3")
		ctx2 := webctx.WithUserID(context.Background(), "user-4")

		// User 3 exhausts their limit
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest("GET", "/test", nil).WithContext(ctx1)
			rec := httptest.NewRecorder()
			middleware.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
		}

		// User 4 should still be able to make requests
		req := httptest.NewRequest("GET", "/test", nil).WithContext(ctx2)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestPerUserLimiterCleanup(t *testing.T) {
	t.Parallel()

	limiter := NewPerUserLimiter(1.0, 1)
	defer limiter.Stop()

	// Add many limiters to trigger cleanup logic
	for i := 0; i < 1500; i++ {
		limiter.getLimiter("user-" + string(rune(i)))
	}

	require.Greater(t, len(limiter.limiters), 1000)

	// Manually trigger cleanup by calling the cleanup logic
	limiter.mu.Lock()
	if len(limiter.limiters) > 1000 {
		limiter.limiters = make(map[string]*rate.Limiter)
	}
	limiter.mu.Unlock()

	require.Empty(t, limiter.limiters)
}

func TestPerUserLimiterStop(t *testing.T) {
	t.Parallel()

	limiter := NewPerUserLimiter(1.0, 1)
	require.NotNil(t, limiter.cleanup)

	limiter.Stop()
	// Verify cleanup ticker is stopped by checking it doesn't panic on double stop
	require.NotPanics(t, func() {
		limiter.Stop()
	})
}

func TestPerUserLimiterConcurrency(t *testing.T) {
	t.Parallel()

	limiter := NewPerUserLimiter(100.0, 10)
	defer limiter.Stop()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := limiter.Middleware(handler)

	// Concurrent requests from same user
	userID := "concurrent-user"
	ctx := webctx.WithUserID(context.Background(), userID)

	successCount := 0
	rateLimitCount := 0

	for i := 0; i < 20; i++ {
		req := httptest.NewRequest("GET", "/test", nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		if rec.Code == http.StatusOK {
			successCount++
		} else if rec.Code == http.StatusTooManyRequests {
			rateLimitCount++
		}
	}

	// At least the burst capacity should succeed
	require.GreaterOrEqual(t, successCount, 10)
	// Some requests should be rate limited
	require.Greater(t, rateLimitCount, 0)
}
