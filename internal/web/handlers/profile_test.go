package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/steipete/discrawl/internal/web/webctx"
	"github.com/stretchr/testify/require"
)

func TestHandleMemberProfile(t *testing.T) {
	t.Parallel()

	t.Run("renders member profile with messages", func(t *testing.T) {
		testDB := setupTestGuildDB(t)
		defer testDB.Close()

		ctx := context.Background()
		// Insert test member
		_, err := testDB.DB().ExecContext(ctx, `
			insert into members (guild_id, user_id, username, nick, role_ids_json, raw_json, updated_at)
			values ('test-guild', 'user-1', 'alice', 'Alice the Great', '[]', '{}', datetime('now'))
		`)
		require.NoError(t, err)

		// Insert test channel
		_, err = testDB.DB().ExecContext(ctx, `
			insert into channels (id, guild_id, kind, name, raw_json, updated_at)
			values ('ch-1', 'test-guild', 'text', 'general', '{}', datetime('now'))
		`)
		require.NoError(t, err)

		// Insert test message from this member
		_, err = testDB.DB().ExecContext(ctx, `
			insert into messages (id, guild_id, channel_id, author_id, message_type, content, normalized_content, created_at, raw_json, updated_at)
			values ('msg-1', 'test-guild', 'ch-1', 'user-1', 0, 'Hello world', 'hello world', datetime('now'), '{}', datetime('now'))
		`)
		require.NoError(t, err)

		ctx = webctx.WithGuildStore(ctx, testDB)
		req := httptest.NewRequest("GET", "/app/g/guild-1/members/user-1", nil)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		rctx.URLParams.Add("userID", "user-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler := HandleMemberProfile()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
		// Template should render member and messages
		body := rec.Body.String()
		require.NotEmpty(t, body)
	})

	t.Run("returns 404 when guild store not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/app/g/guild-1/members/user-1", nil)
		rec := httptest.NewRecorder()

		handler := HandleMemberProfile()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNotFound, rec.Code)
		require.Contains(t, rec.Body.String(), "guild not found")
	})

	t.Run("returns 404 when member not found", func(t *testing.T) {
		testDB := setupTestGuildDB(t)
		defer testDB.Close()

		ctx := webctx.WithGuildStore(context.Background(), testDB)
		req := httptest.NewRequest("GET", "/app/g/guild-1/members/user-999", nil)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		rctx.URLParams.Add("userID", "user-999")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler := HandleMemberProfile()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNotFound, rec.Code)
		require.Contains(t, rec.Body.String(), "member not found")
	})

	t.Run("handles missing messages gracefully", func(t *testing.T) {
		testDB := setupTestGuildDB(t)
		defer testDB.Close()

		ctx := context.Background()
		// Insert member but no messages
		_, err := testDB.DB().ExecContext(ctx, `
			insert into members (guild_id, user_id, username, role_ids_json, raw_json, updated_at)
			values ('test-guild', 'user-1', 'alice', '[]', '{}', datetime('now'))
		`)
		require.NoError(t, err)

		ctx = webctx.WithGuildStore(ctx, testDB)
		req := httptest.NewRequest("GET", "/app/g/guild-1/members/user-1", nil)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		rctx.URLParams.Add("userID", "user-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler := HandleMemberProfile()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
	})
}
