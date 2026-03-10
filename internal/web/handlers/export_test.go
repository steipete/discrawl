package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/web/webctx"
	"github.com/stretchr/testify/require"
)

func TestHandleExportMessages(t *testing.T) {
	t.Parallel()

	t.Run("exports messages as CSV", func(t *testing.T) {
		testDB := setupTestGuildDB(t)
		defer testDB.Close()

		ctx := context.Background()
		// Insert test channels
		_, err := testDB.DB().ExecContext(ctx, `
			insert into channels (id, guild_id, kind, name, raw_json, updated_at)
			values ('ch-1', 'test-guild', 'text', 'general', '{}', datetime('now'))
		`)
		require.NoError(t, err)

		// Insert test members
		_, err = testDB.DB().ExecContext(ctx, `
			insert into members (guild_id, user_id, username, display_name, role_ids_json, raw_json, updated_at)
			values
				('test-guild', 'user-1', 'alice', 'Alice', '[]', '{}', datetime('now')),
				('test-guild', 'user-2', 'bob', 'Bob', '[]', '{}', datetime('now'))
		`)
		require.NoError(t, err)

		// Insert test messages
		_, err = testDB.DB().ExecContext(ctx, `
			insert into messages (id, guild_id, channel_id, author_id, message_type, content, normalized_content, created_at, raw_json, updated_at)
			values
				('msg-1', 'test-guild', 'ch-1', 'user-1', 0, 'Hello world', 'hello world', '2024-01-01T12:00:00Z', '{}', '2024-01-01T12:00:00Z'),
				('msg-2', 'test-guild', 'ch-1', 'user-2', 0, 'Hi there', 'hi there', '2024-01-01T12:05:00Z', '{}', '2024-01-01T12:05:00Z')
		`)
		require.NoError(t, err)

		ctx = webctx.WithGuildStore(ctx, testDB)
		req := httptest.NewRequest("GET", "/api/v1/g/guild-1/export/messages?channel=ch-1", nil)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler := HandleExportMessages()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "text/csv; charset=utf-8", rec.Header().Get("Content-Type"))
		require.Contains(t, rec.Header().Get("Content-Disposition"), "attachment")
		require.Contains(t, rec.Header().Get("Content-Disposition"), "messages-guild-1.csv")

		body := rec.Body.String()
		require.Contains(t, body, "message_id,channel_id,channel_name,author_id,author_name,content,created_at")
		require.Contains(t, body, "msg-1")
		require.Contains(t, body, "msg-2")
	})

	t.Run("returns 404 when guild store not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/g/guild-1/export/messages", nil)
		rec := httptest.NewRecorder()

		handler := HandleExportMessages()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("handles empty message list", func(t *testing.T) {
		testDB := setupTestGuildDB(t)
		defer testDB.Close()

		ctx := webctx.WithGuildStore(context.Background(), testDB)
		req := httptest.NewRequest("GET", "/api/v1/g/guild-1/export/messages", nil)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler := HandleExportMessages()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		// Should have header row only
		require.Contains(t, body, "message_id,channel_id,channel_name,author_id,author_name,content,created_at")
	})
}

// setupTestGuildDB creates a temporary file-based GuildStore for testing.
func setupTestGuildDB(t *testing.T) *store.GuildStore {
	dbPath := t.TempDir() + "/test.db"
	gs, err := store.OpenGuildStore(context.Background(), dbPath, "test-guild")
	require.NoError(t, err)
	return gs
}
