package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/web/webctx"
	messagetmpl "github.com/steipete/discrawl/internal/web/templates/messages"
	"github.com/stretchr/testify/require"
)

// mockSessionManager implements SessionManager for testing.
type mockSessionManager struct {
	data map[string]interface{}
}

func newMockSessionManager() *mockSessionManager {
	return &mockSessionManager{data: make(map[string]interface{})}
}

func (m *mockSessionManager) GetBool(ctx context.Context, key string) bool {
	v, ok := m.data[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

func (m *mockSessionManager) Put(ctx context.Context, key string, val interface{}) {
	m.data[key] = val
}

func setupMessagesTestDB(t *testing.T) (*store.Registry, *store.GuildStore) {
	t.Helper()
	ctx := context.Background()
	tempDir := t.TempDir()
	reg, err := store.NewRegistry(ctx, store.RegistryConfig{DataDir: tempDir})
	require.NoError(t, err)

	dbPath := tempDir + "/guilds/test-guild.db"
	gs, err := store.OpenGuildStore(ctx, dbPath, "test-guild")
	require.NoError(t, err)

	// Insert test data
	db := gs.DB()
	_, err = db.ExecContext(ctx, `INSERT INTO channels (id, guild_id, kind, name, topic, raw_json, updated_at) VALUES
		('ch1', 'test-guild', 'text', 'general', 'Welcome channel', '{}', datetime('now'))`)
	require.NoError(t, err)

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		created := now.Add(time.Duration(-i) * time.Minute).Format("2006-01-02T15:04:05")
		_, err = db.ExecContext(ctx, `INSERT INTO messages (id, guild_id, channel_id, author_id, message_type, content, normalized_content, created_at, raw_json, updated_at) VALUES
			(?, 'test-guild', 'ch1', 'u1', 0, ?, ?, ?, '{}', datetime('now'))`,
			"msg"+string(rune('0'+i)), "content "+string(rune('0'+i)), "content "+string(rune('0'+i)), created)
		require.NoError(t, err)
	}

	return reg, gs
}

func TestHandleMessageViewer(t *testing.T) {
	reg, gs := setupMessagesTestDB(t)
	sm := newMockSessionManager()

	handler := HandleMessageViewer(reg, sm)

	t.Run("success", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/g/test-guild/channels/ch1", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "test-guild")
		rctx.URLParams.Add("channelID", "ch1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		req = req.WithContext(webctx.WithGuildStore(req.Context(), gs))

		rr := httptest.NewRecorder()
		handler(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		require.Contains(t, rr.Header().Get("Content-Type"), "text/html")
	})

	t.Run("no guild store", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/g/test-guild/channels/ch1", nil)
		rr := httptest.NewRecorder()
		handler(rr, req)

		require.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestHandleMessageList(t *testing.T) {
	reg, gs := setupMessagesTestDB(t)

	handler := HandleMessageList(reg)

	t.Run("success without cursor", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/g/test-guild/channels/ch1/messages", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "test-guild")
		rctx.URLParams.Add("channelID", "ch1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		req = req.WithContext(webctx.WithGuildStore(req.Context(), gs))

		rr := httptest.NewRecorder()
		handler(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		require.Contains(t, rr.Header().Get("Content-Type"), "text/html")
	})

	t.Run("success with before cursor", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/g/test-guild/channels/ch1/messages?before=msg2", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "test-guild")
		rctx.URLParams.Add("channelID", "ch1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		req = req.WithContext(webctx.WithGuildStore(req.Context(), gs))

		rr := httptest.NewRecorder()
		handler(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("no guild store", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/g/test-guild/channels/ch1/messages", nil)
		rr := httptest.NewRecorder()
		handler(rr, req)

		require.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestGroupMessages(t *testing.T) {
	now := time.Now()
	messages := []store.MessageRow{
		{MessageID: "m1", AuthorID: "u1", AuthorName: "Alice", CreatedAt: now},
		{MessageID: "m2", AuthorID: "u1", AuthorName: "Alice", CreatedAt: now.Add(1 * time.Minute)},
		{MessageID: "m3", AuthorID: "u2", AuthorName: "Bob", CreatedAt: now.Add(2 * time.Minute)},
		{MessageID: "m4", AuthorID: "u1", AuthorName: "Alice", CreatedAt: now.Add(10 * time.Minute)}, // New group (>5min)
	}

	sections := groupMessages(messages)

	require.Len(t, sections, 1) // All same day
	require.Len(t, sections[0].Groups, 3) // Alice, Bob, Alice (new group)
	require.Equal(t, "u1", sections[0].Groups[0].AuthorID)
	require.Len(t, sections[0].Groups[0].Messages, 2) // m1 and m2 collapsed
	require.Equal(t, "u2", sections[0].Groups[1].AuthorID)
	require.Len(t, sections[0].Groups[1].Messages, 1) // m3
	require.Equal(t, "u1", sections[0].Groups[2].AuthorID)
	require.Len(t, sections[0].Groups[2].Messages, 1) // m4 (new group)
}

func TestTruncateToDay(t *testing.T) {
	input := time.Date(2024, 3, 10, 15, 30, 45, 0, time.UTC)
	expected := time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC)
	result := truncateToDay(input)
	require.Equal(t, expected, result)
}

func TestLastMessageTime(t *testing.T) {
	t.Run("empty group", func(t *testing.T) {
		g := messagetmpl.MessageGroup{}
		result := lastMessageTime(g)
		require.True(t, result.IsZero())
	})

	t.Run("with messages", func(t *testing.T) {
		now := time.Now()
		g := messagetmpl.MessageGroup{
			Messages: []store.MessageRow{
				{CreatedAt: now},
				{CreatedAt: now.Add(1 * time.Minute)},
			},
		}
		result := lastMessageTime(g)
		require.Equal(t, now.Add(1*time.Minute), result)
	})
}
