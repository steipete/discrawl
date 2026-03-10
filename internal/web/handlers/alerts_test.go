package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/web/webctx"
	"github.com/stretchr/testify/require"
)

// mockMetaStore implements a minimal MetaStore for testing alerts.
type mockMetaStore struct {
	alerts map[string]store.AlertRecord
	err    error
}

func newMockMetaStore() *mockMetaStore {
	return &mockMetaStore{
		alerts: make(map[string]store.AlertRecord),
	}
}

func (m *mockMetaStore) CreateAlert(ctx context.Context, alert store.AlertRecord) error {
	if m.err != nil {
		return m.err
	}
	m.alerts[alert.ID] = alert
	return nil
}

func (m *mockMetaStore) ListAlerts(ctx context.Context, guildID string) ([]store.AlertRecord, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []store.AlertRecord
	for _, alert := range m.alerts {
		if alert.GuildID == guildID {
			result = append(result, alert)
		}
	}
	return result, nil
}

func (m *mockMetaStore) GetAlert(ctx context.Context, alertID string) (store.AlertRecord, error) {
	if m.err != nil {
		return store.AlertRecord{}, m.err
	}
	alert, ok := m.alerts[alertID]
	if !ok {
		return store.AlertRecord{}, sql.ErrNoRows
	}
	return alert, nil
}

func (m *mockMetaStore) UpdateAlert(ctx context.Context, alertID, keywords string) error {
	if m.err != nil {
		return m.err
	}
	alert, ok := m.alerts[alertID]
	if !ok {
		return sql.ErrNoRows
	}
	alert.Keywords = keywords
	m.alerts[alertID] = alert
	return nil
}

func (m *mockMetaStore) DeleteAlert(ctx context.Context, alertID string) error {
	if m.err != nil {
		return m.err
	}
	if _, ok := m.alerts[alertID]; !ok {
		return sql.ErrNoRows
	}
	delete(m.alerts, alertID)
	return nil
}

func TestAlertsHandler_CreateAlert(t *testing.T) {
	t.Parallel()

	t.Run("creates alert successfully", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		handler := NewAlertsHandler(mockMeta)

		reqBody := map[string]string{"keywords": "important, urgent"}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/g/guild-1/alerts", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = setUserID(req, "user-1")

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.CreateAlert(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		var result store.AlertRecord
		err := json.NewDecoder(rec.Body).Decode(&result)
		require.NoError(t, err)
		require.Equal(t, "guild-1", result.GuildID)
		require.Equal(t, "user-1", result.UserID)
		require.Equal(t, "important, urgent", result.Keywords)
		require.NotEmpty(t, result.ID)
	})

	t.Run("returns 401 when user not authenticated", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		handler := NewAlertsHandler(mockMeta)

		reqBody := map[string]string{"keywords": "test"}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/g/guild-1/alerts", bytes.NewReader(body))
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.CreateAlert(rec, req)

		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("returns 400 on invalid JSON", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		handler := NewAlertsHandler(mockMeta)

		req := httptest.NewRequest("POST", "/api/v1/g/guild-1/alerts", bytes.NewReader([]byte("invalid")))
		req = setUserID(req, "user-1")

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.CreateAlert(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestAlertsHandler_ListAlerts(t *testing.T) {
	t.Parallel()

	t.Run("lists alerts for guild", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		mockMeta.alerts["alert-1"] = store.AlertRecord{
			ID:       "alert-1",
			GuildID:  "guild-1",
			UserID:   "user-1",
			Keywords: "keyword1",
		}
		mockMeta.alerts["alert-2"] = store.AlertRecord{
			ID:       "alert-2",
			GuildID:  "guild-1",
			UserID:   "user-2",
			Keywords: "keyword2",
		}

		handler := NewAlertsHandler(mockMeta)

		req := httptest.NewRequest("GET", "/api/v1/g/guild-1/alerts", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("guildID", "guild-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.ListAlerts(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		var result map[string][]store.AlertRecord
		err := json.NewDecoder(rec.Body).Decode(&result)
		require.NoError(t, err)
		require.Len(t, result["alerts"], 2)
	})
}

func TestAlertsHandler_GetAlert(t *testing.T) {
	t.Parallel()

	t.Run("gets alert by ID", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		mockMeta.alerts["alert-1"] = store.AlertRecord{
			ID:       "alert-1",
			GuildID:  "guild-1",
			UserID:   "user-1",
			Keywords: "test",
		}

		handler := NewAlertsHandler(mockMeta)

		req := httptest.NewRequest("GET", "/api/v1/g/guild-1/alerts/alert-1", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("alertID", "alert-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.GetAlert(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		var result store.AlertRecord
		err := json.NewDecoder(rec.Body).Decode(&result)
		require.NoError(t, err)
		require.Equal(t, "alert-1", result.ID)
		require.Equal(t, "test", result.Keywords)
	})

	t.Run("returns 404 when alert not found", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		handler := NewAlertsHandler(mockMeta)

		req := httptest.NewRequest("GET", "/api/v1/g/guild-1/alerts/nonexistent", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("alertID", "nonexistent")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.GetAlert(rec, req)

		require.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestAlertsHandler_UpdateAlert(t *testing.T) {
	t.Parallel()

	t.Run("updates alert keywords", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		mockMeta.alerts["alert-1"] = store.AlertRecord{
			ID:       "alert-1",
			GuildID:  "guild-1",
			UserID:   "user-1",
			Keywords: "old",
		}

		handler := NewAlertsHandler(mockMeta)

		reqBody := map[string]string{"keywords": "new, updated"}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("PATCH", "/api/v1/g/guild-1/alerts/alert-1", bytes.NewReader(body))
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("alertID", "alert-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.UpdateAlert(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "new, updated", mockMeta.alerts["alert-1"].Keywords)
	})
}

func TestAlertsHandler_DeleteAlert(t *testing.T) {
	t.Parallel()

	t.Run("deletes alert", func(t *testing.T) {
		mockMeta := newMockMetaStore()
		mockMeta.alerts["alert-1"] = store.AlertRecord{
			ID:      "alert-1",
			GuildID: "guild-1",
			UserID:  "user-1",
		}

		handler := NewAlertsHandler(mockMeta)

		req := httptest.NewRequest("DELETE", "/api/v1/g/guild-1/alerts/alert-1", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("alertID", "alert-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rec := httptest.NewRecorder()
		handler.DeleteAlert(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
		_, exists := mockMeta.alerts["alert-1"]
		require.False(t, exists)
	})
}

// setUserID is a helper to set user ID in request context for tests.
func setUserID(r *http.Request, userID string) *http.Request {
	ctx := webctx.WithUserID(r.Context(), userID)
	return r.WithContext(ctx)
}
