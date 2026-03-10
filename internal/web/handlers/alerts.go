package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/web/auth"
)

// AlertStore defines the interface for alert storage operations.
type AlertStore interface {
	CreateAlert(ctx context.Context, alert store.AlertRecord) error
	ListAlerts(ctx context.Context, guildID string) ([]store.AlertRecord, error)
	GetAlert(ctx context.Context, alertID string) (store.AlertRecord, error)
	UpdateAlert(ctx context.Context, alertID, keywords string) error
	DeleteAlert(ctx context.Context, alertID string) error
}

// AlertsHandler handles keyword alert CRUD operations.
type AlertsHandler struct {
	meta AlertStore
}

// NewAlertsHandler creates a new alerts handler.
func NewAlertsHandler(meta AlertStore) *AlertsHandler {
	return &AlertsHandler{meta: meta}
}

// CreateAlert creates a new keyword alert.
func (h *AlertsHandler) CreateAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	guildID := chi.URLParam(r, "guildID")
	userID := auth.GetUserID(r)
	if userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		Keywords string `json:"keywords"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	alert := store.AlertRecord{
		ID:       uuid.New().String(),
		GuildID:  guildID,
		UserID:   userID,
		Keywords: req.Keywords,
	}

	if err := h.meta.CreateAlert(ctx, alert); err != nil {
		http.Error(w, "failed to create alert", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(alert)
}

// ListAlerts returns all alerts for a guild.
func (h *AlertsHandler) ListAlerts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	guildID := chi.URLParam(r, "guildID")

	alerts, err := h.meta.ListAlerts(ctx, guildID)
	if err != nil {
		http.Error(w, "failed to list alerts", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"alerts": alerts,
	})
}

// GetAlert returns a single alert by ID.
func (h *AlertsHandler) GetAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "alertID")

	alert, err := h.meta.GetAlert(ctx, alertID)
	if err != nil {
		http.Error(w, "alert not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(alert)
}

// UpdateAlert updates an alert's keywords.
func (h *AlertsHandler) UpdateAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "alertID")

	var req struct {
		Keywords string `json:"keywords"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if err := h.meta.UpdateAlert(ctx, alertID, req.Keywords); err != nil {
		http.Error(w, "failed to update alert", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DeleteAlert removes an alert.
func (h *AlertsHandler) DeleteAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "alertID")

	if err := h.meta.DeleteAlert(ctx, alertID); err != nil {
		http.Error(w, "failed to delete alert", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
