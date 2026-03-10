package web

import (
	"context"

	"github.com/steipete/discrawl/internal/web/sse"
)

// SyncerSSEHook bridges syncer events to SSE broker.
type SyncerSSEHook struct {
	broker *sse.Broker
}

// NewSyncerSSEHook creates a hook that publishes syncer events to SSE.
func NewSyncerSSEHook(broker *sse.Broker) *SyncerSSEHook {
	return &SyncerSSEHook{broker: broker}
}

// OnMessageWrite publishes a message event to the SSE broker.
func (h *SyncerSSEHook) OnMessageWrite(ctx context.Context, guildID, channelID, messageID, eventType string) error {
	if h.broker == nil {
		return nil
	}
	// Publish simple event; client will fetch full message HTML via API if needed
	h.broker.Publish(guildID, sse.Event{
		ID:   messageID,
		Type: "message",
		Data: eventType, // "create" or "update"
	})
	return nil
}

// OnMemberWrite publishes a member event to the SSE broker.
func (h *SyncerSSEHook) OnMemberWrite(ctx context.Context, guildID, userID, eventType string) error {
	if h.broker == nil {
		return nil
	}
	h.broker.Publish(guildID, sse.Event{
		ID:   userID,
		Type: "member",
		Data: eventType, // "join" or "leave"
	})
	return nil
}
