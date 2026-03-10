package web

import (
	"context"
	"testing"
	"time"

	"github.com/steipete/discrawl/internal/web/sse"
	"github.com/stretchr/testify/require"
)

func TestNewSyncerSSEHook(t *testing.T) {
	t.Parallel()

	broker := sse.NewBroker()
	hook := NewSyncerSSEHook(broker)

	require.NotNil(t, hook)
	require.Equal(t, broker, hook.broker)
}

func TestSyncerSSEHook_OnMessageWrite(t *testing.T) {
	t.Parallel()

	t.Run("publishes message event to broker", func(t *testing.T) {
		broker := sse.NewBroker()
		hook := NewSyncerSSEHook(broker)

		guildID := "guild-1"
		ch := broker.Subscribe(guildID)
		defer broker.Unsubscribe(guildID, ch)

		ctx := context.Background()
		err := hook.OnMessageWrite(ctx, guildID, "channel-1", "msg-1", "create")
		require.NoError(t, err)

		select {
		case event := <-ch:
			require.Equal(t, "msg-1", event.ID)
			require.Equal(t, "message", event.Type)
			require.Equal(t, "create", event.Data)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for event")
		}
	})

	t.Run("publishes update event", func(t *testing.T) {
		broker := sse.NewBroker()
		hook := NewSyncerSSEHook(broker)

		guildID := "guild-1"
		ch := broker.Subscribe(guildID)
		defer broker.Unsubscribe(guildID, ch)

		ctx := context.Background()
		err := hook.OnMessageWrite(ctx, guildID, "channel-1", "msg-2", "update")
		require.NoError(t, err)

		select {
		case event := <-ch:
			require.Equal(t, "msg-2", event.ID)
			require.Equal(t, "message", event.Type)
			require.Equal(t, "update", event.Data)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for event")
		}
	})

	t.Run("handles nil broker gracefully", func(t *testing.T) {
		hook := NewSyncerSSEHook(nil)

		ctx := context.Background()
		err := hook.OnMessageWrite(ctx, "guild-1", "channel-1", "msg-1", "create")
		require.NoError(t, err)
	})
}

func TestSyncerSSEHook_OnMemberWrite(t *testing.T) {
	t.Parallel()

	t.Run("publishes member join event", func(t *testing.T) {
		broker := sse.NewBroker()
		hook := NewSyncerSSEHook(broker)

		guildID := "guild-1"
		ch := broker.Subscribe(guildID)
		defer broker.Unsubscribe(guildID, ch)

		ctx := context.Background()
		err := hook.OnMemberWrite(ctx, guildID, "user-1", "join")
		require.NoError(t, err)

		select {
		case event := <-ch:
			require.Equal(t, "user-1", event.ID)
			require.Equal(t, "member", event.Type)
			require.Equal(t, "join", event.Data)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for event")
		}
	})

	t.Run("publishes member leave event", func(t *testing.T) {
		broker := sse.NewBroker()
		hook := NewSyncerSSEHook(broker)

		guildID := "guild-1"
		ch := broker.Subscribe(guildID)
		defer broker.Unsubscribe(guildID, ch)

		ctx := context.Background()
		err := hook.OnMemberWrite(ctx, guildID, "user-2", "leave")
		require.NoError(t, err)

		select {
		case event := <-ch:
			require.Equal(t, "user-2", event.ID)
			require.Equal(t, "member", event.Type)
			require.Equal(t, "leave", event.Data)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for event")
		}
	})

	t.Run("handles nil broker gracefully", func(t *testing.T) {
		hook := NewSyncerSSEHook(nil)

		ctx := context.Background()
		err := hook.OnMemberWrite(ctx, "guild-1", "user-1", "join")
		require.NoError(t, err)
	})
}

func TestSyncerSSEHook_MultipleEvents(t *testing.T) {
	t.Parallel()

	broker := sse.NewBroker()
	hook := NewSyncerSSEHook(broker)

	guildID := "guild-1"
	ch := broker.Subscribe(guildID)
	defer broker.Unsubscribe(guildID, ch)

	ctx := context.Background()

	// Send multiple events
	err := hook.OnMessageWrite(ctx, guildID, "channel-1", "msg-1", "create")
	require.NoError(t, err)

	err = hook.OnMemberWrite(ctx, guildID, "user-1", "join")
	require.NoError(t, err)

	err = hook.OnMessageWrite(ctx, guildID, "channel-1", "msg-2", "update")
	require.NoError(t, err)

	// Verify all events received
	events := []sse.Event{}
	for i := 0; i < 3; i++ {
		select {
		case event := <-ch:
			events = append(events, event)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timeout waiting for event %d", i)
		}
	}

	require.Len(t, events, 3)
	require.Equal(t, "msg-1", events[0].ID)
	require.Equal(t, "message", events[0].Type)
	require.Equal(t, "user-1", events[1].ID)
	require.Equal(t, "member", events[1].Type)
	require.Equal(t, "msg-2", events[2].ID)
	require.Equal(t, "message", events[2].Type)
}
