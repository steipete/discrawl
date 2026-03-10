package sse

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBrokerSubscribeUnsubscribe(t *testing.T) {
	t.Parallel()

	broker := NewBroker()
	guildID := "guild-1"

	ch := broker.Subscribe(guildID)
	require.NotNil(t, ch)
	require.Len(t, broker.subscribers[guildID], 1)

	broker.Unsubscribe(guildID, ch)
	require.Empty(t, broker.subscribers[guildID])
}

func TestBrokerPublish(t *testing.T) {
	t.Parallel()

	broker := NewBroker()
	guildID := "guild-1"

	ch := broker.Subscribe(guildID)
	defer broker.Unsubscribe(guildID, ch)

	event := Event{
		ID:   "msg-1",
		Type: "message",
		Data: "create",
	}

	broker.Publish(guildID, event)

	select {
	case received := <-ch:
		require.Equal(t, event.ID, received.ID)
		require.Equal(t, event.Type, received.Type)
		require.Equal(t, event.Data, received.Data)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for event")
	}
}

func TestBrokerMultipleSubscribers(t *testing.T) {
	t.Parallel()

	broker := NewBroker()
	guildID := "guild-1"

	ch1 := broker.Subscribe(guildID)
	ch2 := broker.Subscribe(guildID)
	defer broker.Unsubscribe(guildID, ch1)
	defer broker.Unsubscribe(guildID, ch2)

	event := Event{ID: "msg-1", Type: "message", Data: "test"}
	broker.Publish(guildID, event)

	var wg sync.WaitGroup
	wg.Add(2)

	checkChannel := func(ch chan Event) {
		defer wg.Done()
		select {
		case received := <-ch:
			require.Equal(t, event.ID, received.ID)
		case <-time.After(100 * time.Millisecond):
			t.Error("timeout waiting for event")
		}
	}

	go checkChannel(ch1)
	go checkChannel(ch2)

	wg.Wait()
}

func TestBrokerPublishToNonexistentGuild(t *testing.T) {
	t.Parallel()

	broker := NewBroker()
	// Publishing to a guild with no subscribers should not panic
	require.NotPanics(t, func() {
		broker.Publish("nonexistent", Event{ID: "1", Type: "test", Data: ""})
	})
}

func TestBrokerSlowConsumerDropsEvents(t *testing.T) {
	t.Parallel()

	broker := NewBroker()
	guildID := "guild-1"

	ch := broker.Subscribe(guildID)
	defer broker.Unsubscribe(guildID, ch)

	// Fill the buffered channel (capacity 16)
	for i := 0; i < 16; i++ {
		broker.Publish(guildID, Event{ID: string(rune(i)), Type: "fill"})
	}

	// This event should be dropped (slow consumer)
	broker.Publish(guildID, Event{ID: "dropped", Type: "message", Data: "should-drop"})

	// Drain the channel
	count := 0
	droppedSeen := false
	for i := 0; i < 16; i++ {
		select {
		case evt := <-ch:
			count++
			if evt.ID == "dropped" {
				droppedSeen = true
			}
		case <-time.After(50 * time.Millisecond):
			break
		}
	}

	require.Equal(t, 16, count, "should receive exactly 16 events")
	require.False(t, droppedSeen, "dropped event should not be received")
}

func TestBrokerConcurrentAccess(t *testing.T) {
	t.Parallel()

	broker := NewBroker()
	guildID := "guild-1"

	var wg sync.WaitGroup
	numSubscribers := 10
	numEvents := 50

	// Start multiple subscribers
	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := broker.Subscribe(guildID)
			defer broker.Unsubscribe(guildID, ch)
			time.Sleep(10 * time.Millisecond)
		}()
	}

	// Publish events concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numEvents; i++ {
			broker.Publish(guildID, Event{ID: string(rune(i)), Type: "test"})
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
}
