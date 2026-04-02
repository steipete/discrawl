package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
)

func errMissingAccess() error {
	return fmt.Errorf("HTTP 403 Forbidden, {\"message\": \"Missing Access\", \"code\": 50001}")
}

func TestSyncChannelSubsetExpandsRequestedForumThreads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "f1",
		GuildID: "g1",
		Kind:    "forum",
		Name:    "support",
		RawJSON: `{"id":"f1"}`,
	}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "f1", GuildID: "g1", Name: "support", Type: discordgo.ChannelTypeGuildForum},
			},
		},
		activeThreads: map[string][]*discordgo.Channel{
			"f1": {{
				ID:            "t1",
				GuildID:       "g1",
				ParentID:      "f1",
				Name:          "bug-report",
				Type:          discordgo.ChannelTypeGuildPublicThread,
				LastMessageID: "10",
			}},
		},
		messages: map[string][]*discordgo.Message{
			"t1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "t1",
				Content:   "forum thread body",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}, ChannelIDs: []string{"f1"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Channels)
	require.Equal(t, 1, stats.Threads)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.guildChanCalls)
	require.NotZero(t, client.threadCalls)
	require.Equal(t, 1, client.messageCalls["t1"])

	results, err := s.SearchMessages(ctx, store.SearchOptions{Query: "forum thread"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "t1", results[0].ChannelID)
}

func TestSyncSkipsInaccessibleForumThreadCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "f1", GuildID: "g1", Name: "private-forum", Type: discordgo.ChannelTypeGuildForum},
			},
		},
		threadErrors: map[string]error{
			"f1": errMissingAccess(),
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "still syncs accessible channels",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, client.messageCalls["c1"])
	require.GreaterOrEqual(t, client.threadCalls, 1)

	cursor, err := s.GetSyncState(ctx, "channel:f1:unavailable")
	require.NoError(t, err)
	require.Equal(t, "missing_access", cursor)
}

func TestSyncChannelSubsetMergesStoredAndLiveTargets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"c1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "c1",
				Content:   "stored hit",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
			"c2": {{
				ID:        "20",
				GuildID:   "g1",
				ChannelID: "c2",
				Content:   "live miss",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}, ChannelIDs: []string{"c1", "c2"}})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, client.guildChanCalls)
	require.Equal(t, 1, client.messageCalls["c1"])
	require.Equal(t, 1, client.messageCalls["c2"])

	latest, err := s.GetSyncState(ctx, channelLatestScope("c2"))
	require.NoError(t, err)
	require.Equal(t, "20", latest)
}

func TestSyncSkipsUnchangedThreadsWhenHistoryComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "f1",
		Kind:     "thread_public",
		Name:     "bug-report",
		RawJSON:  `{"id":"t1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("t1"), "200"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("t1"), "1"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "f1", GuildID: "g1", Name: "support", Type: discordgo.ChannelTypeGuildForum},
			},
		},
		activeThreads: map[string][]*discordgo.Channel{
			"f1": {{
				ID:            "t1",
				GuildID:       "g1",
				ParentID:      "f1",
				Name:          "bug-report",
				Type:          discordgo.ChannelTypeGuildPublicThread,
				LastMessageID: "200",
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.Zero(t, client.messageCalls["t1"])
}

func TestSyncSkipsUnchangedTextChannelsWhenHistoryComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("c1"), "200"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {{
				ID:            "c1",
				GuildID:       "g1",
				Name:          "general",
				Type:          discordgo.ChannelTypeGuildText,
				LastMessageID: "200",
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Zero(t, stats.Messages)
	require.Zero(t, client.messageCalls["c1"])
}

func TestFullSyncReusesStoredThreadParents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "c1",
		Kind:     "thread_public",
		Name:     "bug-report",
		RawJSON:  `{"id":"t1"}`,
	}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
		messages: map[string][]*discordgo.Message{
			"t1": {{
				ID:        "10",
				GuildID:   "g1",
				ChannelID: "t1",
				Content:   "stored thread body",
				Timestamp: time.Now().UTC(),
				Author:    &discordgo.User{ID: "u1", Username: "user"},
			}},
		},
	}

	svc := New(client, s, nil)
	stats, err := svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Threads)
	require.Equal(t, 1, stats.Messages)
	require.Zero(t, client.threadCalls)
	require.Equal(t, 1, client.messageCalls["t1"])
}

func TestFullSyncUsesGuildActiveThreadsForStoredParents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:      "c1",
		GuildID: "g1",
		Kind:    "text",
		Name:    "general",
		RawJSON: `{"id":"c1"}`,
	}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{
		ID:       "t1",
		GuildID:  "g1",
		ParentID: "c1",
		Kind:     "thread_public",
		Name:     "bug-report",
		RawJSON:  `{"id":"t1"}`,
	}))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("c1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelHistoryCompleteScope("t1"), "1"))
	require.NoError(t, s.SetSyncState(ctx, channelLatestScope("t1"), "10"))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
			},
		},
		guildThreads: map[string][]*discordgo.Channel{
			"g1": {{
				ID:            "t1",
				GuildID:       "g1",
				ParentID:      "c1",
				Name:          "bug-report",
				Type:          discordgo.ChannelTypeGuildPublicThread,
				LastMessageID: "10",
			}},
		},
	}

	svc := New(client, s, nil)
	_, err = svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 1, client.guildThreadCalls)
	require.Zero(t, client.threadCalls)
}

func TestFullSyncFallsBackToBroadThreadDiscoveryWithoutStoredThreads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))

	client := &fakeClient{
		guilds: []*discordgo.UserGuild{{ID: "g1", Name: "Guild"}},
		guildByID: map[string]*discordgo.Guild{
			"g1": {ID: "g1", Name: "Guild"},
		},
		channels: map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", GuildID: "g1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c2", GuildID: "g1", Name: "random", Type: discordgo.ChannelTypeGuildText},
			},
		},
	}

	svc := New(client, s, nil)
	_, err = svc.Sync(ctx, SyncOptions{Full: true, GuildIDs: []string{"g1"}})
	require.NoError(t, err)
	require.Equal(t, 6, client.threadCalls)
}
