package report

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestBuildDigest(t *testing.T) {
	ctx := context.Background()
	s, now := seedDigestStore(t, ctx)
	defer func() { _ = s.Close() }()

	t.Run("happy path with defaults", func(t *testing.T) {
		digest, err := BuildDigest(ctx, s, DigestOptions{Now: now})
		require.NoError(t, err)

		require.Equal(t, now, digest.GeneratedAt)
		require.Equal(t, now.Add(-7*24*time.Hour), digest.Since)
		require.Equal(t, now, digest.Until)
		require.Equal(t, "7d", digest.WindowLabel)
		require.Equal(t, 3, digest.TopN)
		require.Len(t, digest.Channels, 3)

		require.Equal(t, "c1", digest.Channels[0].ChannelID)
		require.Equal(t, "general", digest.Channels[0].ChannelName)
		require.Equal(t, 4, digest.Channels[0].Messages)
		require.Equal(t, 2, digest.Channels[0].Replies)
		require.Equal(t, 3, digest.Channels[0].ActiveAuthors)

		require.Equal(t, "Alice", digest.Channels[0].TopPosters[0].Name)
		require.Equal(t, 2, digest.Channels[0].TopPosters[0].Count)
		require.Equal(t, "Bob", digest.Channels[0].TopMentions[0].Name)
		require.Equal(t, 2, digest.Channels[0].TopMentions[0].Count)
		require.Equal(t, "Oncall", digest.Channels[0].TopMentions[1].Name)

		require.Equal(t, 6, digest.Totals.Messages)
		require.Equal(t, 2, digest.Totals.Replies)
		require.Equal(t, 3, digest.Totals.Channels)
		require.Equal(t, 4, digest.Totals.ActiveAuthors)
	})

	t.Run("window filter", func(t *testing.T) {
		digest, err := BuildDigest(ctx, s, DigestOptions{Now: now, Since: 24 * time.Hour})
		require.NoError(t, err)
		require.Equal(t, "1d", digest.WindowLabel)
		require.Equal(t, 5, digest.Totals.Messages)
		require.Equal(t, 3, digest.Channels[0].Messages)
	})

	t.Run("channel filter by id", func(t *testing.T) {
		digest, err := BuildDigest(ctx, s, DigestOptions{Now: now, Channel: "c2", TopN: 5})
		require.NoError(t, err)
		require.Len(t, digest.Channels, 1)
		require.Equal(t, "incidents", digest.Channels[0].ChannelName)
		require.Equal(t, 1, digest.Totals.Messages)
		require.Equal(t, 5, digest.TopN)
	})

	t.Run("channel filter by name", func(t *testing.T) {
		digest, err := BuildDigest(ctx, s, DigestOptions{Now: now, Channel: "general"})
		require.NoError(t, err)
		require.Len(t, digest.Channels, 1)
		require.Equal(t, "c1", digest.Channels[0].ChannelID)
		require.Equal(t, 4, digest.Totals.Messages)
	})

	t.Run("guild filter", func(t *testing.T) {
		digest, err := BuildDigest(ctx, s, DigestOptions{Now: now, GuildID: "g1"})
		require.NoError(t, err)
		require.Len(t, digest.Channels, 2)
		require.Equal(t, 5, digest.Totals.Messages)
		require.Equal(t, 3, digest.Totals.ActiveAuthors)
	})

	t.Run("negative since is normalized", func(t *testing.T) {
		digest, err := BuildDigest(ctx, s, DigestOptions{Now: now, Since: -24 * time.Hour})
		require.NoError(t, err)
		require.Equal(t, "1d", digest.WindowLabel)
		require.Equal(t, now.Add(-24*time.Hour), digest.Since)
	})
}

func seedDigestStore(t *testing.T, ctx context.Context) (*store.Store, time.Time) {
	t.Helper()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)

	now := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild 1", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g2", Name: "Guild 2", RawJSON: `{}`}))

	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "incidents", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c3", GuildID: "g1", Kind: "forum", Name: "unused", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c4", GuildID: "g2", Kind: "text", Name: "alpha", RawJSON: `{}`}))

	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "alice", DisplayName: "Alice", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u2", Username: "bob", DisplayName: "Bob", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u3", Username: "carol", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g2", UserID: "u9", Username: "dana", DisplayName: "Dana", RoleIDsJSON: `[]`, RawJSON: `{}`}))

	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{
		{
			Record:   store.MessageRecord{ID: "m1", GuildID: "g1", ChannelID: "c1", ChannelName: "general", AuthorID: "u1", AuthorName: "Alice", CreatedAt: now.Add(-2 * time.Hour).Format(time.RFC3339Nano), Content: "hello", NormalizedContent: "hello", RawJSON: `{}`},
			Mentions: []store.MentionEventRecord{{MessageID: "m1", GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "user", TargetID: "u2", TargetName: "Bob", EventAt: now.Add(-2 * time.Hour).Format(time.RFC3339Nano)}},
		},
		{
			Record:   store.MessageRecord{ID: "m2", GuildID: "g1", ChannelID: "c1", ChannelName: "general", AuthorID: "u2", AuthorName: "Bob", CreatedAt: now.Add(-90 * time.Minute).Format(time.RFC3339Nano), ReplyToMessageID: "m1", Content: "reply", NormalizedContent: "reply", RawJSON: `{}`},
			Mentions: []store.MentionEventRecord{{MessageID: "m2", GuildID: "g1", ChannelID: "c1", AuthorID: "u2", TargetType: "role", TargetID: "r1", TargetName: "Oncall", EventAt: now.Add(-90 * time.Minute).Format(time.RFC3339Nano)}},
		},
		{
			Record:   store.MessageRecord{ID: "m3", GuildID: "g1", ChannelID: "c1", ChannelName: "general", AuthorID: "u1", AuthorName: "Alice", CreatedAt: now.Add(-80 * time.Minute).Format(time.RFC3339Nano), ReplyToMessageID: "m1", Content: "another reply", NormalizedContent: "another reply", RawJSON: `{}`},
			Mentions: []store.MentionEventRecord{{MessageID: "m3", GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "user", TargetID: "u2", TargetName: "Bob", EventAt: now.Add(-80 * time.Minute).Format(time.RFC3339Nano)}},
		},
		{
			Record: store.MessageRecord{ID: "m4", GuildID: "g1", ChannelID: "c1", ChannelName: "general", AuthorID: "u3", AuthorName: "carol", CreatedAt: now.Add(-26 * time.Hour).Format(time.RFC3339Nano), Content: "older", NormalizedContent: "older", RawJSON: `{}`},
		},
		{
			Record: store.MessageRecord{ID: "m5", GuildID: "g1", ChannelID: "c2", ChannelName: "incidents", AuthorID: "u2", AuthorName: "Bob", CreatedAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano), Content: "incident", NormalizedContent: "incident", RawJSON: `{}`},
		},
		{
			Record: store.MessageRecord{ID: "m6", GuildID: "g1", ChannelID: "c2", ChannelName: "incidents", AuthorID: "u2", AuthorName: "Bob", CreatedAt: now.Add(-10 * 24 * time.Hour).Format(time.RFC3339Nano), Content: "stale", NormalizedContent: "stale", RawJSON: `{}`},
		},
		{
			Record: store.MessageRecord{ID: "m7", GuildID: "g2", ChannelID: "c4", ChannelName: "alpha", AuthorID: "u9", AuthorName: "Dana", CreatedAt: now.Add(-2 * time.Hour).Format(time.RFC3339Nano), Content: "other guild", NormalizedContent: "other guild", RawJSON: `{}`},
		},
	}))

	return s, now
}
