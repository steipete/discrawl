package report

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestBuildTrends(t *testing.T) {
	ctx := context.Background()
	s, now := seedTrendsStore(t, ctx)
	defer func() { _ = s.Close() }()

	t.Run("zero fill returns exact Monday weeks for every message channel", func(t *testing.T) {
		trends, err := BuildTrends(ctx, s, TrendsOptions{Now: now, Weeks: 3, GuildID: "g1"})
		require.NoError(t, err)
		require.Equal(t, 3, trends.Weeks)
		require.Equal(t, time.Date(2026, 4, 6, 0, 0, 0, 0, time.UTC), trends.Since)
		require.Len(t, trends.Rows, 4)

		for _, row := range trends.Rows {
			require.Len(t, row.Weekly, 3)
			require.Equal(t, time.Monday, row.Weekly[0].WeekStart.Weekday())
		}

		alpha := trends.Rows[0]
		require.Equal(t, "alpha", alpha.ChannelName)
		require.Equal(t, 2, alpha.Weekly[0].Messages)
		require.Equal(t, 3, alpha.Weekly[1].Messages)
		require.Equal(t, 1, alpha.Weekly[2].Messages)

		beta := trends.Rows[1]
		require.Equal(t, "beta", beta.ChannelName)
		require.Equal(t, 0, beta.Weekly[0].Messages)
		require.Equal(t, 2, beta.Weekly[1].Messages)
		require.Equal(t, 2, beta.Weekly[2].Messages)

		gamma := trends.Rows[2]
		require.Equal(t, "gamma", gamma.ChannelName)
		require.Equal(t, 0, gamma.Weekly[0].Messages)
		require.Equal(t, 0, gamma.Weekly[1].Messages)
		require.Equal(t, 1, gamma.Weekly[2].Messages)

		zeta := trends.Rows[3]
		require.Equal(t, "zeta", zeta.ChannelName)
		require.Equal(t, 0, zeta.Weekly[0].Messages)
		require.Equal(t, 0, zeta.Weekly[1].Messages)
		require.Equal(t, 0, zeta.Weekly[2].Messages)
	})

	t.Run("guild filter", func(t *testing.T) {
		trends, err := BuildTrends(ctx, s, TrendsOptions{Now: now, Weeks: 3, GuildID: "g2"})
		require.NoError(t, err)
		require.Len(t, trends.Rows, 1)
		require.Equal(t, "omega", trends.Rows[0].ChannelName)
	})

	t.Run("channel filter by id", func(t *testing.T) {
		trends, err := BuildTrends(ctx, s, TrendsOptions{Now: now, Weeks: 3, GuildID: "g1", Channel: "c2"})
		require.NoError(t, err)
		require.Len(t, trends.Rows, 1)
		require.Equal(t, "beta", trends.Rows[0].ChannelName)
		require.Len(t, trends.Rows[0].Weekly, 3)
	})

	t.Run("channel filter by name", func(t *testing.T) {
		trends, err := BuildTrends(ctx, s, TrendsOptions{Now: now, Weeks: 3, GuildID: "g1", Channel: "gamma"})
		require.NoError(t, err)
		require.Len(t, trends.Rows, 1)
		require.Equal(t, "c3", trends.Rows[0].ChannelID)
		require.Equal(t, 1, trends.Rows[0].Weekly[2].Messages)
	})

	t.Run("weeks default", func(t *testing.T) {
		trends, err := BuildTrends(ctx, s, TrendsOptions{Now: now, Weeks: 0, GuildID: "g1", Channel: "c1"})
		require.NoError(t, err)
		require.Equal(t, 8, trends.Weeks)
		require.Len(t, trends.Rows, 1)
		require.Len(t, trends.Rows[0].Weekly, 8)
	})
}

func seedTrendsStore(t *testing.T, ctx context.Context) (*store.Store, time.Time) {
	t.Helper()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	week0 := time.Date(2026, 4, 6, 0, 0, 0, 0, time.UTC)

	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild 1", RawJSON: `{}`}))
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g2", Name: "Guild 2", RawJSON: `{}`}))

	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "alpha", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "beta", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c3", GuildID: "g1", Kind: "thread_public", Name: "gamma", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c4", GuildID: "g1", Kind: "text", Name: "zeta", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c5", GuildID: "g1", Kind: "category", Name: "structural", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c6", GuildID: "g1", Kind: "voice", Name: "lobby", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c9", GuildID: "g2", Kind: "text", Name: "omega", RawJSON: `{}`}))

	seed := func(guildID, channelID, channelName string, weekStart time.Time, count int) {
		mutations := make([]store.MessageMutation, 0, count)
		for i := range count {
			createdAt := weekStart.Add(time.Duration(2+i) * time.Hour)
			mutations = append(mutations, store.MessageMutation{Record: store.MessageRecord{
				ID:                channelID + "-" + createdAt.Format("20060102150405"),
				GuildID:           guildID,
				ChannelID:         channelID,
				ChannelName:       channelName,
				AuthorID:          "u1",
				AuthorName:        "u1",
				CreatedAt:         createdAt.Format(time.RFC3339Nano),
				Content:           "message",
				NormalizedContent: "message",
				RawJSON:           `{}`,
			}})
		}
		require.NoError(t, s.UpsertMessages(ctx, mutations))
	}

	seed("g1", "c1", "alpha", week0, 2)
	seed("g1", "c1", "alpha", week0.AddDate(0, 0, 7), 3)
	seed("g1", "c1", "alpha", week0.AddDate(0, 0, 14), 1)

	seed("g1", "c2", "beta", week0.AddDate(0, 0, 7), 2)
	seed("g1", "c2", "beta", week0.AddDate(0, 0, 14), 2)

	seed("g1", "c3", "gamma", week0.AddDate(0, 0, 14), 1)
	seed("g2", "c9", "omega", week0.AddDate(0, 0, 7), 2)

	return s, now
}
