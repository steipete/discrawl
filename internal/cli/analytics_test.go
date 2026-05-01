package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/store"
)

func TestAnalyticsCommand(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	require.NoError(t, seedAnalyticsCLIStore(ctx, dbPath))

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	t.Run("analytics with no subcommand prints usage", func(t *testing.T) {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, []string{"--config", cfgPath, "analytics"}, &out, &bytes.Buffer{}))
		require.Contains(t, out.String(), "Usage: discrawl analytics <subcommand> [flags]")
		require.Contains(t, out.String(), "quiet")
		require.NotContains(t, out.String(), "trends")
	})

	t.Run("analytics quiet json schema", func(t *testing.T) {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "analytics", "quiet", "--since", "30d"}, &out, &bytes.Buffer{}))

		var payload map[string]any
		require.NoError(t, json.Unmarshal(out.Bytes(), &payload))
		require.Contains(t, payload, "generated_at")
		require.Contains(t, payload, "since")
		require.Contains(t, payload, "until")
		require.Contains(t, payload, "channels")

		channels, ok := payload["channels"].([]any)
		require.True(t, ok)
		require.NotEmpty(t, channels)

		first, ok := channels[0].(map[string]any)
		require.True(t, ok)
		require.Contains(t, first, "channel_id")
		require.Contains(t, first, "channel_name")
		require.Contains(t, first, "guild_id")
		require.Contains(t, first, "days_silent")

		totals, ok := payload["totals"].(map[string]any)
		require.True(t, ok)
		require.Contains(t, totals, "channels")
	})

	t.Run("analytics quiet human output", func(t *testing.T) {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, []string{"--config", cfgPath, "analytics", "quiet", "--since", "30d"}, &out, &bytes.Buffer{}))

		text := out.String()
		require.Contains(t, text, "CHANNEL")
		require.Contains(t, text, "stale")
		require.Contains(t, text, "Window:")
		require.Contains(t, text, "Totals: channels=")
	})

	t.Run("analytics quiet plain output", func(t *testing.T) {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--plain", "analytics", "quiet", "--since", "30d"}, &out, &bytes.Buffer{}))

		require.Contains(t, out.String(), "c3\tstale\ttext\tg1\t")
	})

	t.Run("unknown analytics subcommand returns usage error", func(t *testing.T) {
		err := Run(ctx, []string{"--config", cfgPath, "analytics", "unknown-sub"}, &bytes.Buffer{}, &bytes.Buffer{})
		require.Error(t, err)
		require.Equal(t, 2, ExitCode(err))
	})

	t.Run("quiet validates its own flags", func(t *testing.T) {
		cases := [][]string{
			{"--config", cfgPath, "analytics", "quiet", "--bogus"},
			{"--config", cfgPath, "analytics", "quiet", "extra"},
		}
		for _, args := range cases {
			err := Run(ctx, args, &bytes.Buffer{}, &bytes.Buffer{})
			require.Error(t, err)
			require.Equal(t, 2, ExitCode(err))
		}
	})
}

func seedAnalyticsCLIStore(ctx context.Context, path string) error {
	s, err := store.Open(ctx, path)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	now := time.Now().UTC()
	if err := s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, store.ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "incidents", RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, store.ChannelRecord{ID: "c3", GuildID: "g1", Kind: "text", Name: "stale", RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertChannel(ctx, store.ChannelRecord{ID: "c4", GuildID: "g1", Kind: "forum", Name: "never", RawJSON: `{}`}); err != nil {
		return err
	}
	return s.UpsertMessages(ctx, []store.MessageMutation{
		{
			Record: store.MessageRecord{
				ID:                "m1",
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Alice",
				CreatedAt:         now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
				Content:           "hello",
				NormalizedContent: "hello",
				RawJSON:           `{}`,
			},
		},
		{
			Record: store.MessageRecord{
				ID:                "m2",
				GuildID:           "g1",
				ChannelID:         "c2",
				ChannelName:       "incidents",
				AuthorID:          "u2",
				AuthorName:        "Bob",
				CreatedAt:         now.Add(-9 * 24 * time.Hour).Format(time.RFC3339Nano),
				Content:           "incident",
				NormalizedContent: "incident",
				RawJSON:           `{}`,
			},
		},
		{
			Record: store.MessageRecord{
				ID:                "m3",
				GuildID:           "g1",
				ChannelID:         "c3",
				ChannelName:       "stale",
				AuthorID:          "u1",
				AuthorName:        "Alice",
				CreatedAt:         now.Add(-45 * 24 * time.Hour).Format(time.RFC3339Nano),
				Content:           "old",
				NormalizedContent: "old",
				RawJSON:           `{}`,
			},
		},
	})
}
