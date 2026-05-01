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

func TestParseLookback(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{"7d", 7 * 24 * time.Hour, false},
		{"30d", 30 * 24 * time.Hour, false},
		{"72h", 72 * time.Hour, false},
		{"30m", 30 * time.Minute, false},
		{"", 0, true},
		{"abc", 0, true},
		{"-2d", 0, true},
		{"-1h", 0, true},
	}
	for _, tc := range cases {
		d, err := parseLookback(tc.in)
		if tc.err {
			require.Error(t, err, tc.in)
			continue
		}
		require.NoError(t, err, tc.in)
		require.Equal(t, tc.want, d, tc.in)
	}
}

func TestDigestCommand(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	require.NoError(t, seedDigestCLIStore(ctx, dbPath))

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	t.Run("since 7d happy path", func(t *testing.T) {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, []string{"--config", cfgPath, "digest", "--since", "7d"}, &out, &bytes.Buffer{}))
		require.Contains(t, out.String(), "general (text)")
		require.Contains(t, out.String(), "Window:")
		require.Contains(t, out.String(), "Totals: messages=")
	})

	t.Run("json output", func(t *testing.T) {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "digest", "--since", "7d"}, &out, &bytes.Buffer{}))
		var payload map[string]any
		require.NoError(t, json.Unmarshal(out.Bytes(), &payload))
		require.Equal(t, "7d", payload["window_label"])
		require.InEpsilon(t, 3, payload["top_n"], 0.001)
		totals, ok := payload["totals"].(map[string]any)
		require.True(t, ok)
		require.InEpsilon(t, 2, totals["messages"], 0.001)
	})

	t.Run("channel name filter", func(t *testing.T) {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, []string{"--config", cfgPath, "--json", "digest", "--channel", "incidents", "--since", "7d"}, &out, &bytes.Buffer{}))
		var payload map[string]any
		require.NoError(t, json.Unmarshal(out.Bytes(), &payload))
		channels, ok := payload["channels"].([]any)
		require.True(t, ok)
		require.Len(t, channels, 1)
		channel := channels[0].(map[string]any)
		require.Equal(t, "incidents", channel["channel_name"])
	})

	t.Run("unknown flag fails", func(t *testing.T) {
		err := Run(ctx, []string{"--config", cfgPath, "digest", "--bogus"}, &bytes.Buffer{}, &bytes.Buffer{})
		require.Error(t, err)
		require.Equal(t, 2, ExitCode(err))
	})

	t.Run("no positional args allowed", func(t *testing.T) {
		err := Run(ctx, []string{"--config", cfgPath, "digest", "extra"}, &bytes.Buffer{}, &bytes.Buffer{})
		require.Error(t, err)
		require.Equal(t, 2, ExitCode(err))
	})
}

func seedDigestCLIStore(ctx context.Context, path string) error {
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
	if err := s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "alice", DisplayName: "Alice", RoleIDsJSON: `[]`, RawJSON: `{}`}); err != nil {
		return err
	}
	if err := s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u2", Username: "bob", DisplayName: "Bob", RoleIDsJSON: `[]`, RawJSON: `{}`}); err != nil {
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
				MessageType:       0,
				CreatedAt:         now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
				Content:           "hello",
				NormalizedContent: "hello",
				RawJSON:           `{}`,
			},
			Mentions: []store.MentionEventRecord{{
				MessageID:  "m1",
				GuildID:    "g1",
				ChannelID:  "c1",
				AuthorID:   "u1",
				TargetType: "user",
				TargetID:   "u2",
				TargetName: "Bob",
				EventAt:    now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
			}},
		},
		{
			Record: store.MessageRecord{
				ID:                "m2",
				GuildID:           "g1",
				ChannelID:         "c2",
				ChannelName:       "incidents",
				AuthorID:          "u2",
				AuthorName:        "Bob",
				MessageType:       0,
				CreatedAt:         now.Add(-90 * time.Minute).Format(time.RFC3339Nano),
				Content:           "incident",
				NormalizedContent: "incident",
				RawJSON:           `{}`,
			},
		},
	})
}
