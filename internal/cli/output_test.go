package cli

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/syncer"
)

func TestPrintRows(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	require.NoError(t, printRows(&buf, []string{"name", "count"}, [][]string{{"general", "5"}}))
	require.Contains(t, buf.String(), "name")
	require.Contains(t, buf.String(), "general")
}

func TestPrintHumanMemberProfile(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	require.NoError(t, printHuman(&buf, store.MemberProfile{
		Member: store.MemberRow{
			GuildID:     "g1",
			UserID:      "u1",
			Username:    "peter",
			DisplayName: "Peter",
			GitHubLogin: "steipete",
			XHandle:     "steipete",
			Website:     "https://steipete.me",
			Pronouns:    "they/them",
			Location:    "Vienna",
			Bio:         "Builds tools",
			URLs:        []string{"https://steipete.me", "https://github.com/steipete"},
			JoinedAt:    time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC),
		},
		MessageCount: 2,
		RecentMessages: []store.MessageRow{{
			ChannelName: "general",
			CreatedAt:   time.Date(2026, 3, 8, 12, 1, 0, 0, time.UTC),
			Content:     "hello",
		}},
	}))
	require.Contains(t, buf.String(), "github=steipete")
	require.Contains(t, buf.String(), "x=steipete")
	require.Contains(t, buf.String(), "Recent messages:")
	require.Contains(t, buf.String(), "[general]")
}

func TestPlainAndHumanPrintersCoverStructuredRows(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	searchRows := []store.SearchResult{{GuildID: "g1", ChannelID: "c1", ChannelName: "general", AuthorID: "u1", AuthorName: "Peter", Content: "hello", CreatedAt: now}}
	memberRows := []store.MemberRow{{GuildID: "g1", UserID: "u1", Username: "peter", DisplayName: "Peter", XHandle: "steipete", GitHubLogin: "steipete", Website: "https://steipete.me", Bio: "builds useful things"}}
	channelRows := []store.ChannelRow{{GuildID: "g1", ID: "c1", Kind: "text", Name: "general"}}
	messageRows := []store.MessageRow{{GuildID: "g1", ChannelID: "c1", ChannelName: "general", AuthorID: "u1", AuthorName: "Peter", MessageID: "m1", Content: "message", CreatedAt: now}}
	mentionRows := []store.MentionRow{{GuildID: "g1", ChannelID: "c1", ChannelName: "general", AuthorID: "u1", AuthorName: "Peter", TargetType: "user", TargetID: "u2", Content: "mention", CreatedAt: now}}

	for _, value := range []any{searchRows, memberRows, store.MemberProfile{Member: memberRows[0]}, channelRows, messageRows, mentionRows} {
		var buf bytes.Buffer
		require.NoError(t, printPlain(&buf, value))
		require.NotEmpty(t, buf.String())
	}
	var buf bytes.Buffer
	require.Error(t, printPlain(&buf, map[string]any{"x": 1}))

	humanValues := []any{
		syncer.SyncStats{Guilds: 1, Channels: 2, Threads: 3, Members: 4, Messages: 5},
		store.Status{DBPath: "db", GuildCount: 1, ChannelCount: 2, ThreadCount: 3, MessageCount: 4, MemberCount: 5, EmbeddingBacklog: 6, LastSyncAt: now, LastTailEventAt: now},
		store.EmbeddingDrainStats{Processed: 1, Succeeded: 1, Failed: 1, Skipped: 1, RemainingBacklog: 2, Provider: "ollama", Model: "model", InputVersion: "v1", Requeued: 3, RateLimited: true},
		searchRows,
		messageRows,
		mentionRows,
		memberRows,
		channelRows,
		map[string]any{"b": 2, "a": 1},
	}
	for _, value := range humanValues {
		buf.Reset()
		require.NoError(t, printHuman(&buf, value))
		require.NotEmpty(t, buf.String())
	}
	require.Error(t, printHuman(&buf, struct{}{}))

	require.Equal(t, "short", trimForTable(" short "))
	require.Contains(t, trimForTable("01234567890123456789012345678901234567890123456789"), "...")
	require.Contains(t, memberProfileSummary(memberRows[0]), "x:steipete")
}

func TestRuntimePrintModes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	rt := &runtime{stdout: &buf, json: true}
	require.NoError(t, rt.print(map[string]any{"ok": true}))
	require.Contains(t, buf.String(), `"ok": true`)

	buf.Reset()
	rt = &runtime{stdout: &buf, plain: true}
	require.NoError(t, rt.print([]store.SearchResult{{GuildID: "g1", ChannelID: "c1", AuthorID: "u1", Content: "plain"}}))
	require.Contains(t, buf.String(), "plain")

	buf.Reset()
	require.NoError(t, rt.print(map[string]any{"fallback": true}))
	require.Contains(t, buf.String(), "fallback=true")
}
