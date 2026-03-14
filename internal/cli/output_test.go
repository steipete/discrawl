package cli

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
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
