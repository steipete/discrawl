package syncer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
)

func TestBuildMessageMutationIncludesAttachmentTextAndMentions(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("stack trace line one\nstack trace line two"))
	}))
	defer server.Close()

	previousClient := attachmentHTTPClient
	attachmentHTTPClient = server.Client()
	t.Cleanup(func() {
		attachmentHTTPClient = previousClient
	})

	now := time.Now().UTC()
	mutation, err := buildMessageMutation(context.Background(), &discordgo.Message{
		ID:        "m1",
		GuildID:   "g1",
		ChannelID: "c1",
		Content:   "",
		Timestamp: now,
		Author:    &discordgo.User{ID: "u1", Username: "peter"},
		Attachments: []*discordgo.MessageAttachment{{
			ID:          "a1",
			Filename:    "trace.txt",
			ContentType: "text/plain",
			URL:         server.URL,
			Size:        64,
		}},
		Mentions: []*discordgo.User{{
			ID:         "u2",
			Username:   "shadow",
			GlobalName: "Shadow",
		}},
		MentionRoles: []string{"r1"},
	}, "maintainers", false)
	require.NoError(t, err)
	require.Len(t, mutation.Attachments, 1)
	require.Equal(t, "trace.txt", mutation.Attachments[0].Filename)
	require.Contains(t, mutation.Attachments[0].TextContent, "stack trace")
	require.Contains(t, mutation.Record.NormalizedContent, "trace.txt")
	require.Contains(t, mutation.Record.NormalizedContent, "stack trace line one")
	require.Len(t, mutation.Mentions, 2)
	require.Equal(t, "user", mutation.Mentions[0].TargetType)
	require.Equal(t, "u2", mutation.Mentions[0].TargetID)
	require.Equal(t, "Shadow", mutation.Mentions[0].TargetName)
	require.Equal(t, "role", mutation.Mentions[1].TargetType)
	require.Equal(t, "r1", mutation.Mentions[1].TargetID)
}

func TestShouldFetchAttachmentText(t *testing.T) {
	t.Parallel()

	require.True(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{Filename: "trace.txt"}))
	require.True(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{Filename: "payload.json"}))
	require.True(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{ContentType: "text/plain"}))
	require.False(t, shouldFetchAttachmentText(&discordgo.MessageAttachment{Filename: "photo.png", ContentType: "image/png"}))
}
