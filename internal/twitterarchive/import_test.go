package twitterarchive

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
)

func TestImportTwitterArchiveWritesSearchableMessages(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "twitter.zip")
	writeTestArchive(t, archivePath)

	st, err := store.Open(ctx, filepath.Join(dir, "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{
		Path: archivePath,
		Now:  func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) },
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Accounts)
	require.Equal(t, 1, stats.Tweets)
	require.Equal(t, 1, stats.Likes)
	require.Equal(t, 1, stats.DMConversations)
	require.Equal(t, 1, stats.DMMessages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "pull requests", GuildIDs: []string{GuildID}, Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "x:tweet:1952542067017584782", results[0].MessageID)
	require.Equal(t, "steipete", results[0].AuthorName)

	results, err = st.SearchMessages(ctx, store.SearchOptions{Query: "secret roadmap", GuildIDs: []string{GuildID}, Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "x:dm:1052590933307461636", results[0].MessageID)

	cursor, err := st.GetSyncState(ctx, archiveScope)
	require.NoError(t, err)
	require.Equal(t, archivePath, cursor)
}

func TestImportTwitterArchiveDryRunDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "twitter.zip")
	writeTestArchive(t, archivePath)

	stats, err := Import(ctx, nil, Options{Path: archivePath, DryRun: true})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Tweets)
	require.True(t, stats.DryRun)
}

func writeTestArchive(t *testing.T, path string) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	zw := zip.NewWriter(file)
	defer func() { require.NoError(t, zw.Close()) }()

	writeZipEntry(t, zw, "data/account.js", `window.YTD.account.part0 = [{
  "account": {
    "email": "steipete@gmail.com",
    "username": "steipete",
    "accountId": "25401953",
    "createdAt": "2009-03-19T22:54:05.000Z",
    "accountDisplayName": "Peter Steinberger"
  }
}]`)
	writeZipEntry(t, zw, "data/tweets.js", `window.YTD.tweets.part0 = [{
  "tweet": {
    "id_str": "1952542067017584782",
    "created_at": "Tue Aug 05 01:27:59 +0000 2025",
    "full_text": "Getting pull requests with zero user testing aint it.",
    "entities": {"user_mentions": [{"screen_name": "alice", "id_str": "42"}]}
  }
}]`)
	writeZipEntry(t, zw, "data/like.js", `window.YTD.like.part0 = [{
  "like": {
    "tweetId": "1952539858771275983",
    "fullText": "Liked archive import smoke"
  }
}]`)
	writeZipEntry(t, zw, "data/direct-messages.js", `window.YTD.direct_messages.part0 = [{
  "dmConversation": {
    "conversationId": "929-25401953",
    "messages": [{
      "messageCreate": {
        "recipientId": "929",
        "senderId": "25401953",
        "id": "1052590933307461636",
        "createdAt": "2018-10-17T16:03:29.391Z",
        "text": "secret roadmap",
        "mediaUrls": [],
        "urls": []
      }
    }]
  }
}]`)
}

func writeZipEntry(t *testing.T, zw *zip.Writer, name, body string) {
	t.Helper()
	w, err := zw.Create(name)
	require.NoError(t, err)
	_, err = w.Write([]byte(body))
	require.NoError(t, err)
}
