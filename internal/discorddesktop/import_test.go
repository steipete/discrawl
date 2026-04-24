package discorddesktop

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
)

func TestImportExtractsDirectMessageFromDesktopCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Local Storage", "leveldb")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "000001.log"), []byte(`noise
{"id":"111111111111111111","type":1,"recipients":[{"id":"222222222222222222","username":"alice","global_name":"Alice"}]}
binary-ish {"t":"MESSAGE_CREATE","token":"do-not-store","d":{"id":"333333333333333333","channel_id":"111111111111111111","content":"launch checklist in a DM","timestamp":"2026-04-23T18:20:43.123Z","author":{"id":"222222222222222222","username":"alice","global_name":"Alice"},"attachments":[{"id":"444444444444444444","filename":"plan.txt","size":10}],"mentions":[{"id":"555555555555555555","username":"bob"}]}} tail
`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{
		Path: dir,
		Now:  func() time.Time { return time.Date(2026, 4, 23, 18, 30, 0, 0, time.UTC) },
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 1, stats.Messages)
	require.Equal(t, 1, stats.Channels)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, DirectMessageGuildID, results[0].GuildID)
	require.Equal(t, "Alice", results[0].ChannelName)
	require.Equal(t, "Alice", results[0].AuthorName)

	mentions, err := st.ListMentions(ctx, store.MentionListOptions{
		GuildIDs: []string{DirectMessageGuildID},
		Target:   "bob",
		Limit:    10,
	})
	require.NoError(t, err)
	require.Len(t, mentions, 1)
	require.Equal(t, "555555555555555555", mentions[0].TargetID)

	_, rows, err := st.ReadOnlyQuery(ctx, "select raw_json from messages where id = '333333333333333333'")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NotContains(t, rows[0][0], "do-not-store")
}

func TestImportDryRunDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.log"), []byte(`{"id":"333333333333333333","channel_id":"111111111111111111","content":"dry run only","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222222","username":"alice"}}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir, DryRun: true})
	require.NoError(t, err)
	require.True(t, stats.DryRun)
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 1, stats.SkippedMessages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "dry", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestImportExtractsCompressedUnknownMessageArrayFromChromiumCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err := zw.Write([]byte(`[{"id":"333333333333333334","channel_id":"111111111111111112","content":"compressed cache history","timestamp":"2026-04-23T18:20:43.123Z","author":{"id":"222222222222222223","username":"alice"}}]`))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	cacheBlob := append([]byte("https://discord.com/api/v9/channels/111111111111111112/messages?limit=50\x00"), compressed.Bytes()...)
	cacheBlob = append(cacheBlob, []byte("chromium trailing metadata")...)
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), cacheBlob, 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesScanned)
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 0, stats.DMMessages)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, 1, stats.SkippedChannels)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "compressed", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestImportReconcilesMessagesWithLaterGuildChannelMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Local Storage", "leveldb")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "000001.log"), []byte(`{"id":"333333333333333335","channel_id":"111111111111111113","content":"guild cache message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222224","username":"alice"},"mentions":[{"id":"555555555555555556","username":"bob"}],"attachments":[{"id":"444444444444444445","filename":"trace.txt"}]}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "000002.log"), []byte(`{"id":"111111111111111113","guild_id":"999999999999999999","type":0,"name":"backend"}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Messages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "guild cache", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "999999999999999999", results[0].GuildID)
	require.Equal(t, "backend", results[0].ChannelName)

	mentions, err := st.ListMentions(ctx, store.MentionListOptions{
		GuildIDs: []string{"999999999999999999"},
		Target:   "bob",
		Limit:    10,
	})
	require.NoError(t, err)
	require.Len(t, mentions, 1)

	_, rows, err := st.ReadOnlyQuery(ctx, "select guild_id from message_attachments where message_id = '333333333333333335'")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "999999999999999999", rows[0][0])
}

func TestImportClassifiesMessagesFromCachedChannelRoutes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "dm_0"), []byte(`https://discord.com/channels/@me/111111111111111114
{"id":"333333333333333336","channel_id":"111111111111111114","content":"route dm message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222225","username":"alice"}}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "guild_0"), []byte(`https://discord.com/channels/999999999999999998/111111111111111115
{"id":"333333333333333337","channel_id":"111111111111111115","content":"route guild message","timestamp":"2026-04-23T18:20:44Z","author":{"id":"222222222222222226","username":"bob"}}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, stats.DMMessages)
	require.Equal(t, 1, stats.GuildMessages)
	require.Equal(t, 0, stats.SkippedMessages)

	dmResults, err := st.SearchMessages(ctx, store.SearchOptions{Query: "route dm", Limit: 10})
	require.NoError(t, err)
	require.Len(t, dmResults, 1)
	require.Equal(t, DirectMessageGuildID, dmResults[0].GuildID)

	guildResults, err := st.SearchMessages(ctx, store.SearchOptions{Query: "route guild", Limit: 10})
	require.NoError(t, err)
	require.Len(t, guildResults, 1)
	require.Equal(t, "999999999999999998", guildResults[0].GuildID)

	guildChannels, err := st.Channels(ctx, "999999999999999998")
	require.NoError(t, err)
	require.Len(t, guildChannels, 1)
	require.Equal(t, "111111111111111115", guildChannels[0].ID)
	require.Equal(t, "channel-111115", guildChannels[0].Name)

	_, guildRows, err := st.ReadOnlyQuery(ctx, "select name from guilds where id = '999999999999999998'")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"Discord Desktop Guild 999999999999999998"}}, guildRows)
}

func TestImportDropsPreviousUnknownWiretapRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	require.NoError(t, st.UpsertGuild(ctx, store.GuildRecord{ID: "@unknown", Name: "Unknown", RawJSON: `{}`}))
	require.NoError(t, st.UpsertChannel(ctx, store.ChannelRecord{ID: "111111111111111116", GuildID: "@unknown", Kind: "unknown", Name: "unknown", RawJSON: `{}`}))
	require.NoError(t, st.UpsertMessage(ctx, store.MessageRecord{
		ID:                "333333333333333338",
		GuildID:           "@unknown",
		ChannelID:         "111111111111111116",
		AuthorID:          "222222222222222227",
		AuthorName:        "alice",
		MessageType:       0,
		CreatedAt:         "2026-04-23T18:20:43Z",
		Content:           "stale unknown message",
		NormalizedContent: "stale unknown message",
		RawJSON:           `{}`,
	}))

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "stale unknown", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestImportSkipsAmbiguousCachedChannelRoutes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "Cache", "Cache_Data")
	require.NoError(t, os.MkdirAll(cachePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cachePath, "entry_0"), []byte(`https://discord.com/channels/999999999999999998/111111111111111118
https://discord.com/channels/999999999999999997/111111111111111118
{"id":"333333333333333340","channel_id":"111111111111111118","content":"ambiguous route message","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222229","username":"alice"}}`), 0o600))

	dbPath := filepath.Join(dir, "discrawl.db")
	st, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()

	stats, err := Import(ctx, st, Options{Path: dir})
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 1, stats.SkippedMessages)
	require.Equal(t, 1, stats.SkippedChannels)

	results, err := st.SearchMessages(ctx, store.SearchOptions{Query: "ambiguous", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)
}
