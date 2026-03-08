package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUpsertMessagesBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{
		{
			Record: MessageRecord{
				ID:                "m1",
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         now,
				Content:           "one",
				NormalizedContent: "one",
				RawJSON:           `{"id":"m1"}`,
			},
			EventType:   "upsert",
			PayloadJSON: `{"id":"m1"}`,
			Options: WriteOptions{
				AppendEvent: true,
			},
		},
		{
			Record: MessageRecord{
				ID:                "m2",
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         now,
				Content:           "two",
				NormalizedContent: "two",
				RawJSON:           `{"id":"m2"}`,
			},
			EventType:   "upsert",
			PayloadJSON: `{"id":"m2"}`,
			Options: WriteOptions{
				AppendEvent: true,
			},
		},
	}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from message_events")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
}

func TestUpsertMessagesSkipsEventsAndEmbeddingsByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, s.UpsertMessages(ctx, []MessageMutation{{
		Record: MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         now,
			Content:           "one",
			NormalizedContent: "one",
			RawJSON:           `{"id":"m1"}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m1"}`,
	}}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from message_events")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from embedding_jobs")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestUpsertMessageWithEmbeddingsQueuesJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMessageWithOptions(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}, WriteOptions{EnqueueEmbedding: true}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from embedding_jobs")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])
}

func TestConcurrentMessageUpsertsShareSingleWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errCh <- s.UpsertMessage(ctx, MessageRecord{
				ID:                stringify(i),
				GuildID:           "g1",
				ChannelID:         "c1",
				MessageType:       0,
				CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
				Content:           "hello",
				NormalizedContent: "hello",
				RawJSON:           `{}`,
			})
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "8", rows[0][0])
}

func TestMessageFTSUsesSnowflakeRowID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	record := MessageRecord{
		ID:                "1469950701764350208",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "first body",
		NormalizedContent: "first body",
		RawJSON:           `{"author":{"username":"Peter"}}`,
	}
	require.NoError(t, s.UpsertMessage(ctx, record))

	record.Content = "second body"
	record.NormalizedContent = "second body"
	require.NoError(t, s.UpsertMessage(ctx, record))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*), min(rowid), max(rowid), min(content) from message_fts")
	require.NoError(t, err)
	require.Equal(t, []string{"1", "1469950701764350208", "1469950701764350208", "second body"}, rows[0])
}

func TestMemberFTSUpdatesOnUpsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	record := MemberRecord{
		GuildID:       "g1",
		UserID:        "u1",
		Username:      "peter",
		DisplayName:   "Peter",
		RoleIDsJSON:   `[]`,
		RawJSON:       `{"bio":"Maintainer","github":"steipete"}`,
		Discriminator: "0",
	}
	require.NoError(t, s.UpsertMember(ctx, record))

	record.RawJSON = `{"bio":"Updated bio","github":"steipete","website":"https://steipete.me"}`
	require.NoError(t, s.UpsertMember(ctx, record))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*), min(profile_text) from member_fts")
	require.NoError(t, err)
	require.Equal(t, []string{"1", "Updated bio steipete https://steipete.me"}, rows[0])
}

func TestOpenRebuildsLegacyFTSRowIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)

	messageID := "1469950701764350208"
	channelID := "c1"
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: channelID, GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                messageID,
		GuildID:           "g1",
		ChannelID:         channelID,
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic database is locked",
		NormalizedContent: "panic database is locked",
		RawJSON:           `{"author":{"username":"Peter"}}`,
	}))
	require.NoError(t, s.Close())

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `delete from message_fts`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `
		insert into message_fts(message_id, guild_id, channel_id, author_id, author_name, channel_name, content)
		values(?, ?, ?, ?, ?, ?, ?)
	`, messageID, "g1", channelID, "u1", "Peter", "general", "panic database is locked")
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `delete from sync_state where scope = 'schema:message_fts_rowid_version'`)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	s, err = Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	_, rows, err := s.ReadOnlyQuery(ctx, "select rowid, message_id from message_fts")
	require.NoError(t, err)
	require.Equal(t, []string{messageID, messageID}, rows[0])

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "panic", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, messageID, results[0].MessageID)
}
