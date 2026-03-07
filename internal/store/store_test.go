package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreReadWriteAndSearch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "t1", GuildID: "g1", Kind: "thread_public", Name: "thread", RawJSON: `{}`}))
	require.NoError(t, s.ReplaceMembers(ctx, "g1", []MemberRecord{{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `["r1"]`,
		RawJSON:     `{}`,
	}}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic: database is locked",
		NormalizedContent: "panic database is locked",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "t1",
		ChannelName:       "thread",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
		Content:           "rate limit discussion",
		NormalizedContent: "rate limit discussion",
		RawJSON:           `{}`,
	}))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "panic", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "panic", Channel: "general", Author: "Peter", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "panic:error", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, results)

	members, err := s.Members(ctx, "g1", "pet", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)

	channels, err := s.Channels(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, channels, 2)

	status, err := s.Status(ctx, dbPath, "g1")
	require.NoError(t, err)
	require.Equal(t, 1, status.GuildCount)
	require.Equal(t, 2, status.ChannelCount)
	require.Equal(t, 1, status.ThreadCount)
	require.Equal(t, 2, status.MessageCount)
	require.Equal(t, 1, status.MemberCount)
	require.Equal(t, "Guild", status.DefaultGuildName)
}

func TestReadOnlyQueryGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	cols, rows, err := s.ReadOnlyQuery(ctx, "select 1 as one")
	require.NoError(t, err)
	require.Equal(t, []string{"one"}, cols)
	require.Equal(t, [][]string{{"1"}}, rows)

	_, _, err = s.ReadOnlyQuery(ctx, "delete from messages")
	require.Error(t, err)
}

func TestUpsertAndDeleteMember(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))
	rows, err := s.MemberByID(ctx, "u1")
	require.NoError(t, err)
	require.Len(t, rows, 1)

	require.NoError(t, s.DeleteMember(ctx, "g1", "u1"))
	rows, err = s.MemberByID(ctx, "u1")
	require.NoError(t, err)
	require.Empty(t, rows)

	require.NoError(t, s.ReplaceMembers(ctx, "g1", []MemberRecord{{
		GuildID:     "g1",
		UserID:      "u2",
		Username:    "other",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}}))
	rows, err = s.Members(ctx, "g1", "", 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "u2", rows[0].UserID)
}

func TestEventsSyncStateAndHelpers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NotNil(t, s.DB())
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.AppendMessageEvent(ctx, "g1", "c1", "m1", "create", map[string]string{"ok": "1"}))
	require.NoError(t, s.MarkMessageDeleted(ctx, "g1", "c1", "m1", map[string]string{"deleted": "1"}))
	require.NoError(t, s.SetSyncState(ctx, "scope:test", "cursor-1"))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		EditedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello again",
		NormalizedContent: "hello again",
		RawJSON:           `{}`,
	}))

	cursor, err := s.GetSyncState(ctx, "scope:test")
	require.NoError(t, err)
	require.Equal(t, "cursor-1", cursor)

	_, rows, err := s.ReadOnlyQuery(ctx, "select deleted_at from messages where id = 'm1'")
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	cols, rows, err := s.ReadOnlyQuery(ctx, "pragma foreign_keys")
	require.NoError(t, err)
	require.NotEmpty(t, cols)
	require.NotEmpty(t, rows)

	require.Equal(t, "1", stringify(int64(1)))
	require.Equal(t, "value", stringify("value"))
	require.Empty(t, stringify(nil))
	require.Equal(t, "abc", stringify([]byte("abc")))
	require.True(t, parseTime(time.Now().UTC().Format(time.RFC3339Nano)).After(time.Time{}))
	require.Equal(t, "?, ?, ?", placeholders(3))

	results, err := s.searchFallback(ctx, SearchOptions{Query: "hello", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)

	empty, err := s.GetSyncState(ctx, "missing")
	require.NoError(t, err)
	require.Empty(t, empty)
}

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
		},
	}))

	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])

	_, rows, err = s.ReadOnlyQuery(ctx, "select count(*) from message_events")
	require.NoError(t, err)
	require.Equal(t, "2", rows[0][0])
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
