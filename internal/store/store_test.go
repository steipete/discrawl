package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
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
		RawJSON:     `{"bio":"Maintainer at Example","website":"https://steipete.me","github":"steipete","twitter":"steipete"}`,
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
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m3",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339Nano),
		Content:           "",
		NormalizedContent: "",
		RawJSON:           `{"author":{"username":"Peter"}}`,
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

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "Peter", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 2)

	results, err = s.SearchMessages(ctx, SearchOptions{Query: "Peter", Limit: 10, IncludeEmpty: true})
	require.NoError(t, err)
	require.Len(t, results, 3)

	members, err := s.Members(ctx, "g1", "pet", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "steipete", members[0].GitHubLogin)
	require.Equal(t, "steipete", members[0].XHandle)
	require.Equal(t, "https://steipete.me", members[0].Website)

	members, err = s.Members(ctx, "g1", "Maintainer", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)

	profile, err := s.MemberProfile(ctx, "g1", "u1", 2)
	require.NoError(t, err)
	require.Equal(t, 3, profile.MessageCount)
	require.Len(t, profile.RecentMessages, 2)
	require.Equal(t, "steipete", profile.Member.GitHubLogin)

	channels, err := s.Channels(ctx, "g1")
	require.NoError(t, err)
	require.Len(t, channels, 2)

	status, err := s.Status(ctx, dbPath, "g1")
	require.NoError(t, err)
	require.Equal(t, 1, status.GuildCount)
	require.Equal(t, 2, status.ChannelCount)
	require.Equal(t, 1, status.ThreadCount)
	require.Equal(t, 3, status.MessageCount)
	require.Equal(t, 1, status.MemberCount)
	require.Equal(t, "Guild", status.DefaultGuildName)

	oldest, newest, err := s.ChannelMessageBounds(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "m1", oldest)
	require.Equal(t, "m3", newest)

	messageRows, err := s.ListMessages(ctx, MessageListOptions{
		Channel: "#general",
		Since:   parseTime("2000-01-01T00:00:00Z"),
	})
	require.NoError(t, err)
	require.Len(t, messageRows, 1)
	require.Equal(t, "m1", messageRows[0].MessageID)
	require.Equal(t, "Peter", messageRows[0].AuthorName)
}

func TestSearchMessagesPrefersRecentMessageIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))

	base := time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1458939673664684210",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "OpenClaw first hit",
		NormalizedContent: "openclaw first hit",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1489845247147118682",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "OpenClaw newest hit",
		NormalizedContent: "openclaw newest hit",
		RawJSON:           `{}`,
	}))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "OpenClaw", Limit: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "1489845247147118682", results[0].MessageID)
	require.Contains(t, results[0].Content, "newest")
}

func TestCheckMessageFTSProbe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.CheckMessageFTS(ctx))

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1458939673664684210",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "searchable text",
		NormalizedContent: "searchable text",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.CheckMessageFTS(ctx))
}

func TestRebuildSearchIndexesAndGuildCounts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertGuild(ctx, GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Searchable profile"}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "1458939673664684210",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "rebuildable launch text",
		NormalizedContent: "rebuildable launch text",
		RawJSON:           `{}`,
	}))

	_, err = s.DB().ExecContext(ctx, `delete from message_fts`)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `delete from member_fts`)
	require.NoError(t, err)
	require.NoError(t, s.RebuildSearchIndexes(ctx))

	results, err := s.SearchMessages(ctx, SearchOptions{Query: "rebuildable", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	members, err := s.Members(ctx, "g1", "Searchable", 10)
	require.NoError(t, err)
	require.Len(t, members, 1)
	channelCount, err := s.GuildChannelCount(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, 1, channelCount)
	memberCount, err := s.GuildMemberCount(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, 1, memberCount)
}

func TestOpenSetsSchemaVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)
}

func TestOpenFailsOnFutureSchemaVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `pragma user_version = 999`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = Open(ctx, dbPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "newer than supported")
}

func TestOpenBackfillsMissingSchemaVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `pragma user_version = 0`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err = Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	var version int
	require.NoError(t, s.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version))
	require.Equal(t, storeSchemaVersion, version)
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

func TestQueryAndExec(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	affected, err := s.Exec(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values('scope:test-exec', 'cursor-1', '2026-03-08T00:00:00Z')
	`)
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	cols, rows, err := s.Query(ctx, `select scope, cursor from sync_state where scope = 'scope:test-exec'`)
	require.NoError(t, err)
	require.Equal(t, []string{"scope", "cursor"}, cols)
	require.Equal(t, [][]string{{"scope:test-exec", "cursor-1"}}, rows)

	_, _, err = s.Query(ctx, "   ")
	require.Error(t, err)

	_, err = s.Exec(ctx, "   ")
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

func TestOpenTightensDBFilePerms(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("windows does not expose unix permission bits")
	}

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")
	require.NoError(t, os.WriteFile(dbPath, nil, 0o644))

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestOpenCreatesQueryIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	messageIndexes := indexNames(t, ctx, s.DB(), "messages")
	require.Contains(t, messageIndexes, "idx_messages_guild_created_id")
	require.Contains(t, messageIndexes, "idx_messages_channel_created_id")
	require.Contains(t, messageIndexes, "idx_messages_author_created_id")

	mentionIndexes := indexNames(t, ctx, s.DB(), "mention_events")
	require.Contains(t, mentionIndexes, "idx_mentions_guild_event")
	require.Contains(t, mentionIndexes, "idx_mentions_channel_event")
}

func TestOpenMigratesLegacyQueryIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "discrawl.db")

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	legacy := &Store{db: sqlDB, path: dbPath}
	require.NoError(t, legacy.applyBaselineSchema(ctx))
	require.NoError(t, legacy.setSchemaVersion(ctx, 1))
	for _, indexName := range []string{
		"idx_messages_guild_created_id",
		"idx_messages_channel_created_id",
		"idx_messages_author_created_id",
		"idx_mentions_guild_event",
		"idx_mentions_channel_event",
	} {
		_, err = sqlDB.ExecContext(ctx, `drop index if exists `+indexName)
		require.NoError(t, err)
	}
	require.NoError(t, sqlDB.Close())

	s, err := Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	version, err := s.schemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, storeSchemaVersion, version)
	require.Contains(t, indexNames(t, ctx, s.DB(), "messages"), "idx_messages_channel_created_id")
	require.Contains(t, indexNames(t, ctx, s.DB(), "mention_events"), "idx_mentions_guild_event")
}

func indexNames(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()

	rows, err := db.QueryContext(ctx, `pragma index_list(`+quoteSQLString(table)+`)`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		require.NoError(t, rows.Scan(&seq, &name, &unique, &origin, &partial))
		out = append(out, name)
	}
	require.NoError(t, rows.Err())
	return out
}

func quoteSQLString(value string) string {
	return "'" + value + "'"
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

	require.NoError(t, s.DeleteSyncState(ctx, "scope:test"))
	cursor, err = s.GetSyncState(ctx, "scope:test")
	require.NoError(t, err)
	require.Empty(t, cursor)

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

	require.Equal(t, "maintainers", normalizeChannelFilter("#maintainers"))
	require.Equal(t, "maintainers", normalizeChannelFilter(" maintainers "))
	require.True(t, IsReadOnlySQL("select 1"))
	require.True(t, IsReadOnlySQL("-- comment\nselect 1"))
	require.True(t, IsReadOnlySQL("with latest as (select 1 as one) select one from latest"))
	require.False(t, IsReadOnlySQL("delete from messages"))
}

func TestIncompleteMessageChannelIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g1", Kind: "thread_public", Name: "thread", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c3", GuildID: "g1", Kind: "text", Name: "restricted", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c4", GuildID: "g2", Kind: "text", Name: "other", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "v1", GuildID: "g1", Kind: "voice", Name: "voice", RawJSON: `{}`}))

	require.NoError(t, s.SetSyncState(ctx, "channel:c2:history_complete", "1"))
	require.NoError(t, s.SetSyncState(ctx, "channel:c3:unavailable", "missing_access"))

	ids, err := s.IncompleteMessageChannelIDs(ctx, "g1")
	require.NoError(t, err)
	require.Equal(t, []string{"c1"}, ids)

	ids, err = s.IncompleteMessageChannelIDs(ctx, "")
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "c4"}, ids)
}

func TestListMessagesFiltersAndLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "maintainers", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, ChannelRecord{ID: "c2", GuildID: "g1", Kind: "text", Name: "random", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{}`,
	}))

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-01T10:00:00Z",
		Content:           "first",
		NormalizedContent: "first",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-02T10:00:00Z",
		Content:           "second",
		NormalizedContent: "second",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m3",
		GuildID:           "g1",
		ChannelID:         "c2",
		ChannelName:       "random",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-03T10:00:00Z",
		Content:           "ignore",
		NormalizedContent: "ignore",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))

	rows, err := s.ListMessages(ctx, MessageListOptions{
		Channel: "#maintainer",
		Since:   parseTime("2026-03-01T12:00:00Z"),
		Author:  "Peter",
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m2", rows[0].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Limit:   1,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m1", rows[0].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Last:    1,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m2", rows[0].MessageID)

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m4",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u2",
		MessageType:       0,
		CreatedAt:         "2026-03-04T10:00:00Z",
		Content:           "third",
		NormalizedContent: "third",
		Pinned:            true,
		HasAttachments:    true,
		RawJSON:           `{"author":{"username":"fallback-user"}}`,
	}))

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "c1",
		Before:  parseTime("2026-03-04T00:00:00Z"),
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "m1", rows[0].MessageID)
	require.Equal(t, "m2", rows[1].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Author:  "fallback-user",
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "m4", rows[0].MessageID)
	require.Equal(t, "fallback-user", rows[0].AuthorName)
	require.True(t, rows[0].Pinned)
	require.True(t, rows[0].HasAttachments)

	require.NoError(t, s.UpsertMessage(ctx, MessageRecord{
		ID:                "m5",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "maintainers",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         "2026-03-05T10:00:00Z",
		Content:           "",
		NormalizedContent: "",
		RawJSON:           `{"author":{"username":"peter"}}`,
	}))

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
	})
	require.NoError(t, err)
	require.Len(t, rows, 3)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel:      "maintainers",
		IncludeEmpty: true,
	})
	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, "m5", rows[3].MessageID)

	rows, err = s.ListMessages(ctx, MessageListOptions{
		Channel: "maintainers",
		Last:    2,
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "m2", rows[0].MessageID)
	require.Equal(t, "m4", rows[1].MessageID)
}
