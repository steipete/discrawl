package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	queryTimeout  = 15 * time.Second
	queryRowLimit = 50000
)

func (s *Store) GetSyncState(ctx context.Context, scope string) (string, error) {
	var cursor sql.NullString
	err := s.db.QueryRowContext(ctx, `select cursor from sync_state where scope = ?`, scope).Scan(&cursor)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return cursor.String, nil
}

func (s *Store) ChannelMessageBounds(ctx context.Context, channelID string) (string, string, error) {
	var oldest sql.NullString
	var newest sql.NullString
	if err := s.db.QueryRowContext(ctx, `
		select min(id), max(id)
		from messages
		where channel_id = ?
	`, channelID).Scan(&oldest, &newest); err != nil {
		return "", "", err
	}
	return oldest.String, newest.String, nil
}

func (s *Store) SearchMessages(ctx context.Context, opts SearchOptions) ([]SearchResult, error) {
	if strings.TrimSpace(opts.Query) == "" {
		return nil, nil
	}
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	args := []any{normalizeFTSQuery(opts.Query)}
	clauses := []string{"message_fts match ?"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "message_fts.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if strings.TrimSpace(opts.Channel) != "" {
		clauses = append(clauses, "(message_fts.channel_id = ? or message_fts.channel_name like ?)")
		args = append(args, opts.Channel, "%"+opts.Channel+"%")
	}
	if strings.TrimSpace(opts.Author) != "" {
		clauses = append(clauses, "(message_fts.author_id = ? or message_fts.author_name like ?)")
		args = append(args, opts.Author, "%"+opts.Author+"%")
	}
	if !opts.IncludeEmpty {
		clauses = append(clauses, "trim(coalesce(m.normalized_content, '')) <> ''")
	}
	args = append(args, opts.Limit)
	query := `
		select
			m.id, m.guild_id, m.channel_id, coalesce(c.name, ''),
			coalesce(m.author_id, ''), coalesce(message_fts.author_name, ''),
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at
		from message_fts
		join messages m on m.id = message_fts.message_id
		left join channels c on c.id = m.channel_id
		where ` + strings.Join(clauses, " and ") + `
		order by bm25(message_fts), m.created_at desc
		limit ?
	`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return s.searchFallback(ctx, opts)
	}
	defer func() { _ = rows.Close() }()
	var out []SearchResult
	for rows.Next() {
		var row SearchResult
		var created string
		if err := rows.Scan(&row.MessageID, &row.GuildID, &row.ChannelID, &row.ChannelName, &row.AuthorID, &row.AuthorName, &row.Content, &created); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) searchFallback(ctx context.Context, opts SearchOptions) ([]SearchResult, error) {
	args := []any{"%" + opts.Query + "%"}
	clauses := []string{"m.normalized_content like ?"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "m.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if strings.TrimSpace(opts.Channel) != "" {
		clauses = append(clauses, "(m.channel_id = ? or c.name like ?)")
		args = append(args, opts.Channel, "%"+opts.Channel+"%")
	}
	if strings.TrimSpace(opts.Author) != "" {
		clauses = append(clauses, "(m.author_id = ? or m.raw_json like ?)")
		args = append(args, opts.Author, "%"+opts.Author+"%")
	}
	if !opts.IncludeEmpty {
		clauses = append(clauses, "trim(coalesce(m.normalized_content, '')) <> ''")
	}
	args = append(args, opts.Limit)
	rows, err := s.db.QueryContext(ctx, `
		select
			m.id,
			m.guild_id,
			m.channel_id,
			coalesce(c.name, ''),
			coalesce(m.author_id, ''),
			'',
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at
		from messages m
		left join channels c on c.id = m.channel_id
		where `+strings.Join(clauses, " and ")+`
		order by m.created_at desc
		limit ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []SearchResult
	for rows.Next() {
		var row SearchResult
		var created string
		if err := rows.Scan(&row.MessageID, &row.GuildID, &row.ChannelID, &row.ChannelName, &row.AuthorID, &row.AuthorName, &row.Content, &created); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) Members(ctx context.Context, guildID, query string, limit int) ([]MemberRow, error) {
	if strings.TrimSpace(query) != "" {
		return s.searchMembers(ctx, guildID, query, limit)
	}
	if limit <= 0 {
		limit = 100
	}
	args := []any{}
	clauses := []string{"1=1"}
	if guildID != "" {
		clauses = append(clauses, "guild_id = ?")
		args = append(args, guildID)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		select guild_id, user_id, username, coalesce(global_name, ''), coalesce(display_name, ''),
		       coalesce(nick, ''), coalesce(discriminator, ''), coalesce(avatar, ''),
		       role_ids_json, bot, coalesce(joined_at, ''), raw_json
		from members
		where `+strings.Join(clauses, " and ")+`
		order by coalesce(nullif(display_name, ''), nullif(nick, ''), nullif(global_name, ''), username), username
		limit ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanMemberRows(rows)
}

func (s *Store) MemberByID(ctx context.Context, userID string) ([]MemberRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		select guild_id, user_id, username, coalesce(global_name, ''), coalesce(display_name, ''),
		       coalesce(nick, ''), coalesce(discriminator, ''), coalesce(avatar, ''),
		       role_ids_json, bot, coalesce(joined_at, ''), raw_json
		from members
		where user_id = ?
		order by guild_id, username
	`, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanMemberRows(rows)
}

func (s *Store) Channels(ctx context.Context, guildID string) ([]ChannelRow, error) {
	args := []any{}
	query := `
		select id, guild_id, coalesce(parent_id, ''), kind, name, coalesce(topic, ''), position,
		       is_nsfw, is_archived, is_locked, is_private_thread, coalesce(thread_parent_id, ''), coalesce(archive_timestamp, '')
		from channels
	`
	if guildID != "" {
		query += ` where guild_id = ?`
		args = append(args, guildID)
	}
	query += ` order by guild_id, position, name`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ChannelRow
	for rows.Next() {
		var row ChannelRow
		var archived int
		var locked int
		var nsfw int
		var priv int
		var archiveTS string
		if err := rows.Scan(&row.ID, &row.GuildID, &row.ParentID, &row.Kind, &row.Name, &row.Topic, &row.Position, &nsfw, &archived, &locked, &priv, &row.ThreadParentID, &archiveTS); err != nil {
			return nil, err
		}
		row.IsNSFW = nsfw == 1
		row.IsArchived = archived == 1
		row.IsLocked = locked == 1
		row.IsPrivateThread = priv == 1
		row.ArchiveTimestamp = parseTime(archiveTS)
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) Status(ctx context.Context, dbPath, defaultGuildID string) (Status, error) {
	status := Status{DBPath: dbPath, DefaultGuildID: defaultGuildID}
	queries := map[string]*int{
		`select count(*) from guilds`:                                 &status.GuildCount,
		`select count(*) from channels`:                               &status.ChannelCount,
		`select count(*) from messages`:                               &status.MessageCount,
		`select count(*) from members`:                                &status.MemberCount,
		`select count(*) from embedding_jobs where state = 'pending'`: &status.EmbeddingBacklog,
	}
	for query, target := range queries {
		if err := s.db.QueryRowContext(ctx, query).Scan(target); err != nil {
			return Status{}, err
		}
	}
	if err := s.db.QueryRowContext(ctx, `select count(*) from channels where kind like 'thread_%'`).Scan(&status.ThreadCount); err != nil {
		return Status{}, err
	}
	var lastSync string
	_ = s.db.QueryRowContext(ctx, `select updated_at from sync_state where scope = 'sync:last_success'`).Scan(&lastSync)
	status.LastSyncAt = parseTime(lastSync)
	var lastTail string
	_ = s.db.QueryRowContext(ctx, `select updated_at from sync_state where scope = 'tail:last_event'`).Scan(&lastTail)
	status.LastTailEventAt = parseTime(lastTail)
	if defaultGuildID != "" {
		_ = s.db.QueryRowContext(ctx, `select name from guilds where id = ?`, defaultGuildID).Scan(&status.DefaultGuildName)
	}
	rows, err := s.db.QueryContext(ctx, `select id from guilds order by id`)
	if err != nil {
		return Status{}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var guildID string
		if err := rows.Scan(&guildID); err != nil {
			return Status{}, err
		}
		status.AccessibleGuildIDs = append(status.AccessibleGuildIDs, guildID)
	}
	return status, rows.Err()
}

func (s *Store) ReadOnlyQuery(ctx context.Context, query string) ([]string, [][]string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil, fmt.Errorf("empty query")
	}
	if !IsReadOnlySQL(query) {
		return nil, nil, fmt.Errorf("only read-only sql is allowed")
	}
	db, closeFn, err := s.openReadOnlyDB()
	if err != nil {
		return nil, nil, err
	}
	if closeFn != nil {
		defer closeFn()
	}
	return queryRows(ctx, db, query)
}

func (s *Store) Query(ctx context.Context, query string) ([]string, [][]string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil, fmt.Errorf("empty query")
	}
	return queryRows(ctx, s.db, query)
}

func (s *Store) Exec(ctx context.Context, query string) (int64, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return 0, fmt.Errorf("empty query")
	}
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()

	result, err := s.db.ExecContext(queryCtx, query)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func queryRows(ctx context.Context, db *sql.DB, query string) ([]string, [][]string, error) {
	queryCtx, cancel := withQueryTimeout(ctx)
	defer cancel()

	rows, err := db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("query returned no columns")
	}

	var out [][]string
	for rows.Next() {
		if len(out) >= queryRowLimit {
			return nil, nil, fmt.Errorf("query returned more than %d rows", queryRowLimit)
		}
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		record := make([]string, len(cols))
		for i, value := range values {
			record[i] = stringify(value)
		}
		out = append(out, record)
	}
	return cols, out, rows.Err()
}

func (s *Store) openReadOnlyDB() (*sql.DB, func(), error) {
	if strings.TrimSpace(s.path) == "" {
		return s.db, nil, nil
	}
	if _, err := os.Stat(s.path); err != nil {
		return nil, nil, err
	}
	dsn := fmt.Sprintf(
		"file:%s?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)&_pragma=temp_store(MEMORY)&_pragma=mmap_size(268435456)",
		s.path,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, func() { _ = db.Close() }, nil
}

func withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, queryTimeout)
}

func IsReadOnlySQL(query string) bool {
	switch leadingSQLKeyword(query) {
	case "select", "explain", "pragma":
		return true
	default:
		return false
	}
}

func leadingSQLKeyword(query string) string {
	trimmed := strings.TrimSpace(query)
	for trimmed != "" {
		switch {
		case strings.HasPrefix(trimmed, "--"):
			if idx := strings.IndexByte(trimmed, '\n'); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+1:])
				continue
			}
			return ""
		case strings.HasPrefix(trimmed, "/*"):
			end := strings.Index(trimmed, "*/")
			if end < 0 {
				return ""
			}
			trimmed = strings.TrimSpace(trimmed[end+2:])
		default:
			fields := strings.Fields(trimmed)
			if len(fields) == 0 {
				return ""
			}
			return strings.ToLower(fields[0])
		}
	}
	return ""
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

func stringify(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	case string:
		return v
	case time.Time:
		return v.Format(timeLayout)
	default:
		return fmt.Sprint(v)
	}
}

func normalizeFTSQuery(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	fields := strings.Fields(raw)
	for i, field := range fields {
		fields[i] = `"` + strings.ReplaceAll(field, `"`, " ") + `"`
	}
	return strings.Join(fields, " ")
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	t, err := time.Parse(timeLayout, value)
	if err == nil {
		return t
	}
	t, _ = time.Parse(time.RFC3339, value)
	return t
}
