package store

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"strings"
	"time"
)

type MemberProfile struct {
	Member         MemberRow    `json:"member"`
	RawJSON        string       `json:"raw_json,omitempty"`
	MessageCount   int          `json:"message_count"`
	FirstMessageAt time.Time    `json:"first_message_at,omitempty"`
	LastMessageAt  time.Time    `json:"last_message_at,omitempty"`
	RecentMessages []MessageRow `json:"recent_messages,omitempty"`
}

func (s *Store) ensureMemberFTSRowIDs(ctx context.Context) error {
	var version sql.NullString
	err := s.db.QueryRowContext(ctx, `
		select cursor
		from sync_state
		where scope = 'schema:member_fts_rowid_version'
	`).Scan(&version)
	if err == nil && version.String == memberFTSVersion {
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("check member fts schema version: %w", err)
	}
	if err := s.rebuildMemberFTS(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		insert into sync_state(scope, cursor, updated_at)
		values(?, ?, ?)
		on conflict(scope) do update set
			cursor=excluded.cursor,
			updated_at=excluded.updated_at
	`, "schema:member_fts_rowid_version", memberFTSVersion, time.Now().UTC().Format(timeLayout)); err != nil {
		return fmt.Errorf("stamp member fts schema version: %w", err)
	}
	return nil
}

func (s *Store) rebuildMemberFTS(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	if _, err := tx.ExecContext(ctx, `drop table if exists member_fts`); err != nil {
		return fmt.Errorf("drop member_fts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		create virtual table member_fts using fts5(
			member_key unindexed,
			guild_id unindexed,
			user_id unindexed,
			username,
			display_name,
			profile_text
		)
	`); err != nil {
		return fmt.Errorf("create member_fts: %w", err)
	}
	if err := configureFTSBulkLoad(ctx, tx, "member_fts"); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `
		select
			guild_id,
			user_id,
			username,
			coalesce(nullif(display_name, ''), nullif(nick, ''), nullif(global_name, ''), username, ''),
			raw_json
		from members
		order by guild_id, user_id
	`)
	if err != nil {
		return fmt.Errorf("query member fts rebuild rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	stmt, err := tx.PrepareContext(ctx, `
		insert into member_fts(
			rowid, member_key, guild_id, user_id, username, display_name, profile_text
		) values(?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare member fts rebuild: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for rows.Next() {
		var guildID string
		var userID string
		var username string
		var displayName string
		var rawJSON string
		if err := rows.Scan(&guildID, &userID, &username, &displayName, &rawJSON); err != nil {
			return fmt.Errorf("scan member fts rebuild row: %w", err)
		}
		rowID := memberFTSRowID(guildID, userID)
		if _, err := stmt.ExecContext(
			ctx,
			rowID,
			memberKey(guildID, userID),
			guildID,
			userID,
			username,
			displayName,
			memberProfileSearchText(rawJSON),
		); err != nil {
			return fmt.Errorf("insert member fts rebuild row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate member fts rebuild rows: %w", err)
	}
	if err := optimizeFTS(ctx, tx, "member_fts"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) searchMembers(ctx context.Context, guildID, query string, limit int) ([]MemberRow, error) {
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	args := []any{normalizeFTSQuery(query), query}
	clauses := []string{"(member_fts match ? or m.user_id = ?)"}
	if guildID != "" {
		clauses = append(clauses, "member_fts.guild_id = ?")
		args = append(args, guildID)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		select
			m.guild_id,
			m.user_id,
			m.username,
			coalesce(m.global_name, ''),
			coalesce(m.display_name, ''),
			coalesce(m.nick, ''),
			coalesce(m.discriminator, ''),
			coalesce(m.avatar, ''),
			m.role_ids_json,
			m.bot,
			coalesce(m.joined_at, ''),
			m.raw_json
		from member_fts
		join members m on m.guild_id = member_fts.guild_id and m.user_id = member_fts.user_id
		where `+strings.Join(clauses, " and ")+`
		order by bm25(member_fts), coalesce(nullif(m.display_name, ''), nullif(m.nick, ''), nullif(m.global_name, ''), m.username), m.username
		limit ?
	`, args...)
	if err != nil {
		return s.searchMembersFallback(ctx, guildID, query, limit)
	}
	defer func() { _ = rows.Close() }()
	return scanMemberRows(rows)
}

func (s *Store) searchMembersFallback(ctx context.Context, guildID, query string, limit int) ([]MemberRow, error) {
	args := []any{}
	clauses := []string{"1=1"}
	if guildID != "" {
		clauses = append(clauses, "guild_id = ?")
		args = append(args, guildID)
	}
	if query != "" {
		clauses = append(clauses, `(user_id = ? or username like ? or coalesce(global_name, '') like ? or coalesce(display_name, '') like ? or coalesce(nick, '') like ? or coalesce(discriminator, '') = ? or raw_json like ?)`)
		args = append(args, query, "%"+query+"%", "%"+query+"%", "%"+query+"%", "%"+query+"%", query, "%"+query+"%")
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		select
			guild_id,
			user_id,
			username,
			coalesce(global_name, ''),
			coalesce(display_name, ''),
			coalesce(nick, ''),
			coalesce(discriminator, ''),
			coalesce(avatar, ''),
			role_ids_json,
			bot,
			coalesce(joined_at, ''),
			raw_json
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

func (s *Store) MemberProfile(ctx context.Context, guildID, userID string, recentLimit int) (MemberProfile, error) {
	if recentLimit <= 0 {
		recentLimit = 20
	}
	rows, err := s.searchMembersFallback(ctx, guildID, userID, 10)
	if err != nil {
		return MemberProfile{}, err
	}
	var member *MemberRow
	for i := range rows {
		if rows[i].UserID == userID && (guildID == "" || rows[i].GuildID == guildID) {
			member = &rows[i]
			break
		}
	}
	if member == nil {
		return MemberProfile{}, sql.ErrNoRows
	}

	profile := MemberProfile{
		Member:  *member,
		RawJSON: member.RawJSON,
	}
	var first string
	var last string
	if err := s.db.QueryRowContext(ctx, `
		select count(*), coalesce(min(created_at), ''), coalesce(max(created_at), '')
		from messages
		where guild_id = ? and author_id = ?
	`, member.GuildID, member.UserID).Scan(&profile.MessageCount, &first, &last); err != nil {
		return MemberProfile{}, err
	}
	profile.FirstMessageAt = parseTime(first)
	profile.LastMessageAt = parseTime(last)

	recentRows, err := s.db.QueryContext(ctx, `
		select
			m.id,
			m.guild_id,
			m.channel_id,
			coalesce(c.name, ''),
			coalesce(m.author_id, ''),
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				''
			),
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at,
			coalesce(m.reply_to_message_id, ''),
			m.has_attachments,
			m.pinned
		from messages m
		left join channels c on c.id = m.channel_id
		left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
		where m.guild_id = ? and m.author_id = ?
		order by m.created_at desc, m.id desc
		limit ?
	`, member.GuildID, member.UserID, recentLimit)
	if err != nil {
		return MemberProfile{}, err
	}
	defer func() { _ = recentRows.Close() }()

	for recentRows.Next() {
		var row MessageRow
		var created string
		var hasAttachments int
		var pinned int
		if err := recentRows.Scan(
			&row.MessageID,
			&row.GuildID,
			&row.ChannelID,
			&row.ChannelName,
			&row.AuthorID,
			&row.AuthorName,
			&row.Content,
			&created,
			&row.ReplyToMessage,
			&hasAttachments,
			&pinned,
		); err != nil {
			return MemberProfile{}, err
		}
		row.CreatedAt = parseTime(created)
		row.HasAttachments = hasAttachments == 1
		row.Pinned = pinned == 1
		profile.RecentMessages = append(profile.RecentMessages, row)
	}
	if err := recentRows.Err(); err != nil {
		return MemberProfile{}, err
	}
	return profile, nil
}

func scanMemberRows(rows *sql.Rows) ([]MemberRow, error) {
	var out []MemberRow
	for rows.Next() {
		var row MemberRow
		var joined string
		var bot int
		if err := rows.Scan(
			&row.GuildID,
			&row.UserID,
			&row.Username,
			&row.GlobalName,
			&row.DisplayName,
			&row.Nick,
			&row.Discriminator,
			&row.Avatar,
			&row.RoleIDsJSON,
			&bot,
			&joined,
			&row.RawJSON,
		); err != nil {
			return nil, err
		}
		row.Bot = bot == 1
		row.JoinedAt = parseTime(joined)
		enrichMemberRow(&row)
		out = append(out, row)
	}
	return out, rows.Err()
}

func enrichMemberRow(row *MemberRow) {
	if row == nil || row.RawJSON == "" {
		return
	}
	profile := extractMemberProfile(row.RawJSON)
	row.Bio = profile.Bio
	row.Pronouns = profile.Pronouns
	row.Location = profile.Location
	row.Website = profile.Website
	row.XHandle = profile.XHandle
	row.GitHubLogin = profile.GitHubLogin
	row.URLs = profile.URLs
}

func memberKey(guildID, userID string) string {
	return guildID + ":" + userID
}

func memberFTSRowID(guildID, userID string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(memberKey(guildID, userID)))
	rowID := int64(hash.Sum64() & ((uint64(1) << 63) - 1))
	if rowID == 0 {
		rowID = 1
	}
	return rowID
}
