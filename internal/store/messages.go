package store

import (
	"context"
	"strings"
	"time"
)

type MessageListOptions struct {
	GuildIDs     []string
	Channel      string
	Author       string
	Since        time.Time
	Before       time.Time
	Limit        int
	Last         int
	IncludeEmpty bool
}

type MentionListOptions struct {
	GuildIDs   []string
	Channel    string
	Author     string
	Target     string
	TargetType string
	Since      time.Time
	Before     time.Time
	Limit      int
}

type MessageRow struct {
	MessageID      string    `json:"message_id"`
	GuildID        string    `json:"guild_id"`
	GuildName      string    `json:"guild_name,omitempty"`
	ChannelID      string    `json:"channel_id"`
	ChannelName    string    `json:"channel_name"`
	AuthorID       string    `json:"author_id"`
	AuthorName     string    `json:"author_name"`
	Content        string    `json:"content"`
	DisplayContent string    `json:"display_content,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	ReplyToMessage string    `json:"reply_to_message_id,omitempty"`
	Source         string    `json:"source,omitempty"`
	HasAttachments bool      `json:"has_attachments"`
	Pinned         bool      `json:"pinned"`
}

func (s *Store) ListMessages(ctx context.Context, opts MessageListOptions) ([]MessageRow, error) {
	args := []any{}
	clauses := []string{"1=1"}
	if len(opts.GuildIDs) > 0 {
		clauses = append(clauses, "m.guild_id in ("+placeholders(len(opts.GuildIDs))+")")
		for _, guildID := range opts.GuildIDs {
			args = append(args, guildID)
		}
	}
	if channel := normalizeChannelFilter(opts.Channel); channel != "" {
		clauses = append(clauses, "(m.channel_id = ? or c.name = ? or c.name like ?)")
		args = append(args, channel, channel, "%"+channel+"%")
	}
	if author := strings.TrimSpace(opts.Author); author != "" {
		clauses = append(clauses, `(m.author_id = ? or coalesce(mem.username, '') = ? or coalesce(mem.display_name, '') = ? or coalesce(mem.username, '') like ? or coalesce(mem.display_name, '') like ? or json_extract(m.raw_json, '$.author.username') = ?)`)
		args = append(args, author, author, author, "%"+author+"%", "%"+author+"%", author)
	}
	if !opts.Since.IsZero() {
		clauses = append(clauses, "m.created_at >= ?")
		args = append(args, opts.Since.UTC().Format(timeLayout))
	}
	if !opts.Before.IsZero() {
		clauses = append(clauses, "m.created_at < ?")
		args = append(args, opts.Before.UTC().Format(timeLayout))
	}
	if !opts.IncludeEmpty {
		clauses = append(clauses, "trim(coalesce(m.normalized_content, '')) <> ''")
	}

	baseQuery := `
		select
			m.id,
			m.guild_id,
			coalesce(g.name, ''),
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
			coalesce(json_extract(m.raw_json, '$.source'), ''),
			m.has_attachments,
			m.pinned
		from messages m
		left join guilds g on g.id = m.guild_id
		left join channels c on c.id = m.channel_id
		left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
		where ` + strings.Join(clauses, " and ") + `
	`

	query := baseQuery
	switch {
	case opts.Last > 0:
		query = `
			select * from (` + baseQuery + `
				order by m.created_at desc, m.id desc
				limit ?
			) recent
			order by created_at asc, id asc
		`
		args = append(args, opts.Last)
	case opts.Limit > 0:
		query += `
			order by m.created_at asc, m.id asc
			limit ?
		`
		args = append(args, opts.Limit)
	default:
		query += `
			order by m.created_at asc, m.id asc
		`
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []MessageRow
	for rows.Next() {
		var row MessageRow
		var created string
		var hasAttachments int
		var pinned int
		if err := rows.Scan(
			&row.MessageID,
			&row.GuildID,
			&row.GuildName,
			&row.ChannelID,
			&row.ChannelName,
			&row.AuthorID,
			&row.AuthorName,
			&row.Content,
			&created,
			&row.ReplyToMessage,
			&row.Source,
			&hasAttachments,
			&pinned,
		); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		row.HasAttachments = hasAttachments == 1
		row.Pinned = pinned == 1
		row.DisplayContent = row.Content
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.resolveMessageDisplayMentions(ctx, out)
}

func normalizeChannelFilter(raw string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "#"))
}

func (s *Store) resolveMessageDisplayMentions(ctx context.Context, rows []MessageRow) error {
	if len(rows) == 0 {
		return nil
	}
	ids := make([]any, 0, len(rows))
	indexByID := make(map[string]int, len(rows))
	for index, row := range rows {
		id := strings.TrimSpace(row.MessageID)
		if id == "" {
			continue
		}
		ids = append(ids, id)
		indexByID[id] = index
	}
	if len(ids) == 0 {
		return nil
	}
	query := `select message_id, target_type, target_id, target_name from mention_events where message_id in (` + placeholders(len(ids)) + `)`
	mentionRows, err := s.db.QueryContext(ctx, query, ids...)
	if err != nil {
		return err
	}
	defer func() { _ = mentionRows.Close() }()
	for mentionRows.Next() {
		var messageID, targetType, targetID, targetName string
		if err := mentionRows.Scan(&messageID, &targetType, &targetID, &targetName); err != nil {
			return err
		}
		index, ok := indexByID[messageID]
		if !ok {
			continue
		}
		rows[index].DisplayContent = replaceDiscordMention(rows[index].DisplayContent, targetType, targetID, targetName)
	}
	return mentionRows.Err()
}

func replaceDiscordMention(content, targetType, targetID, targetName string) string {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return content
	}
	label := strings.TrimSpace(targetName)
	if label == "" {
		label = targetID
	}
	switch strings.TrimSpace(targetType) {
	case "role":
		return strings.ReplaceAll(content, "<@&"+targetID+">", "@"+label)
	case "channel":
		return strings.ReplaceAll(content, "<#"+targetID+">", "#"+label)
	default:
		content = strings.ReplaceAll(content, "<@"+targetID+">", "@"+label)
		return strings.ReplaceAll(content, "<@!"+targetID+">", "@"+label)
	}
}
