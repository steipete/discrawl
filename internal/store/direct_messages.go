package store

import (
	"context"
	"strings"
	"time"
)

const DirectMessageGuildID = "@me"

type DirectMessageConversationOptions struct {
	With  string
	Limit int
}

type DirectMessageConversationRow struct {
	ChannelID      string    `json:"channel_id"`
	Name           string    `json:"name"`
	MessageCount   int       `json:"message_count"`
	AuthorCount    int       `json:"author_count"`
	FirstMessageAt time.Time `json:"first_message_at,omitempty"`
	LastMessageAt  time.Time `json:"last_message_at,omitempty"`
}

func (s *Store) DirectMessageConversations(ctx context.Context, opts DirectMessageConversationOptions) ([]DirectMessageConversationRow, error) {
	args := []any{DirectMessageGuildID}
	clauses := []string{"c.guild_id = ?"}
	if with := strings.TrimSpace(opts.With); with != "" {
		clauses = append(clauses, `(
			c.id = ? or c.name = ? or c.name like ? or exists (
				select 1
				from messages mx
				where mx.guild_id = ?
				  and mx.channel_id = c.id
				  and (
					mx.author_id = ?
					or coalesce(json_extract(mx.raw_json, '$.author.username'), '') = ?
					or coalesce(json_extract(mx.raw_json, '$.author.global_name'), '') = ?
					or coalesce(json_extract(mx.raw_json, '$.author.username'), '') like ?
					or coalesce(json_extract(mx.raw_json, '$.author.global_name'), '') like ?
				  )
			)
		)`)
		like := "%" + with + "%"
		args = append(args, with, with, like, DirectMessageGuildID, with, with, with, like, like)
	}

	query := `
		select
			c.id,
			c.name,
			count(m.id),
			count(distinct m.author_id),
			coalesce(min(m.created_at), ''),
			coalesce(max(m.created_at), '')
		from channels c
		left join messages m on m.guild_id = c.guild_id and m.channel_id = c.id
		where ` + strings.Join(clauses, " and ") + `
		group by c.id, c.name
		order by coalesce(max(m.created_at), '') desc, c.name
	`
	if opts.Limit > 0 {
		query += ` limit ?`
		args = append(args, opts.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := []DirectMessageConversationRow{}
	for rows.Next() {
		var row DirectMessageConversationRow
		var first string
		var last string
		if err := rows.Scan(&row.ChannelID, &row.Name, &row.MessageCount, &row.AuthorCount, &first, &last); err != nil {
			return nil, err
		}
		row.FirstMessageAt = parseTime(first)
		row.LastMessageAt = parseTime(last)
		out = append(out, row)
	}
	return out, rows.Err()
}
