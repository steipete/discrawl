package report

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/steipete/discrawl/internal/store"
)

// DigestOptions controls how a Digest is built.
type DigestOptions struct {
	Now     time.Time
	Since   time.Duration
	GuildID string
	Channel string
	TopN    int
}

// Digest summarizes recent activity for each channel inside a window.
type Digest struct {
	GeneratedAt time.Time       `json:"generated_at"`
	Since       time.Time       `json:"since"`
	Until       time.Time       `json:"until"`
	WindowLabel string          `json:"window_label"`
	Guild       string          `json:"guild,omitempty"`
	Channel     string          `json:"channel,omitempty"`
	TopN        int             `json:"top_n"`
	Channels    []ChannelDigest `json:"channels"`
	Totals      DigestTotals    `json:"totals"`
}

// ChannelDigest is the per-channel roll-up inside a Digest.
type ChannelDigest struct {
	ChannelID     string        `json:"channel_id"`
	ChannelName   string        `json:"channel_name"`
	Kind          string        `json:"kind,omitempty"`
	GuildID       string        `json:"guild_id"`
	Messages      int           `json:"messages"`
	Replies       int           `json:"replies"`
	ActiveAuthors int           `json:"active_authors"`
	TopPosters    []RankedCount `json:"top_posters"`
	TopMentions   []RankedCount `json:"top_mentions"`
}

// DigestTotals sums message and channel counts across the digest window.
type DigestTotals struct {
	Messages      int `json:"messages"`
	Replies       int `json:"replies"`
	Channels      int `json:"channels"`
	ActiveAuthors int `json:"active_authors"`
}

// BuildDigest computes a per-channel activity digest from the local store.
func BuildDigest(ctx context.Context, s *store.Store, opts DigestOptions) (Digest, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()

	sinceDuration := opts.Since
	if sinceDuration < 0 {
		sinceDuration = -sinceDuration
	}
	if sinceDuration == 0 {
		sinceDuration = 7 * 24 * time.Hour
	}

	topN := opts.TopN
	if topN <= 0 {
		topN = 3
	}

	digest := Digest{
		GeneratedAt: now,
		Since:       now.Add(-sinceDuration),
		Until:       now,
		WindowLabel: humanDuration(sinceDuration),
		Guild:       strings.TrimSpace(opts.GuildID),
		Channel:     strings.TrimSpace(opts.Channel),
		TopN:        topN,
	}

	channels, err := perChannelDigest(ctx, s.DB(), digest.Since, digest.Until, digest.Guild, digest.Channel)
	if err != nil {
		return Digest{}, err
	}
	for i := range channels {
		channels[i].TopPosters, err = topPostersForDigestChannel(ctx, s.DB(), digest.Since, digest.Until, channels[i].GuildID, channels[i].ChannelID, topN)
		if err != nil {
			return Digest{}, err
		}
		channels[i].TopMentions, err = topMentionsForDigestChannel(ctx, s.DB(), digest.Since, digest.Until, channels[i].GuildID, channels[i].ChannelID, topN)
		if err != nil {
			return Digest{}, err
		}
	}
	digest.Channels = channels

	totals, err := digestTotals(ctx, s.DB(), digest.Since, digest.Until, digest.Guild, digest.Channel)
	if err != nil {
		return Digest{}, err
	}
	digest.Totals = totals

	return digest, nil
}

func perChannelDigest(ctx context.Context, db *sql.DB, since, until time.Time, guildID, channel string) ([]ChannelDigest, error) {
	query := &strings.Builder{}
	query.WriteString(`
select
	c.guild_id,
	c.id,
	coalesce(nullif(c.name, ''), c.id) as channel_name,
	coalesce(c.kind, '') as kind,
	count(m.id) as messages,
	count(case when nullif(m.reply_to_message_id, '') is not null then 1 else null end) as replies,
	count(distinct nullif(m.author_id, '')) as active_authors
from channels c
left join messages m on m.guild_id = c.guild_id
	and m.channel_id = c.id
	and m.created_at >= ?
	and m.created_at < ?
where 1=1
`)
	args := []any{since.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano)}
	if guildID != "" {
		query.WriteString("  and c.guild_id = ?\n")
		args = append(args, guildID)
	}
	if channel != "" {
		query.WriteString("  and (c.id = ? or c.name = ?)\n")
		args = append(args, channel, channel)
	}
	query.WriteString(`
group by c.guild_id, c.id, c.name, c.kind
having count(m.id) > 0
order by messages desc, channel_name asc
`)

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("digest per-channel query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ChannelDigest
	for rows.Next() {
		var row ChannelDigest
		if err := rows.Scan(&row.GuildID, &row.ChannelID, &row.ChannelName, &row.Kind, &row.Messages, &row.Replies, &row.ActiveAuthors); err != nil {
			return nil, fmt.Errorf("digest per-channel scan: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func topPostersForDigestChannel(ctx context.Context, db *sql.DB, since, until time.Time, guildID, channelID string, limit int) ([]RankedCount, error) {
	return ranked(ctx, db, `
select
	coalesce(
		nullif(mem.display_name, ''),
		nullif(mem.nick, ''),
		nullif(mem.global_name, ''),
		nullif(mem.username, ''),
		nullif(m.author_id, ''),
		'unknown'
	) as name,
	count(*) as total
from messages m
left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
where m.created_at >= ?
  and m.created_at < ?
  and m.guild_id = ?
  and m.channel_id = ?
group by m.author_id, name
order by total desc, name asc
limit ?
`, since.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano), guildID, channelID, limit)
}

func topMentionsForDigestChannel(ctx context.Context, db *sql.DB, since, until time.Time, guildID, channelID string, limit int) ([]RankedCount, error) {
	return ranked(ctx, db, `
select
	coalesce(
		nullif(me.target_name, ''),
		nullif(me.target_id, ''),
		'unknown'
	) as name,
	count(*) as total
from mention_events me
where me.event_at >= ?
  and me.event_at < ?
  and me.guild_id = ?
  and me.channel_id = ?
group by me.target_type, me.target_id, name
order by total desc, name asc
limit ?
`, since.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano), guildID, channelID, limit)
}

func digestTotals(ctx context.Context, db *sql.DB, since, until time.Time, guildID, channel string) (DigestTotals, error) {
	query := &strings.Builder{}
	query.WriteString(`
select
	count(*) as messages,
	count(case when nullif(m.reply_to_message_id, '') is not null then 1 else null end) as replies,
	count(distinct m.guild_id || '|' || m.channel_id) as channels,
	count(distinct nullif(m.author_id, '')) as active_authors
from messages m
left join channels c on c.id = m.channel_id and c.guild_id = m.guild_id
where m.created_at >= ?
  and m.created_at < ?
`)
	args := []any{since.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano)}
	if guildID != "" {
		query.WriteString("  and m.guild_id = ?\n")
		args = append(args, guildID)
	}
	if channel != "" {
		query.WriteString("  and (m.channel_id = ? or c.name = ?)\n")
		args = append(args, channel, channel)
	}

	var totals DigestTotals
	if err := db.QueryRowContext(ctx, query.String(), args...).Scan(&totals.Messages, &totals.Replies, &totals.Channels, &totals.ActiveAuthors); err != nil {
		return DigestTotals{}, fmt.Errorf("digest totals: %w", err)
	}
	return totals, nil
}

func humanDuration(d time.Duration) string {
	if d <= 0 {
		return "0"
	}
	if d%(24*time.Hour) == 0 {
		days := int(d / (24 * time.Hour))
		return fmt.Sprintf("%dd", days)
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return d.String()
}
