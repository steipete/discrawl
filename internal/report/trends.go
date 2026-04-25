package report

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/steipete/discrawl/internal/store"
)

const secondsPerWeek = int64(7 * 24 * 60 * 60)

// TrendsOptions controls how a Trends report is built.
type TrendsOptions struct {
	Now     time.Time
	Weeks   int
	GuildID string
	Channel string
}

// Trends summarizes week-over-week message volume per channel.
type Trends struct {
	GeneratedAt time.Time   `json:"generated_at"`
	Since       time.Time   `json:"since"`
	Until       time.Time   `json:"until"`
	Weeks       int         `json:"weeks"`
	Guild       string      `json:"guild,omitempty"`
	Channel     string      `json:"channel,omitempty"`
	Rows        []TrendsRow `json:"rows"`
}

// TrendsRow is one channel's weekly message trend.
type TrendsRow struct {
	GuildID     string        `json:"guild_id"`
	ChannelID   string        `json:"channel_id"`
	ChannelName string        `json:"channel_name"`
	Kind        string        `json:"kind,omitempty"`
	Weekly      []WeeklyCount `json:"weekly"`
}

// WeeklyCount is the message count for one week bucket.
type WeeklyCount struct {
	WeekStart time.Time `json:"week_start"`
	Messages  int       `json:"messages"`
}

// BuildTrends computes weekly message counts per channel.
func BuildTrends(ctx context.Context, s *store.Store, opts TrendsOptions) (Trends, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()

	weeks := opts.Weeks
	if weeks <= 0 {
		weeks = 8
	}

	guildID := strings.TrimSpace(opts.GuildID)
	channel := strings.TrimSpace(opts.Channel)

	untilBucket := now.Unix() / secondsPerWeek
	sinceBucket := untilBucket - int64(weeks) + 1
	since := time.Unix(sinceBucket*secondsPerWeek, 0).UTC()

	rows, err := trendsRows(ctx, s.DB(), since, now, sinceBucket, untilBucket, weeks, guildID, channel)
	if err != nil {
		return Trends{}, err
	}

	return Trends{
		GeneratedAt: now,
		Since:       since,
		Until:       now,
		Weeks:       weeks,
		Guild:       guildID,
		Channel:     channel,
		Rows:        rows,
	}, nil
}

type trendChannel struct {
	guildID     string
	channelID   string
	channelName string
	kind        string
}

func trendsRows(ctx context.Context, db *sql.DB, since, until time.Time, sinceBucket, untilBucket int64, weeks int, guildID, channel string) ([]TrendsRow, error) {
	channels, err := listTrendChannels(ctx, db, guildID, channel)
	if err != nil {
		return nil, err
	}
	counts, err := listTrendCounts(ctx, db, since, until, sinceBucket, untilBucket, guildID, channel)
	if err != nil {
		return nil, err
	}

	out := make([]TrendsRow, 0, len(channels))
	for _, ch := range channels {
		weekly := make([]WeeklyCount, 0, weeks)
		for i := 0; i < weeks; i++ {
			bucket := sinceBucket + int64(i)
			count := counts[ch.channelID][bucket]
			weekly = append(weekly, WeeklyCount{
				WeekStart: time.Unix(bucket*secondsPerWeek, 0).UTC(),
				Messages:  count,
			})
		}
		out = append(out, TrendsRow{
			GuildID:     ch.guildID,
			ChannelID:   ch.channelID,
			ChannelName: ch.channelName,
			Kind:        ch.kind,
			Weekly:      weekly,
		})
	}

	sort.SliceStable(out, func(i, j int) bool {
		it := weeklyTotal(out[i].Weekly)
		jt := weeklyTotal(out[j].Weekly)
		if it != jt {
			return it > jt
		}
		if out[i].ChannelName != out[j].ChannelName {
			return out[i].ChannelName < out[j].ChannelName
		}
		if out[i].GuildID != out[j].GuildID {
			return out[i].GuildID < out[j].GuildID
		}
		return out[i].ChannelID < out[j].ChannelID
	})

	return out, nil
}

func listTrendChannels(ctx context.Context, db *sql.DB, guildID, channel string) ([]trendChannel, error) {
	query := &strings.Builder{}
	query.WriteString(`
select
	c.guild_id,
	c.id,
	coalesce(nullif(c.name, ''), c.id) as channel_name,
	coalesce(c.kind, '') as kind
from channels c
where 1 = 1
`)
	args := make([]any, 0, 3)
	if guildID != "" {
		query.WriteString("  and c.guild_id = ?\n")
		args = append(args, guildID)
	}
	if channel != "" {
		query.WriteString("  and (c.id = ? or c.name = ?)\n")
		args = append(args, channel, channel)
	}
	query.WriteString("order by channel_name asc, c.guild_id asc, c.id asc\n")

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("trends channels query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]trendChannel, 0)
	for rows.Next() {
		var row trendChannel
		if err := rows.Scan(&row.guildID, &row.channelID, &row.channelName, &row.kind); err != nil {
			return nil, fmt.Errorf("trends channels scan: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func listTrendCounts(ctx context.Context, db *sql.DB, since, until time.Time, sinceBucket, untilBucket int64, guildID, channel string) (map[string]map[int64]int, error) {
	query := &strings.Builder{}
	query.WriteString(`
select
	m.channel_id,
	cast(strftime('%s', m.created_at) / ? as integer) as week_bucket,
	count(*) as messages
from messages m
left join channels c on c.id = m.channel_id and c.guild_id = m.guild_id
where m.created_at >= ?
  and m.created_at < ?
  and cast(strftime('%s', m.created_at) / ? as integer) >= ?
  and cast(strftime('%s', m.created_at) / ? as integer) <= ?
`)
	args := []any{
		secondsPerWeek,
		since.UTC().Format(time.RFC3339Nano),
		until.UTC().Format(time.RFC3339Nano),
		secondsPerWeek,
		sinceBucket,
		secondsPerWeek,
		untilBucket,
	}
	if guildID != "" {
		query.WriteString("  and m.guild_id = ?\n")
		args = append(args, guildID)
	}
	if channel != "" {
		query.WriteString("  and (m.channel_id = ? or c.name = ?)\n")
		args = append(args, channel, channel)
	}
	query.WriteString(`group by m.channel_id, week_bucket
order by m.channel_id asc, week_bucket asc
`)

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("trends counts query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]map[int64]int)
	for rows.Next() {
		var channelID string
		var weekBucket int64
		var messages int
		if err := rows.Scan(&channelID, &weekBucket, &messages); err != nil {
			return nil, fmt.Errorf("trends counts scan: %w", err)
		}
		if out[channelID] == nil {
			out[channelID] = make(map[int64]int)
		}
		out[channelID][weekBucket] = messages
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func weeklyTotal(weekly []WeeklyCount) int {
	total := 0
	for _, w := range weekly {
		total += w.Messages
	}
	return total
}
