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

const (
	quietChannelKindPredicate   = "c.kind in ('text', 'announcement')"
	messageChannelKindPredicate = "c.kind in ('text', 'announcement', 'thread_public', 'thread_private', 'thread_announcement')"
)

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

// WeeklyCount is the message count for one Monday-start UTC week.
type WeeklyCount struct {
	WeekStart time.Time `json:"week_start"`
	Messages  int       `json:"messages"`
}

// BuildTrends computes Monday-start UTC weekly message counts per channel.
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
	currentWeek := startOfWeekUTC(now)
	since := currentWeek.AddDate(0, 0, -7*(weeks-1))
	until := now

	guildID := strings.TrimSpace(opts.GuildID)
	channel := strings.TrimSpace(opts.Channel)

	rows, err := trendsRows(ctx, s.DB(), since, until, weeks, guildID, channel)
	if err != nil {
		return Trends{}, err
	}

	return Trends{
		GeneratedAt: now,
		Since:       since,
		Until:       until,
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

func (c trendChannel) key() string {
	return c.guildID + "\x00" + c.channelID
}

func trendsRows(ctx context.Context, db *sql.DB, since, until time.Time, weeks int, guildID, channel string) ([]TrendsRow, error) {
	channels, err := listTrendChannels(ctx, db, guildID, channel)
	if err != nil {
		return nil, err
	}
	counts, err := listTrendCounts(ctx, db, since, until, guildID, channel)
	if err != nil {
		return nil, err
	}

	out := make([]TrendsRow, 0, len(channels))
	for _, ch := range channels {
		weekly := make([]WeeklyCount, 0, weeks)
		for i := 0; i < weeks; i++ {
			start := since.AddDate(0, 0, 7*i)
			weekly = append(weekly, WeeklyCount{
				WeekStart: start,
				Messages:  counts[ch.key()][start.Format(time.DateOnly)],
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
where ` + messageChannelKindPredicate + `
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

func listTrendCounts(ctx context.Context, db *sql.DB, since, until time.Time, guildID, channel string) (map[string]map[string]int, error) {
	query := &strings.Builder{}
	query.WriteString(`
select
	m.guild_id,
	m.channel_id,
	date(m.created_at, '-' || ((cast(strftime('%w', m.created_at) as integer) + 6) % 7) || ' days') as week_start,
	count(*) as messages
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
	query.WriteString(`group by m.guild_id, m.channel_id, week_start
order by m.guild_id asc, m.channel_id asc, week_start asc
`)

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("trends counts query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]map[string]int)
	for rows.Next() {
		var guildID, channelID, weekStart string
		var messages int
		if err := rows.Scan(&guildID, &channelID, &weekStart, &messages); err != nil {
			return nil, fmt.Errorf("trends counts scan: %w", err)
		}
		key := guildID + "\x00" + channelID
		if out[key] == nil {
			out[key] = make(map[string]int)
		}
		out[key][weekStart] = messages
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func startOfWeekUTC(t time.Time) time.Time {
	t = t.UTC()
	dayStart := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	offset := (int(dayStart.Weekday()) + 6) % 7
	return dayStart.AddDate(0, 0, -offset)
}

func weeklyTotal(weekly []WeeklyCount) int {
	total := 0
	for _, w := range weekly {
		total += w.Messages
	}
	return total
}
