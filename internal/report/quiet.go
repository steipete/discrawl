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

// QuietOptions controls how a Quiet report is built.
type QuietOptions struct {
	Now     time.Time
	Since   time.Duration
	GuildID string
}

// Quiet summarizes channels with no activity in a window.
type Quiet struct {
	GeneratedAt time.Time      `json:"generated_at"`
	Since       time.Time      `json:"since"`
	Until       time.Time      `json:"until"`
	Guild       string         `json:"guild,omitempty"`
	Channels    []QuietChannel `json:"channels"`
	Totals      QuietTotals    `json:"totals"`
}

// QuietChannel is one channel with no recent activity.
type QuietChannel struct {
	ChannelID   string `json:"channel_id"`
	ChannelName string `json:"channel_name"`
	Kind        string `json:"kind,omitempty"`
	GuildID     string `json:"guild_id"`
	LastMessage string `json:"last_message,omitempty"`
	DaysSilent  int    `json:"days_silent"`
}

// QuietTotals summarizes quiet-channel counts.
type QuietTotals struct {
	Channels int `json:"channels"`
}

// BuildQuiet computes channels with no messages newer than the lookback window.
func BuildQuiet(ctx context.Context, s *store.Store, opts QuietOptions) (Quiet, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()

	sinceWindow := opts.Since
	if sinceWindow < 0 {
		sinceWindow = -sinceWindow
	}
	if sinceWindow == 0 {
		sinceWindow = 30 * 24 * time.Hour
	}
	since := now.Add(-sinceWindow)

	out := Quiet{
		GeneratedAt: now,
		Since:       since,
		Until:       now,
		Guild:       strings.TrimSpace(opts.GuildID),
	}

	channels, err := quietChannels(ctx, s.DB(), since, now, out.Guild)
	if err != nil {
		return Quiet{}, err
	}
	out.Channels = channels
	out.Totals = QuietTotals{Channels: len(channels)}
	return out, nil
}

func quietChannels(ctx context.Context, db *sql.DB, since, now time.Time, guildID string) ([]QuietChannel, error) {
	query := &strings.Builder{}
	query.WriteString(`
with latest_messages as (
	select
		m.guild_id,
		m.channel_id,
		max(m.created_at) as last_message
	from messages m
	group by m.guild_id, m.channel_id
)
select
	c.guild_id,
	c.id as channel_id,
	coalesce(nullif(c.name, ''), c.id) as channel_name,
	coalesce(c.kind, '') as kind,
	coalesce(lm.last_message, '') as last_message
from channels c
left join latest_messages lm on lm.guild_id = c.guild_id and lm.channel_id = c.id
where ` + quietChannelKindPredicate + `
`)
	args := make([]any, 0, 2)
	if guildID != "" {
		query.WriteString("  and c.guild_id = ?\n")
		args = append(args, guildID)
	}
	query.WriteString("  and (lm.last_message is null or lm.last_message < ?)\n")
	args = append(args, since.UTC().Format(time.RFC3339Nano))
	query.WriteString("order by channel_name asc\n")

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("quiet channels query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]QuietChannel, 0)
	for rows.Next() {
		var row QuietChannel
		if err := rows.Scan(&row.GuildID, &row.ChannelID, &row.ChannelName, &row.Kind, &row.LastMessage); err != nil {
			return nil, fmt.Errorf("quiet channels scan: %w", err)
		}
		if strings.TrimSpace(row.LastMessage) == "" {
			row.LastMessage = ""
			row.DaysSilent = -1
		} else {
			last, err := time.Parse(time.RFC3339Nano, row.LastMessage)
			if err != nil {
				return nil, fmt.Errorf("quiet last message parse: %w", err)
			}
			last = last.UTC()
			row.LastMessage = last.Format(time.RFC3339)
			row.DaysSilent = int(now.Sub(last).Hours() / 24)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.SliceStable(out, func(i, j int) bool {
		iNever := out[i].DaysSilent < 0
		jNever := out[j].DaysSilent < 0
		if iNever != jNever {
			return iNever
		}
		if out[i].DaysSilent != out[j].DaysSilent {
			return out[i].DaysSilent > out[j].DaysSilent
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
