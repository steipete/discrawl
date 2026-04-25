package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/steipete/discrawl/internal/discorddesktop"
	"github.com/steipete/discrawl/internal/report"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/syncer"
)

func (r *runtime) print(value any) error {
	if r.json {
		enc := json.NewEncoder(r.stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(value)
	}
	if r.plain {
		if err := printPlain(r.stdout, value); err == nil {
			return nil
		}
	}
	if err := printHuman(r.stdout, value); err == nil {
		return nil
	}
	enc := json.NewEncoder(r.stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func printPlain(w io.Writer, value any) error {
	switch v := value.(type) {
	case []store.SearchResult:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", row.GuildID, row.ChannelID, row.AuthorID, row.Content)
		}
		return nil
	case []store.MemberRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", row.GuildID, row.UserID, row.Username)
		}
		return nil
	case store.MemberProfile:
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", v.Member.GuildID, v.Member.UserID, v.Member.Username)
		return nil
	case []store.ChannelRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", row.GuildID, row.ID, row.Kind, row.Name)
		}
		return nil
	case []store.MessageRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", formatTime(row.CreatedAt), row.GuildID, row.ChannelID, row.AuthorID, row.MessageID, row.Content)
		}
		return nil
	case []store.DirectMessageConversationRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n", row.ChannelID, row.Name, row.MessageCount, row.AuthorCount, formatTime(row.FirstMessageAt), formatTime(row.LastMessageAt))
		}
		return nil
	case []store.MentionRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", formatTime(row.CreatedAt), row.GuildID, row.ChannelID, row.AuthorID, row.TargetType, row.TargetID, row.Content)
		}
		return nil
	case report.Digest:
		for _, row := range v.Channels {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\n", row.ChannelID, row.ChannelName, row.Kind, row.GuildID, row.Messages, row.Threads, row.ActiveAuthors)
		}
		return nil
	case report.Quiet:
		for _, row := range v.Channels {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n", row.ChannelID, row.ChannelName, row.Kind, row.GuildID, row.LastMessage, row.DaysSilent)
		}
		return nil
	case report.Trends:
		for _, row := range v.Rows {
			for _, week := range row.Weekly {
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n", row.GuildID, row.ChannelID, row.ChannelName, row.Kind, formatTime(week.WeekStart), week.Messages)
			}
		}
		return nil
	default:
		return errors.New("no plain printer")
	}
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, `discrawl archives Discord guild data into local SQLite.

Usage:
  discrawl [global flags] <command> [args]

Commands:
  init
  sync
  tail
  wiretap
  search
  messages
  digest
  analytics
  dms
  mentions
  embed
  sql
  members
  channels
  status
  report
  doctor
`)
}

func printRows(w io.Writer, cols []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, strings.Join(cols, "\t"))
	for _, row := range rows {
		_, _ = fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	return tw.Flush()
}

func printHuman(w io.Writer, value any) error {
	switch v := value.(type) {
	case syncRunStats:
		if _, err := fmt.Fprintf(w, "source=%s\n", v.Source); err != nil {
			return err
		}
		if v.Discord != nil {
			if _, err := fmt.Fprintf(w, "discord_guilds=%d\ndiscord_channels=%d\ndiscord_threads=%d\ndiscord_members=%d\ndiscord_messages=%d\n",
				v.Discord.Guilds, v.Discord.Channels, v.Discord.Threads, v.Discord.Members, v.Discord.Messages); err != nil {
				return err
			}
		}
		if v.Wiretap != nil {
			if _, err := fmt.Fprintf(w, "wiretap_messages=%d\nwiretap_dm_messages=%d\nwiretap_dm_channels=%d\nwiretap_guild_messages=%d\nwiretap_skipped_messages=%d\nwiretap_skipped_channels=%d\n",
				v.Wiretap.Messages, v.Wiretap.DMMessages, v.Wiretap.DMChannels, v.Wiretap.GuildMessages, v.Wiretap.SkippedMessages, v.Wiretap.SkippedChannels); err != nil {
				return err
			}
		}
		return nil
	case syncer.SyncStats:
		_, err := fmt.Fprintf(w, "guilds=%d channels=%d threads=%d members=%d messages=%d\n", v.Guilds, v.Channels, v.Threads, v.Members, v.Messages)
		return err
	case discorddesktop.Stats:
		_, err := fmt.Fprintf(w, "path=%s\nfiles=%d\nskipped=%d\nobjects=%d\nguilds=%d\nchannels=%d\nmessages=%d\ndm_messages=%d\ndm_channels=%d\nguild_messages=%d\nskipped_messages=%d\nskipped_channels=%d\ndry_run=%t\n",
			v.Path, v.FilesScanned, v.FilesSkipped, v.JSONObjects, v.Guilds, v.Channels, v.Messages, v.DMMessages, v.DMChannels, v.GuildMessages, v.SkippedMessages, v.SkippedChannels, v.DryRun)
		return err
	case store.Status:
		_, err := fmt.Fprintf(w, "db=%s\nguilds=%d\nchannels=%d\nthreads=%d\nmessages=%d\nmembers=%d\nembedding_backlog=%d\nlast_sync=%s\nlast_tail_event=%s\n",
			v.DBPath, v.GuildCount, v.ChannelCount, v.ThreadCount, v.MessageCount, v.MemberCount, v.EmbeddingBacklog,
			formatTime(v.LastSyncAt), formatTime(v.LastTailEventAt))
		return err
	case store.EmbeddingDrainStats:
		_, err := fmt.Fprintf(w, "processed=%d\nsucceeded=%d\nfailed=%d\nskipped=%d\nremaining_backlog=%d\nprovider=%s\nmodel=%s\ninput_version=%s\n",
			v.Processed, v.Succeeded, v.Failed, v.Skipped, v.RemainingBacklog, v.Provider, v.Model, v.InputVersion)
		if err != nil {
			return err
		}
		if v.Requeued > 0 {
			if _, err := fmt.Fprintf(w, "requeued=%d\n", v.Requeued); err != nil {
				return err
			}
		}
		if v.RateLimited {
			_, err = fmt.Fprintln(w, "rate_limited=true")
		}
		return err
	case []store.SearchResult:
		for _, row := range v {
			if _, err := fmt.Fprintf(w, "[%s/%s] %s %s\n%s\n\n", row.GuildID, row.ChannelName, row.AuthorName, formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.MessageRow:
		for _, row := range v {
			if _, err := fmt.Fprintf(w, "[%s/%s] %s %s\n%s\n\n", row.GuildID, row.ChannelName, row.AuthorName, formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.DirectMessageConversationRow:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "CHANNEL\tNAME\tMESSAGES\tAUTHORS\tFIRST\tLAST")
		for _, row := range v {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\n",
				row.ChannelID,
				row.Name,
				row.MessageCount,
				row.AuthorCount,
				formatTime(row.FirstMessageAt),
				formatTime(row.LastMessageAt),
			)
		}
		return tw.Flush()
	case []store.MentionRow:
		for _, row := range v {
			if _, err := fmt.Fprintf(w, "[%s/%s] %s -> %s:%s %s\n%s\n\n", row.GuildID, row.ChannelName, row.AuthorName, row.TargetType, firstNonEmpty(row.TargetName, row.TargetID), formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.MemberRow:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "GUILD\tUSER\tNAME\tDISPLAY\tPROFILE")
		for _, row := range v {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
				row.GuildID,
				row.UserID,
				row.Username,
				firstNonEmpty(row.DisplayName, row.Nick, row.GlobalName),
				memberProfileSummary(row),
			)
		}
		return tw.Flush()
	case store.MemberProfile:
		if _, err := fmt.Fprintf(w, "guild=%s\nuser=%s\nusername=%s\ndisplay=%s\njoined=%s\nbot=%t\n",
			v.Member.GuildID,
			v.Member.UserID,
			v.Member.Username,
			firstNonEmpty(v.Member.DisplayName, v.Member.Nick, v.Member.GlobalName),
			formatTime(v.Member.JoinedAt),
			v.Member.Bot,
		); err != nil {
			return err
		}
		if v.Member.XHandle != "" {
			if _, err := fmt.Fprintf(w, "x=%s\n", v.Member.XHandle); err != nil {
				return err
			}
		}
		if v.Member.GitHubLogin != "" {
			if _, err := fmt.Fprintf(w, "github=%s\n", v.Member.GitHubLogin); err != nil {
				return err
			}
		}
		if v.Member.Website != "" {
			if _, err := fmt.Fprintf(w, "website=%s\n", v.Member.Website); err != nil {
				return err
			}
		}
		if v.Member.Pronouns != "" {
			if _, err := fmt.Fprintf(w, "pronouns=%s\n", v.Member.Pronouns); err != nil {
				return err
			}
		}
		if v.Member.Location != "" {
			if _, err := fmt.Fprintf(w, "location=%s\n", v.Member.Location); err != nil {
				return err
			}
		}
		if v.Member.Bio != "" {
			if _, err := fmt.Fprintf(w, "bio=%s\n", v.Member.Bio); err != nil {
				return err
			}
		}
		if len(v.Member.URLs) > 0 {
			if _, err := fmt.Fprintf(w, "urls=%s\n", strings.Join(v.Member.URLs, ", ")); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "message_count=%d\nfirst_message=%s\nlast_message=%s\n",
			v.MessageCount,
			formatTime(v.FirstMessageAt),
			formatTime(v.LastMessageAt),
		); err != nil {
			return err
		}
		if len(v.RecentMessages) == 0 {
			return nil
		}
		if _, err := fmt.Fprintln(w, "\nRecent messages:"); err != nil {
			return err
		}
		for _, row := range v.RecentMessages {
			if _, err := fmt.Fprintf(w, "[%s] %s\n%s\n\n", row.ChannelName, formatTime(row.CreatedAt), row.Content); err != nil {
				return err
			}
		}
		return nil
	case []store.ChannelRow:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "GUILD\tCHANNEL\tKIND\tNAME")
		for _, row := range v {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", row.GuildID, row.ID, row.Kind, row.Name)
		}
		return tw.Flush()
	case report.Digest:
		for _, channel := range v.Channels {
			if _, err := fmt.Fprintf(w, "%s (%s)\n", channel.ChannelName, firstNonEmpty(channel.Kind, "unknown")); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  messages=%d threads=%d authors=%d\n", channel.Messages, channel.Threads, channel.ActiveAuthors); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  top posters  %s\n", formatRankedCounts(channel.TopPosters)); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  top mentions %s\n\n", formatRankedCounts(channel.TopMentions)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "Window: %s to %s (%s)\n", formatTime(v.Since), formatTime(v.Until), v.WindowLabel); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "Totals: messages=%d threads=%d channels=%d authors=%d\n", v.Totals.Messages, v.Totals.Threads, v.Totals.Channels, v.Totals.ActiveAuthors)
		return err
	case report.Quiet:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "CHANNEL\tKIND\tLAST MESSAGE\tDAYS SILENT")
		for _, row := range v.Channels {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				row.ChannelName,
				firstNonEmpty(row.Kind, "unknown"),
				firstNonEmpty(row.LastMessage, "never"),
				formatDaysSilent(row.DaysSilent),
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "\nWindow: %s to %s (%s)\n", formatTime(v.Since), formatTime(v.Until), formatWindowDuration(v.Until.Sub(v.Since))); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "Totals: channels=%d\n", v.Totals.Channels)
		return err
	case report.Trends:
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		header := []string{"CHANNEL", "KIND", "TOTAL"}
		weekStarts := make([]time.Time, 0, v.Weeks)
		if len(v.Rows) > 0 {
			for _, week := range v.Rows[0].Weekly {
				weekStarts = append(weekStarts, week.WeekStart)
			}
		} else {
			for i := 0; i < v.Weeks; i++ {
				weekStarts = append(weekStarts, v.Since.Add(time.Duration(i)*7*24*time.Hour))
			}
		}
		for _, start := range weekStarts {
			header = append(header, start.Format("2006-01-02"))
		}
		_, _ = fmt.Fprintln(tw, strings.Join(header, "\t"))
		for _, row := range v.Rows {
			cols := []string{row.ChannelName, firstNonEmpty(row.Kind, "unknown"), fmt.Sprintf("%d", trendsRowTotal(row.Weekly))}
			for _, week := range row.Weekly {
				cols = append(cols, fmt.Sprintf("%d", week.Messages))
			}
			_, _ = fmt.Fprintln(tw, strings.Join(cols, "\t"))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "\nWindow: %s to %s (%d weeks)\n", formatTime(v.Since), formatTime(v.Until), v.Weeks)
		return err
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, err := fmt.Fprintf(w, "%s=%v\n", key, v[key]); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("no human printer")
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func memberProfileSummary(row store.MemberRow) string {
	parts := []string{}
	if row.XHandle != "" {
		parts = append(parts, "x:"+row.XHandle)
	}
	if row.GitHubLogin != "" {
		parts = append(parts, "gh:"+row.GitHubLogin)
	}
	if row.Website != "" {
		parts = append(parts, row.Website)
	}
	if row.Bio != "" {
		parts = append(parts, trimForTable(row.Bio))
	}
	return strings.Join(parts, " | ")
}

func trimForTable(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 40 {
		return value
	}
	return value[:37] + "..."
}

func formatRankedCounts(rows []report.RankedCount) string {
	if len(rows) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, fmt.Sprintf("%s (%d)", firstNonEmpty(row.Name, "unknown"), row.Count))
	}
	return strings.Join(parts, ", ")
}

func formatDaysSilent(days int) string {
	if days < 0 {
		return "-"
	}
	return fmt.Sprintf("%d", days)
}

func formatWindowDuration(d time.Duration) string {
	if d <= 0 {
		return "0"
	}
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return d.String()
}

func trendsRowTotal(weekly []report.WeeklyCount) int {
	total := 0
	for _, row := range weekly {
		total += row.Messages
	}
	return total
}
