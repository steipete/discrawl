package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

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
	case []store.MentionRow:
		for _, row := range v {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", formatTime(row.CreatedAt), row.GuildID, row.ChannelID, row.AuthorID, row.TargetType, row.TargetID, row.Content)
		}
		return nil
	default:
		return fmt.Errorf("no plain printer")
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
  search
  messages
  mentions
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
	case syncer.SyncStats:
		_, err := fmt.Fprintf(w, "guilds=%d channels=%d threads=%d members=%d messages=%d\n", v.Guilds, v.Channels, v.Threads, v.Members, v.Messages)
		return err
	case store.Status:
		_, err := fmt.Fprintf(w, "db=%s\nguilds=%d\nchannels=%d\nthreads=%d\nmessages=%d\nmembers=%d\nembedding_backlog=%d\nlast_sync=%s\nlast_tail_event=%s\n",
			v.DBPath, v.GuildCount, v.ChannelCount, v.ThreadCount, v.MessageCount, v.MemberCount, v.EmbeddingBacklog,
			formatTime(v.LastSyncAt), formatTime(v.LastTailEventAt))
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
		return fmt.Errorf("no human printer")
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
