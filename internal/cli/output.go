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
		_, _ = fmt.Fprintln(tw, "GUILD\tUSER\tNAME\tDISPLAY")
		for _, row := range v {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", row.GuildID, row.UserID, row.Username, firstNonEmpty(row.DisplayName, row.Nick, row.GlobalName))
		}
		return tw.Flush()
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
