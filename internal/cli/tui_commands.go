package cli

import (
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/vincentkoc/crawlkit/tui"

	"github.com/steipete/discrawl/internal/store"
)

func (r *runtime) runTUI(args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	limit := fs.Int("limit", 200, "")
	includeEmpty := fs.Bool("include-empty", false, "")
	dm := fs.Bool("dm", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("tui takes flags only"))
	}
	if *limit <= 0 {
		return usageErr(errors.New("tui --limit must be positive"))
	}
	guildIDs, err := directMessageGuildScope(*dm, *guildFlag, *guildsFlag)
	if err != nil {
		return usageErr(err)
	}
	rows, err := r.store.ListMessages(r.ctx, store.MessageListOptions{
		GuildIDs:     guildIDs,
		Channel:      *channel,
		Author:       *author,
		Limit:        *limit,
		IncludeEmpty: *includeEmpty,
	})
	if err != nil {
		return err
	}
	return tui.Browse(r.ctx, tui.BrowseOptions{
		AppName:      "discrawl",
		Title:        "discrawl archive",
		EmptyMessage: "discrawl has no local messages yet",
		Rows:         discordTUIRows(rows),
		JSON:         r.json,
		Stdout:       r.stdout,
	})
}

func discordTUIRows(rows []store.MessageRow) []tui.Row {
	items := make([]tui.Row, 0, len(rows))
	for _, row := range rows {
		title := strings.TrimSpace(row.Content)
		if title == "" {
			title = row.MessageID
		}
		tags := []string{row.GuildID, row.ChannelID}
		if row.GuildID == "@me" {
			tags = append(tags, "dm")
		}
		items = append(items, tui.Row{
			Source:    "discord",
			Kind:      "message",
			ID:        row.MessageID,
			ParentID:  row.ReplyToMessage,
			Scope:     row.GuildID,
			Container: firstNonEmpty(row.ChannelName, row.ChannelID),
			Author:    firstNonEmpty(row.AuthorName, row.AuthorID),
			Title:     title,
			Text:      row.Content,
			CreatedAt: formatTime(row.CreatedAt),
			Tags:      tags,
			Fields: map[string]string{
				"channel_id": row.ChannelID,
				"author_id":  row.AuthorID,
			},
		})
	}
	return items
}
