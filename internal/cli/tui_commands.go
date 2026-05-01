package cli

import (
	"errors"
	"flag"
	"fmt"
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
	items := discordTUIItems(rows)
	if r.json {
		return r.print(items)
	}
	if err := tui.Run(r.ctx, tui.Options{
		Title:        "discrawl archive",
		EmptyMessage: "discrawl has no local messages yet",
		Items:        items,
	}); err != nil {
		if errors.Is(err, tui.ErrNotTerminal) {
			return fmt.Errorf("%w; run discrawl tui from a TTY or pass --json", err)
		}
		return err
	}
	return nil
}

func discordTUIItems(rows []store.MessageRow) []tui.Item {
	items := make([]tui.Item, 0, len(rows))
	for _, row := range rows {
		title := strings.TrimSpace(row.Content)
		if title == "" {
			title = row.MessageID
		}
		tags := []string{"message", row.GuildID, row.ChannelID}
		if row.GuildID == "@me" {
			tags = append(tags, "dm")
		}
		items = append(items, tui.Item{
			Title:    title,
			Subtitle: strings.TrimSpace(strings.Join([]string{row.GuildID, row.ChannelName, row.AuthorName, formatTime(row.CreatedAt)}, " ")),
			Detail: strings.TrimSpace(strings.Join([]string{
				"id=" + row.MessageID,
				"channel=" + row.ChannelID,
				"author=" + row.AuthorID,
				"reply_to=" + row.ReplyToMessage,
			}, "\n")),
			Tags: tags,
		})
	}
	return items
}
