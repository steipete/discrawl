package cli

import (
	"errors"
	"flag"
	"strings"

	"github.com/vincentkoc/crawlkit/tui"

	"github.com/steipete/discrawl/internal/store"
)

func (r *runtime) runTUI(args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(r.stderr)
	if hasHelpArg(args) {
		fs.SetOutput(r.stdout)
	}
	channel := fs.String("channel", "", "channel id")
	author := fs.String("author", "", "author/user id")
	limit := fs.Int("limit", 200, "row limit")
	includeEmpty := fs.Bool("include-empty", false, "include empty messages")
	dm := fs.Bool("dm", false, "browse direct messages")
	guildsFlag := fs.String("guilds", "", "comma-separated guild ids")
	guildFlag := fs.String("guild", "", "guild id")
	jsonOut := fs.Bool("json", false, "write browser rows as JSON")
	if len(args) == 1 && args[0] == "help" {
		fs.Usage()
		return nil
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return usageErr(err)
	}
	if *jsonOut {
		r.json = true
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("tui takes flags only"))
	}
	if *limit <= 0 {
		return usageErr(errors.New("tui --limit must be positive"))
	}
	guildIDs, err := r.resolveTUIGuilds(*dm, *guildFlag, *guildsFlag)
	if err != nil {
		return usageErr(err)
	}
	if r.store == nil {
		return tui.Browse(r.ctx, tui.BrowseOptions{
			AppName:        "discrawl",
			Title:          "discrawl archive",
			EmptyMessage:   "discrawl has no local messages yet",
			JSON:           r.json,
			Layout:         tui.LayoutChat,
			SourceKind:     r.archiveSourceKind(),
			SourceLocation: r.archiveSourceLocation(),
			Stdout:         r.stdout,
		})
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
		AppName:        "discrawl",
		Title:          "discrawl archive",
		EmptyMessage:   "discrawl has no local messages yet",
		Rows:           discordTUIRows(rows),
		JSON:           r.json,
		Layout:         tui.LayoutChat,
		SourceKind:     r.archiveSourceKind(),
		SourceLocation: r.archiveSourceLocation(),
		Stdout:         r.stdout,
	})
}

func (r *runtime) resolveTUIGuilds(dm bool, guild, guilds string) ([]string, error) {
	guildIDs, err := directMessageGuildScope(dm, guild, guilds)
	if err != nil || dm || len(guildIDs) > 0 {
		return guildIDs, err
	}
	if defaultGuild := r.cfg.EffectiveDefaultGuildID(); defaultGuild != "" {
		return []string{defaultGuild}, nil
	}
	return nil, nil
}

func (r *runtime) archiveSourceKind() string {
	if strings.TrimSpace(r.cfg.Share.Remote) != "" {
		return tui.SourceRemote
	}
	return tui.SourceLocal
}

func (r *runtime) archiveSourceLocation() string {
	if strings.TrimSpace(r.cfg.Share.Remote) != "" {
		return r.cfg.Share.Remote
	}
	return r.cfg.DBPath
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
		if row.Source != "" {
			tags = append(tags, row.Source)
		}
		items = append(items, tui.Row{
			Source:    "discord",
			Kind:      "message",
			ID:        row.MessageID,
			ParentID:  row.ReplyToMessage,
			Scope:     discordScopeLabel(row),
			Container: discordContainerLabel(row),
			Author:    discordAuthorLabel(row),
			Title:     title,
			Text:      row.Content,
			CreatedAt: formatTime(row.CreatedAt),
			Tags:      tags,
			Fields: map[string]string{
				"attachments": boolString(row.HasAttachments),
				"author_id":   row.AuthorID,
				"channel_id":  row.ChannelID,
				"guild_id":    row.GuildID,
				"pinned":      boolString(row.Pinned),
				"reply_to":    row.ReplyToMessage,
				"source":      row.Source,
			},
		})
	}
	return items
}

func discordScopeLabel(row store.MessageRow) string {
	if row.GuildID == "@me" {
		return "Direct messages"
	}
	return firstNonEmpty(row.GuildName, row.GuildID)
}

func discordContainerLabel(row store.MessageRow) string {
	if row.GuildID == "@me" {
		return firstNonEmpty(row.ChannelName, "DM "+compactDiscordID(row.ChannelID))
	}
	return firstNonEmpty(row.ChannelName, row.ChannelID)
}

func discordAuthorLabel(row store.MessageRow) string {
	if name := strings.TrimSpace(row.AuthorName); name != "" {
		return name
	}
	if id := strings.TrimSpace(row.AuthorID); id != "" {
		return "user:" + compactDiscordID(id)
	}
	return ""
}

func compactDiscordID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) <= 10 {
		return id
	}
	return id[:6] + "..." + id[len(id)-4:]
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return ""
}
