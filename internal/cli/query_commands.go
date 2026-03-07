package cli

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/steipete/discrawl/internal/store"
)

func (r *runtime) runSearch(args []string) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mode := fs.String("mode", r.cfg.Search.DefaultMode, "")
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	limit := fs.Int("limit", 20, "")
	includeEmpty := fs.Bool("include-empty", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("search requires a query"))
	}
	_ = mode
	results, err := r.store.SearchMessages(r.ctx, store.SearchOptions{
		Query:        fs.Arg(0),
		GuildIDs:     r.resolveSearchGuilds(*guildFlag, *guildsFlag),
		Channel:      *channel,
		Author:       *author,
		Limit:        *limit,
		IncludeEmpty: *includeEmpty,
	})
	if err != nil {
		return err
	}
	return r.print(results)
}

func (r *runtime) runSQL(args []string) error {
	fs := flag.NewFlagSet("sql", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	unsafe := fs.Bool("unsafe", false, "")
	confirm := fs.Bool("confirm", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *confirm && !*unsafe {
		return usageErr(fmt.Errorf("--confirm requires --unsafe"))
	}

	var query string
	rest := fs.Args()
	if len(rest) == 0 || rest[0] == "-" {
		body, err := io.ReadAll(bufio.NewReader(os.Stdin))
		if err != nil {
			return err
		}
		query = string(body)
	} else {
		query = strings.Join(rest, " ")
	}

	if !*unsafe {
		cols, rows, err := r.store.ReadOnlyQuery(r.ctx, query)
		if err != nil {
			return err
		}
		if r.json {
			return r.print(map[string]any{"columns": cols, "rows": rows})
		}
		return printRows(r.stdout, cols, rows)
	}
	if !*confirm {
		return usageErr(fmt.Errorf("--unsafe requires --confirm"))
	}

	if store.IsReadOnlySQL(query) {
		cols, rows, err := r.store.Query(r.ctx, query)
		if err != nil {
			return err
		}
		if r.json {
			return r.print(map[string]any{"columns": cols, "rows": rows})
		}
		return printRows(r.stdout, cols, rows)
	}

	affected, err := r.store.Exec(r.ctx, query)
	if err != nil {
		return err
	}
	return r.print(map[string]any{"rows_affected": affected})
}

func (r *runtime) runMembers(args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("members requires a subcommand"))
	}
	switch args[0] {
	case "list":
		rows, err := r.store.Members(r.ctx, r.cfg.EffectiveDefaultGuildID(), "", 500)
		if err != nil {
			return err
		}
		return r.print(rows)
	case "show":
		if len(args) < 2 {
			return usageErr(fmt.Errorf("members show requires a user id"))
		}
		rows, err := r.store.MemberByID(r.ctx, args[1])
		if err != nil {
			return err
		}
		return r.print(rows)
	case "search":
		if len(args) < 2 {
			return usageErr(fmt.Errorf("members search requires a query"))
		}
		rows, err := r.store.Members(r.ctx, "", strings.Join(args[1:], " "), 100)
		if err != nil {
			return err
		}
		return r.print(rows)
	default:
		return usageErr(fmt.Errorf("unknown members subcommand %q", args[0]))
	}
}

func (r *runtime) runChannels(args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("channels requires a subcommand"))
	}
	rows, err := r.store.Channels(r.ctx, "")
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		return r.print(rows)
	case "show":
		if len(args) < 2 {
			return usageErr(fmt.Errorf("channels show requires a channel id"))
		}
		filtered := make([]store.ChannelRow, 0, 1)
		for _, row := range rows {
			if row.ID == args[1] {
				filtered = append(filtered, row)
			}
		}
		return r.print(filtered)
	default:
		return usageErr(fmt.Errorf("unknown channels subcommand %q", args[0]))
	}
}
