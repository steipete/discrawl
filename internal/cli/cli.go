package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/discord"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/syncer"
)

const version = "0.1.0"

type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string {
	return e.err.Error()
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var codeErr *cliError
	if errors.As(err, &codeErr) {
		return codeErr.code
	}
	return 1
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	global := flag.NewFlagSet("discrawl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	configPath := global.String("config", "", "")
	jsonOut := global.Bool("json", false, "")
	plainOut := global.Bool("plain", false, "")
	quiet := global.Bool("quiet", false, "")
	global.BoolVar(quiet, "q", false, "")
	verbose := global.Bool("verbose", false, "")
	global.BoolVar(verbose, "v", false, "")
	versionFlag := global.Bool("version", false, "")
	global.Bool("no-color", false, "")
	if err := global.Parse(args); err != nil {
		return usageErr(err)
	}
	if *versionFlag {
		_, _ = fmt.Fprintln(stdout, version)
		return nil
	}
	rest := global.Args()
	if len(rest) == 0 || rest[0] == "help" {
		printUsage(stdout)
		return nil
	}
	level := slog.LevelInfo
	if *quiet {
		level = slog.LevelError
	}
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	cmd := rest[0]
	runtime := &runtime{
		ctx:        ctx,
		configPath: config.ResolvePath(*configPath),
		stdout:     stdout,
		stderr:     stderr,
		json:       *jsonOut,
		plain:      *plainOut,
		logger:     logger,
	}
	switch cmd {
	case "init":
		return runtime.runInit(rest[1:])
	case "sync":
		return runtime.withServices(true, func() error { return runtime.runSync(rest[1:]) })
	case "tail":
		return runtime.withServices(true, func() error { return runtime.runTail(rest[1:]) })
	case "search":
		return runtime.withServices(false, func() error { return runtime.runSearch(rest[1:]) })
	case "sql":
		return runtime.withServices(false, func() error { return runtime.runSQL(rest[1:]) })
	case "members":
		return runtime.withServices(false, func() error { return runtime.runMembers(rest[1:]) })
	case "channels":
		return runtime.withServices(false, func() error { return runtime.runChannels(rest[1:]) })
	case "status":
		return runtime.withServices(false, func() error { return runtime.runStatus(rest[1:]) })
	case "doctor":
		return runtime.runDoctor(rest[1:])
	default:
		return usageErr(fmt.Errorf("unknown command %q", cmd))
	}
}

type runtime struct {
	ctx        context.Context
	configPath string
	cfg        config.Config
	stdout     io.Writer
	stderr     io.Writer
	json       bool
	plain      bool
	logger     *slog.Logger
	store      *store.Store
	client     discordClient
	syncer     syncService
	openStore  func(context.Context, string) (*store.Store, error)
	newDiscord func(config.Config) (discordClient, error)
	newSyncer  func(syncer.Client, *store.Store, *slog.Logger) syncService
}

type discordClient interface {
	syncer.Client
	Close() error
	Self(context.Context) (*discordgo.User, error)
	Guilds(context.Context) ([]*discordgo.UserGuild, error)
}

type syncService interface {
	DiscoverGuilds(context.Context) ([]*discordgo.UserGuild, error)
	Sync(context.Context, syncer.SyncOptions) (syncer.SyncStats, error)
	RunTail(context.Context, []string, time.Duration) error
}

func (r *runtime) withServices(withDiscord bool, fn func() error) error {
	cfg, err := config.Load(r.configPath)
	if err != nil {
		return configErr(err)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	storeFactory := r.openStore
	if storeFactory == nil {
		storeFactory = store.Open
	}
	r.store, err = storeFactory(r.ctx, dbPath)
	if err != nil {
		return dbErr(err)
	}
	defer func() { _ = r.store.Close() }()
	if withDiscord {
		discordFactory := r.newDiscord
		if discordFactory == nil {
			discordFactory = func(cfg config.Config) (discordClient, error) {
				token, err := config.ResolveDiscordToken(cfg)
				if err != nil {
					return nil, err
				}
				return discord.New(token.Token)
			}
		}
		r.client, err = discordFactory(cfg)
		if err != nil {
			return authErr(err)
		}
		defer func() { _ = r.client.Close() }()
		syncerFactory := r.newSyncer
		if syncerFactory == nil {
			syncerFactory = func(client syncer.Client, s *store.Store, logger *slog.Logger) syncService {
				return syncer.New(client, s, logger)
			}
		}
		r.syncer = syncerFactory(r.client, r.store, r.logger)
	}
	return fn()
}

func (r *runtime) runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fromOpenClaw := fs.String("from-openclaw", "", "")
	guildID := fs.String("guild", "", "")
	dbPath := fs.String("db", "", "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	cfg := config.Default()
	if *fromOpenClaw != "" {
		cfg.Discord.OpenClawConfig = *fromOpenClaw
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	cfg.Search.Embeddings.Enabled = *withEmbeddings
	if err := cfg.Normalize(); err != nil {
		return configErr(err)
	}
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		return authErr(err)
	}
	discordFactory := r.newDiscord
	if discordFactory == nil {
		discordFactory = func(cfg config.Config) (discordClient, error) {
			return discord.New(token.Token)
		}
	}
	client, err := discordFactory(cfg)
	if err != nil {
		return authErr(err)
	}
	defer func() { _ = client.Close() }()
	syncerFactory := r.newSyncer
	if syncerFactory == nil {
		syncerFactory = func(client syncer.Client, s *store.Store, logger *slog.Logger) syncService {
			return syncer.New(client, s, logger)
		}
	}
	syncerSvc := syncerFactory(client, nil, r.logger)
	guilds, err := syncerSvc.DiscoverGuilds(r.ctx)
	if err != nil {
		return authErr(err)
	}
	cfg.GuildIDs = make([]string, 0, len(guilds))
	for _, guild := range guilds {
		cfg.GuildIDs = append(cfg.GuildIDs, guild.ID)
	}
	if *guildID != "" {
		cfg.DefaultGuildID = *guildID
	} else if info, err := config.LoadOpenClawDiscord(cfg.Discord.OpenClawConfig, cfg.Discord.Account); err == nil {
		if len(info.GuildIDs) == 1 {
			cfg.DefaultGuildID = info.GuildIDs[0]
		}
	}
	if cfg.DefaultGuildID == "" && len(cfg.GuildIDs) == 1 {
		cfg.DefaultGuildID = cfg.GuildIDs[0]
	}
	if err := config.Write(r.configPath, cfg); err != nil {
		return configErr(err)
	}
	return r.print(map[string]any{
		"config_path":       r.configPath,
		"db_path":           cfg.DBPath,
		"token_source":      token.Source,
		"default_guild_id":  cfg.DefaultGuildID,
		"discovered_guilds": cfg.GuildIDs,
	})
}

func (r *runtime) runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	full := fs.Bool("full", false, "")
	since := fs.String("since", "", "")
	channels := fs.String("channels", "", "")
	concurrency := fs.Int("concurrency", r.cfg.Sync.Concurrency, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	var sinceTime time.Time
	if *since != "" {
		parsed, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return usageErr(fmt.Errorf("invalid --since: %w", err))
		}
		sinceTime = parsed
	}
	opts := syncer.SyncOptions{
		Full:        *full,
		GuildIDs:    r.resolveSyncGuilds(*guildFlag, *guildsFlag),
		ChannelIDs:  csvList(*channels),
		Concurrency: *concurrency,
		Since:       sinceTime,
		Embeddings:  *withEmbeddings,
	}
	stats, err := r.syncer.Sync(r.ctx, opts)
	if err != nil {
		return err
	}
	return r.print(stats)
}

func (r *runtime) runTail(args []string) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repairEvery := fs.Duration("repair-every", mustDuration(r.cfg.Sync.RepairEvery), "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	ctx, stop := signal.NotifyContext(r.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return r.syncer.RunTail(ctx, r.resolveSyncGuilds(*guildFlag, *guildsFlag), *repairEvery)
}

func (r *runtime) runSearch(args []string) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mode := fs.String("mode", r.cfg.Search.DefaultMode, "")
	channel := fs.String("channel", "", "")
	author := fs.String("author", "", "")
	limit := fs.Int("limit", 20, "")
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
		Query:    fs.Arg(0),
		GuildIDs: r.resolveSearchGuilds(*guildFlag, *guildsFlag),
		Channel:  *channel,
		Author:   *author,
		Limit:    *limit,
	})
	if err != nil {
		return err
	}
	return r.print(results)
}

func (r *runtime) runSQL(args []string) error {
	var query string
	if len(args) == 0 || args[0] == "-" {
		body, err := io.ReadAll(bufio.NewReader(os.Stdin))
		if err != nil {
			return err
		}
		query = string(body)
	} else {
		query = strings.Join(args, " ")
	}
	cols, rows, err := r.store.ReadOnlyQuery(r.ctx, query)
	if err != nil {
		return err
	}
	if r.json {
		return r.print(map[string]any{"columns": cols, "rows": rows})
	}
	w := tabwriter.NewWriter(r.stdout, 2, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, strings.Join(cols, "\t"))
	for _, row := range rows {
		_, _ = fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	return w.Flush()
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

func (r *runtime) runStatus(args []string) error {
	if len(args) != 0 {
		return usageErr(fmt.Errorf("status takes no arguments"))
	}
	dbPath, err := config.ExpandPath(r.cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	status, err := r.store.Status(r.ctx, dbPath, r.cfg.EffectiveDefaultGuildID())
	if err != nil {
		return err
	}
	return r.print(status)
}

func (r *runtime) runDoctor(args []string) error {
	if len(args) != 0 {
		return usageErr(fmt.Errorf("doctor takes no arguments"))
	}
	report := map[string]any{
		"config_path": r.configPath,
	}
	cfg, err := config.Load(r.configPath)
	if err != nil {
		report["config"] = err.Error()
		return r.print(report)
	}
	report["config"] = "ok"
	report["default_guild_id"] = cfg.EffectiveDefaultGuildID()
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		report["discord_token"] = err.Error()
	} else {
		report["discord_token"] = token.Source
		discordFactory := r.newDiscord
		if discordFactory == nil {
			discordFactory = func(cfg config.Config) (discordClient, error) {
				return discord.New(token.Token)
			}
		}
		client, clientErr := discordFactory(cfg)
		if clientErr == nil {
			defer func() { _ = client.Close() }()
			self, authErr := client.Self(r.ctx)
			if authErr != nil {
				report["discord_auth"] = authErr.Error()
			} else {
				report["discord_auth"] = "ok"
				report["bot_user_id"] = self.ID
			}
			guilds, guildErr := client.Guilds(r.ctx)
			if guildErr != nil {
				report["guild_access"] = guildErr.Error()
			} else {
				report["guild_access"] = len(guilds)
			}
		}
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err == nil {
		db, dbErr := store.Open(r.ctx, dbPath)
		if dbErr != nil {
			report["database"] = dbErr.Error()
		} else {
			report["database"] = "ok"
			_, _, ftsErr := db.ReadOnlyQuery(r.ctx, `select count(*) from message_fts`)
			if ftsErr != nil {
				report["fts"] = ftsErr.Error()
			} else {
				report["fts"] = "ok"
			}
			report["vector"] = "not configured"
			_ = db.Close()
		}
	}
	return r.print(report)
}

func (r *runtime) resolveSyncGuilds(guild, guilds string) []string {
	requested := append(csvList(guilds), strings.TrimSpace(guild))
	requested = csvList(strings.Join(requested, ","))
	if len(requested) > 0 {
		return requested
	}
	if defaultGuild := r.cfg.EffectiveDefaultGuildID(); defaultGuild != "" {
		return []string{defaultGuild}
	}
	return nil
}

func (r *runtime) resolveSearchGuilds(guild, guilds string) []string {
	requested := append(csvList(guilds), strings.TrimSpace(guild))
	return csvList(strings.Join(requested, ","))
}

func (r *runtime) print(value any) error {
	if r.json {
		enc := json.NewEncoder(r.stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(value)
	}
	if r.plain {
		switch v := value.(type) {
		case []store.SearchResult:
			for _, row := range v {
				_, _ = fmt.Fprintf(r.stdout, "%s\t%s\t%s\t%s\n", row.GuildID, row.ChannelID, row.AuthorID, row.Content)
			}
			return nil
		case []store.MemberRow:
			for _, row := range v {
				_, _ = fmt.Fprintf(r.stdout, "%s\t%s\t%s\n", row.GuildID, row.UserID, row.Username)
			}
			return nil
		case []store.ChannelRow:
			for _, row := range v {
				_, _ = fmt.Fprintf(r.stdout, "%s\t%s\t%s\t%s\n", row.GuildID, row.ID, row.Kind, row.Name)
			}
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

func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, `discrawl archives Discord guild data into local SQLite.

Usage:
  discrawl [global flags] <command> [args]

Commands:
  init
  sync
  tail
  search
  sql
  members
  channels
  status
  doctor
`)
}

func csvList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func mustDuration(raw string) time.Duration {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 6 * time.Hour
	}
	return d
}

func usageErr(err error) error {
	return &cliError{code: 2, err: err}
}

func configErr(err error) error {
	return &cliError{code: 3, err: err}
}

func authErr(err error) error {
	return &cliError{code: 4, err: err}
}

func dbErr(err error) error {
	return &cliError{code: 5, err: err}
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

func firstNonEmpty(items ...string) string {
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			return item
		}
	}
	return ""
}
