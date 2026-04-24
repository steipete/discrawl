package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/discord"
	"github.com/steipete/discrawl/internal/discorddesktop"
	"github.com/steipete/discrawl/internal/embed"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/syncer"
)

type syncSources struct {
	name    string
	discord bool
	wiretap bool
}

type syncRunStats struct {
	Source  string                `json:"source"`
	Discord *syncer.SyncStats     `json:"discord,omitempty"`
	Wiretap *discorddesktop.Stats `json:"wiretap,omitempty"`
}

func (r *runtime) runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fromOpenClaw := fs.String("from-openclaw", "", "")
	account := fs.String("account", "", "")
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
	if *account != "" {
		cfg.Discord.Account = *account
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
	all := fs.Bool("all", false, "")
	allChannels := fs.Bool("all-channels", false, "")
	since := fs.String("since", "", "")
	channels := fs.String("channels", "", "")
	concurrency := fs.Int("concurrency", r.cfg.Sync.Concurrency, "")
	source := fs.String("source", r.cfg.Sync.Source, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	skipMembers := fs.Bool("skip-members", false, "")
	latestOnly := fs.Bool("latest-only", false, "")
	guildsFlag := fs.String("guilds", "", "")
	guildFlag := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	sources, err := parseSyncSources(*source)
	if err != nil {
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
	guildIDs, err := r.resolveSyncGuildsAll(*guildFlag, *guildsFlag, *all)
	if err != nil {
		return usageErr(err)
	}
	defaultLatest := !*full && !*allChannels && *since == "" && *channels == ""
	latestMode := *latestOnly || defaultLatest
	opts := syncer.SyncOptions{
		Full:        *full,
		GuildIDs:    guildIDs,
		ChannelIDs:  csvList(*channels),
		Concurrency: *concurrency,
		Since:       sinceTime,
		Embeddings:  *withEmbeddings,
		SkipMembers: *skipMembers || defaultLatest,
		LatestOnly:  latestMode,
	}
	var apiStats *syncer.SyncStats
	if sources.discord {
		shouldClose := r.client == nil
		if err := r.ensureDiscordServices(); err != nil {
			return err
		}
		if shouldClose && r.client != nil {
			defer func() { _ = r.client.Close() }()
		}
		stats, err := r.syncer.Sync(r.ctx, opts)
		if err != nil {
			return err
		}
		apiStats = &stats
	}
	var wiretapStats *discorddesktop.Stats
	if sources.wiretap {
		stats, err := discorddesktop.Import(r.ctx, r.store, discorddesktop.Options{
			Path:         r.cfg.Desktop.Path,
			MaxFileBytes: r.cfg.Desktop.MaxFileBytes,
			Now:          r.now,
		})
		if err != nil {
			return err
		}
		wiretapStats = &stats
	}
	if sources.discord && !sources.wiretap {
		return r.print(*apiStats)
	}
	if sources.wiretap && !sources.discord {
		return r.print(*wiretapStats)
	}
	return r.print(syncRunStats{Source: sources.name, Discord: apiStats, Wiretap: wiretapStats})
}

func parseSyncSources(raw string) (syncSources, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		normalized = "both"
	}
	normalized = strings.ReplaceAll(normalized, "+", ",")
	parts := strings.Split(normalized, ",")
	out := syncSources{name: normalized}
	for _, part := range parts {
		switch strings.TrimSpace(part) {
		case "", "both", "all":
			out.discord = true
			out.wiretap = true
		case "discord", "api", "bot", "key":
			out.discord = true
		case "wiretap", "desktop", "cache":
			out.wiretap = true
		default:
			return syncSources{}, fmt.Errorf("invalid --source %q; use both, discord, or wiretap", raw)
		}
	}
	switch {
	case out.discord && out.wiretap:
		out.name = "both"
	case out.discord:
		out.name = "discord"
	case out.wiretap:
		out.name = "wiretap"
	default:
		return syncSources{}, fmt.Errorf("invalid --source %q; use both, discord, or wiretap", raw)
	}
	return out, nil
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

func (r *runtime) runWiretap(args []string) error {
	fs := flag.NewFlagSet("wiretap", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("path", r.cfg.Desktop.Path, "")
	maxFileBytes := fs.Int64("max-file-bytes", r.cfg.Desktop.MaxFileBytes, "")
	dryRun := fs.Bool("dry-run", false, "")
	watchEvery := fs.Duration("watch-every", 0, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("wiretap takes flags only"))
	}
	if *maxFileBytes <= 0 {
		return usageErr(fmt.Errorf("--max-file-bytes must be positive"))
	}
	runOnce := func(ctx context.Context) error {
		stats, err := discorddesktop.Import(ctx, r.store, discorddesktop.Options{
			Path:         *path,
			MaxFileBytes: *maxFileBytes,
			DryRun:       *dryRun,
			Now:          r.now,
		})
		if err != nil {
			return err
		}
		return r.print(stats)
	}
	if *watchEvery <= 0 {
		return runOnce(r.ctx)
	}
	if *watchEvery < time.Second {
		return usageErr(fmt.Errorf("--watch-every must be at least 1s"))
	}
	ctx, stop := signal.NotifyContext(r.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(*watchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := runOnce(ctx); err != nil {
				return err
			}
		}
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

func (r *runtime) runEmbed(args []string) error {
	fs := flag.NewFlagSet("embed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := fs.Int("limit", store.DefaultEmbedLimit(), "")
	batchSize := fs.Int("batch-size", r.cfg.Search.Embeddings.BatchSize, "")
	rebuild := fs.Bool("rebuild", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("embed takes no positional arguments"))
	}
	if *limit <= 0 {
		return usageErr(fmt.Errorf("--limit must be positive"))
	}
	if *batchSize <= 0 {
		return usageErr(fmt.Errorf("--batch-size must be positive"))
	}
	if !r.cfg.Search.Embeddings.Enabled {
		return usageErr(fmt.Errorf("embeddings are disabled in config"))
	}
	providerFactory := r.newEmbed
	if providerFactory == nil {
		providerFactory = func(cfg config.EmbeddingsConfig) (embed.Provider, error) {
			return embed.NewProvider(cfg)
		}
	}
	provider, err := providerFactory(r.cfg.Search.Embeddings)
	if err != nil {
		return configErr(err)
	}
	opts := store.EmbeddingDrainOptions{
		Provider:      r.cfg.Search.Embeddings.Provider,
		Model:         r.cfg.Search.Embeddings.Model,
		InputVersion:  store.EmbeddingInputVersion,
		Limit:         *limit,
		BatchSize:     *batchSize,
		MaxInputChars: r.cfg.Search.Embeddings.MaxInputChars,
		Now:           r.now,
	}
	requeued := 0
	if *rebuild {
		requeued, err = r.store.RequeueAllEmbeddingJobs(r.ctx, opts)
		if err != nil {
			return err
		}
	}
	stats, err := r.store.DrainEmbeddingJobs(r.ctx, provider, opts)
	if err != nil {
		return err
	}
	stats.Requeued = requeued
	return r.print(stats)
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
	if cfg.ShareEnabled() {
		report["share_remote"] = cfg.Share.Remote
		report["share_repo_path"] = cfg.Share.RepoPath
		report["share_auto_update"] = cfg.Share.AutoUpdate
		report["share_stale_after"] = cfg.Share.StaleAfter
	}
	if cfg.Search.Embeddings.Enabled {
		check := embed.CheckProvider(r.ctx, cfg.Search.Embeddings)
		report["embeddings"] = check.Status
		report["embeddings_provider"] = check.Provider
		report["embeddings_model"] = check.Model
		report["embeddings_base_url"] = check.BaseURL
		if check.Probed {
			report["embeddings_probe"] = "ok"
		}
		if check.Warning != "" {
			report["embeddings_warning"] = check.Warning
		}
	} else {
		report["embeddings"] = "disabled"
	}
	token, err := config.ResolveDiscordToken(cfg)
	if err != nil {
		if cfg.Discord.TokenSource == "none" && cfg.ShareEnabled() {
			report["discord_token"] = "disabled (git share mode)"
		} else {
			report["discord_token"] = err.Error()
		}
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
			ftsErr := db.CheckMessageFTS(r.ctx)
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
