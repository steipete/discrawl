package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/discord"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/syncer"
)

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
		_, _ = io.WriteString(stdout, version+"\n")
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
	runtime := &runtime{
		ctx:        ctx,
		configPath: config.ResolvePath(*configPath),
		stdout:     stdout,
		stderr:     stderr,
		json:       *jsonOut,
		plain:      *plainOut,
		logger:     slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level})),
	}
	return runtime.dispatch(rest)
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
	now        func() time.Time
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

type attachmentTextConfigurer interface {
	SetAttachmentTextEnabled(bool)
}

func (r *runtime) dispatch(rest []string) error {
	switch rest[0] {
	case "init":
		return r.runInit(rest[1:])
	case "sync":
		return r.withServices(true, func() error { return r.runSync(rest[1:]) })
	case "tail":
		return r.withServices(true, func() error { return r.runTail(rest[1:]) })
	case "search":
		return r.withServices(false, func() error { return r.runSearch(rest[1:]) })
	case "messages":
		return r.withServices(false, func() error { return r.runMessages(rest[1:]) })
	case "mentions":
		return r.withServices(false, func() error { return r.runMentions(rest[1:]) })
	case "sql":
		return r.withServices(false, func() error { return r.runSQL(rest[1:]) })
	case "members":
		return r.withServices(false, func() error { return r.runMembers(rest[1:]) })
	case "channels":
		return r.withServices(false, func() error { return r.runChannels(rest[1:]) })
	case "status":
		return r.withServices(false, func() error { return r.runStatus(rest[1:]) })
	case "doctor":
		return r.runDoctor(rest[1:])
	default:
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
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
		if configurable, ok := r.syncer.(attachmentTextConfigurer); ok {
			configurable.SetAttachmentTextEnabled(cfg.AttachmentTextEnabled())
		}
	}
	return fn()
}
