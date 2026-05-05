package cli

import (
	"errors"
	"flag"
	"io"
	"os"

	"github.com/openclaw/discrawl/internal/config"
	"github.com/openclaw/discrawl/internal/report"
	"github.com/openclaw/discrawl/internal/share"
	"github.com/openclaw/discrawl/internal/store"
)

func (r *runtime) runPublish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoPath := fs.String("repo", r.cfg.Share.RepoPath, "")
	remote := fs.String("remote", r.cfg.Share.Remote, "")
	branch := fs.String("branch", r.cfg.Share.Branch, "")
	message := fs.String("message", "", "")
	readmePath := fs.String("readme", "", "")
	noCommit := fs.Bool("no-commit", false, "")
	push := fs.Bool("push", false, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("publish takes no positional arguments"))
	}
	opts, err := shareOptionsFromFlags(*repoPath, *remote, *branch)
	if err != nil {
		return err
	}
	if *withEmbeddings {
		applyEmbeddingShareOptions(&opts, r.cfg)
	}
	manifest, err := share.Export(r.ctx, r.store, opts)
	if err != nil {
		return err
	}
	if *readmePath != "" {
		activity, err := report.Build(r.ctx, r.store, report.Options{})
		if err != nil {
			return err
		}
		section, err := report.RenderMarkdown(activity)
		if err != nil {
			return err
		}
		if err := report.WriteReadme(*readmePath, section); err != nil {
			return err
		}
	}
	committed := false
	if !*noCommit {
		msg := *message
		if msg == "" {
			msg = "sync: discord archive"
		}
		committed, err = share.Commit(r.ctx, opts, msg)
		if err != nil {
			return err
		}
	}
	if *push {
		if err := share.Push(r.ctx, opts); err != nil {
			return err
		}
		if err := share.MarkImported(r.ctx, r.store, manifest); err != nil {
			return err
		}
	}
	return r.print(map[string]any{
		"repo_path":    opts.RepoPath,
		"remote":       opts.Remote,
		"generated_at": manifest.GeneratedAt,
		"tables":       manifest.Tables,
		"embeddings":   manifest.Embeddings,
		"readme":       *readmePath,
		"committed":    committed,
		"pushed":       *push,
	})
}

func (r *runtime) runSubscribe(args []string) error {
	fs := flag.NewFlagSet("subscribe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoPath := fs.String("repo", "", "")
	branch := fs.String("branch", "main", "")
	staleAfter := fs.String("stale-after", "15m", "")
	noAutoUpdate := fs.Bool("no-auto-update", false, "")
	noImport := fs.Bool("no-import", false, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(errors.New("subscribe requires one remote"))
	}
	remote := fs.Arg(0)
	cfg, err := loadConfigOrDefault(r.configPath)
	if err != nil {
		return err
	}
	if *repoPath != "" {
		cfg.Share.RepoPath = *repoPath
	}
	cfg.Share.Remote = remote
	cfg.Share.Branch = *branch
	cfg.Share.AutoUpdate = !*noAutoUpdate
	cfg.Share.StaleAfter = *staleAfter
	cfg.Discord.TokenSource = "none"
	if err := config.Write(r.configPath, cfg); err != nil {
		return configErr(err)
	}
	if *noImport {
		return r.print(map[string]any{"config_path": r.configPath, "remote": remote, "repo_path": cfg.Share.RepoPath})
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}
	dbPath, err := config.ExpandPath(cfg.DBPath)
	if err != nil {
		return configErr(err)
	}
	r.cfg = cfg
	return r.withSyncLock(func() error {
		s, err := store.Open(r.ctx, dbPath)
		if err != nil {
			return dbErr(err)
		}
		defer func() { _ = s.Close() }()
		expandedRepo, err := config.ExpandPath(cfg.Share.RepoPath)
		if err != nil {
			return configErr(err)
		}
		opts := share.Options{RepoPath: expandedRepo, Remote: cfg.Share.Remote, Branch: cfg.Share.Branch, Progress: r.shareProgress}
		if *withEmbeddings {
			applyEmbeddingShareOptions(&opts, cfg)
		}
		r.setSyncLockPhase("share pull")
		if err := share.Pull(r.ctx, opts); err != nil {
			return err
		}
		r.setSyncLockPhase("share import")
		manifest, imported, err := share.ImportIfChanged(r.ctx, s, opts)
		if err != nil {
			return err
		}
		return r.print(map[string]any{
			"config_path":  r.configPath,
			"repo_path":    opts.RepoPath,
			"remote":       opts.Remote,
			"generated_at": manifest.GeneratedAt,
			"tables":       manifest.Tables,
			"embeddings":   manifest.Embeddings,
			"imported":     imported,
		})
	})
}

func (r *runtime) runUpdate(args []string) error {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoPath := fs.String("repo", r.cfg.Share.RepoPath, "")
	remote := fs.String("remote", r.cfg.Share.Remote, "")
	branch := fs.String("branch", r.cfg.Share.Branch, "")
	withEmbeddings := fs.Bool("with-embeddings", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("update takes no positional arguments"))
	}
	opts, err := shareOptionsFromFlags(*repoPath, *remote, *branch)
	if err != nil {
		return err
	}
	opts.Progress = r.shareProgress
	if *withEmbeddings {
		applyEmbeddingShareOptions(&opts, r.cfg)
	}
	r.setSyncLockPhase("share pull")
	if err := share.Pull(r.ctx, opts); err != nil {
		return err
	}
	r.setSyncLockPhase("share import")
	manifest, imported, err := share.ImportIfChanged(r.ctx, r.store, opts)
	if err != nil {
		return err
	}
	return r.print(map[string]any{
		"repo_path":    opts.RepoPath,
		"remote":       opts.Remote,
		"generated_at": manifest.GeneratedAt,
		"tables":       manifest.Tables,
		"embeddings":   manifest.Embeddings,
		"imported":     imported,
	})
}

func shareOptionsFromFlags(repoPath, remote, branch string) (share.Options, error) {
	expandedRepo, err := config.ExpandPath(repoPath)
	if err != nil {
		return share.Options{}, configErr(err)
	}
	if remote == "" {
		return share.Options{}, configErr(errors.New("share remote is required"))
	}
	if branch == "" {
		branch = "main"
	}
	return share.Options{RepoPath: expandedRepo, Remote: remote, Branch: branch}, nil
}

func applyEmbeddingShareOptions(opts *share.Options, cfg config.Config) {
	opts.IncludeEmbeddings = true
	opts.EmbeddingProvider = cfg.Search.Embeddings.Provider
	opts.EmbeddingModel = cfg.Search.Embeddings.Model
	opts.EmbeddingInputVersion = store.EmbeddingInputVersion
}

func loadConfigOrDefault(path string) (config.Config, error) {
	cfg, err := config.Load(path)
	if err == nil {
		return cfg, nil
	}
	if !os.IsNotExist(err) {
		return config.Config{}, configErr(err)
	}
	cfg = config.Default()
	if err := cfg.Normalize(); err != nil {
		return config.Config{}, configErr(err)
	}
	return cfg, nil
}
