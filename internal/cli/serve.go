package cli

import (
	"flag"
	"fmt"

	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/web"
)

func (r *runtime) runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(r.stderr)
	port := fs.Int("port", 0, "HTTP listen port (default: from config, fallback 8080)")
	host := fs.String("host", "", "HTTP listen host (default: from config, fallback localhost)")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	cfg, err := config.Load(r.configPath)
	if err != nil {
		return configErr(err)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}

	// Generate and save session secret on first init if missing.
	if err := config.EnsureSessionSecret(r.configPath, &cfg); err != nil {
		return configErr(fmt.Errorf("session secret: %w", err))
	}

	dataDir, err := config.ExpandPath(cfg.EffectiveDataDir())
	if err != nil {
		return configErr(fmt.Errorf("data dir: %w", err))
	}

	registry, err := store.NewRegistry(r.ctx, store.RegistryConfig{
		DataDir: dataDir,
	})
	if err != nil {
		return dbErr(fmt.Errorf("open registry: %w", err))
	}
	defer func() { _ = registry.Close() }()

	listenHost := cfg.Web.Host
	if *host != "" {
		listenHost = *host
	}
	listenPort := cfg.Web.Port
	if *port != 0 {
		listenPort = *port
	}

	srv := web.NewServer(cfg, registry, r.logger)
	return srv.ListenAndServe(r.ctx, listenHost, listenPort)
}
