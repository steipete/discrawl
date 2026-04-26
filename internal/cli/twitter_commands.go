package cli

import (
	"flag"
	"fmt"
	"io"

	"github.com/steipete/discrawl/internal/twitterarchive"
)

func (r *runtime) runTwitter(args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("twitter requires subcommand: import"))
	}
	switch args[0] {
	case "import":
		return r.runTwitterImport(args[1:])
	default:
		return usageErr(fmt.Errorf("unknown twitter subcommand %q", args[0]))
	}
}

func (r *runtime) runTwitterImport(args []string) error {
	fs := flag.NewFlagSet("twitter import", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	archivePath := fs.String("archive", "", "")
	fs.StringVar(archivePath, "path", "", "")
	dryRun := fs.Bool("dry-run", false, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() > 0 {
		return usageErr(fmt.Errorf("twitter import takes flags only"))
	}
	stats, err := twitterarchive.Import(r.ctx, r.store, twitterarchive.Options{
		Path:   *archivePath,
		DryRun: *dryRun,
		Now:    r.now,
	})
	if err != nil {
		return err
	}
	return r.print(stats)
}
