package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/steipete/discrawl/internal/report"
)

func (r *runtime) runAnalytics(args []string) error {
	if len(args) == 0 {
		printAnalyticsUsage(r.stdout)
		return nil
	}

	subcommand := strings.TrimSpace(args[0])
	subArgs := args[1:]
	switch subcommand {
	case "quiet":
		return r.withLocalStoreDefaultLocked(true, true, func() error {
			return r.runAnalyticsQuiet(subArgs)
		})
	case "trends":
		return r.withLocalStoreDefaultLocked(true, true, func() error {
			return r.runAnalyticsTrends(subArgs)
		})
	default:
		return usageErr(fmt.Errorf("unknown analytics subcommand %q", subcommand))
	}
}

func printAnalyticsUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage: discrawl analytics <subcommand> [flags]")
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "Subcommands:")
	_, _ = fmt.Fprintln(w, "  quiet   Channels with no activity in the lookback window.")
	_, _ = fmt.Fprintln(w, "  trends  Week-over-week message counts per channel.")
}

func (r *runtime) runAnalyticsQuiet(args []string) error {
	fs := flag.NewFlagSet("analytics quiet", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	since := fs.String("since", "30d", "")
	guild := fs.String("guild", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("analytics quiet takes no positional arguments"))
	}

	lookback, err := parseLookback(*since)
	if err != nil {
		return usageErr(fmt.Errorf("parse --since: %w", err))
	}
	guildID := strings.TrimSpace(*guild)
	if guildID == "" {
		guildID = r.cfg.EffectiveDefaultGuildID()
	}

	quiet, err := report.BuildQuiet(r.ctx, r.store, report.QuietOptions{
		Since:   lookback,
		GuildID: guildID,
	})
	if err != nil {
		return err
	}
	return r.print(quiet)
}

func (r *runtime) runAnalyticsTrends(args []string) error {
	fs := flag.NewFlagSet("analytics trends", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	weeks := fs.Int("weeks", 8, "")
	guild := fs.String("guild", "", "")
	channel := fs.String("channel", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("analytics trends takes no positional arguments"))
	}
	if *weeks < 0 {
		return usageErr(errors.New("--weeks must be zero or greater"))
	}

	guildID := strings.TrimSpace(*guild)
	if guildID == "" {
		guildID = r.cfg.EffectiveDefaultGuildID()
	}

	trends, err := report.BuildTrends(r.ctx, r.store, report.TrendsOptions{
		Weeks:   *weeks,
		GuildID: guildID,
		Channel: *channel,
	})
	if err != nil {
		return err
	}
	return r.print(trends)
}
