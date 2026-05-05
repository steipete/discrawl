package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/report"
)

func (r *runtime) runDigest(args []string) error {
	fs := flag.NewFlagSet("digest", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	since := fs.String("since", "7d", "")
	guild := fs.String("guild", "", "")
	channel := fs.String("channel", "", "")
	topN := fs.Int("top-n", 3, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(errors.New("digest takes no positional arguments"))
	}

	lookback, err := parseLookback(*since)
	if err != nil {
		return usageErr(fmt.Errorf("parse --since: %w", err))
	}
	guildID := strings.TrimSpace(*guild)
	if guildID == "" {
		guildID = r.cfg.EffectiveDefaultGuildID()
	}

	digest, err := report.BuildDigest(r.ctx, r.store, report.DigestOptions{
		Since:   lookback,
		GuildID: guildID,
		Channel: *channel,
		TopN:    *topN,
	})
	if err != nil {
		return err
	}
	return r.print(digest)
}

func parseLookback(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("empty duration")
	}
	if daysValue, ok := strings.CutSuffix(value, "d"); ok {
		days, err := strconv.Atoi(daysValue)
		if err != nil {
			return 0, fmt.Errorf("invalid day count: %w", err)
		}
		if days < 0 {
			return 0, errors.New("negative duration")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, errors.New("negative duration")
	}
	return d, nil
}
