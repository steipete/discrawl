package cli

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/share"
)

type shareUpdateMode string

const (
	shareUpdateConfigured shareUpdateMode = "configured"
	shareUpdateAuto       shareUpdateMode = "auto"
	shareUpdateNever      shareUpdateMode = "never"
	shareUpdateForce      shareUpdateMode = "force"
)

func boolShareUpdateMode(enabled bool) shareUpdateMode {
	if enabled {
		return shareUpdateConfigured
	}
	return shareUpdateNever
}

func parseShareUpdateMode(raw string) (shareUpdateMode, error) {
	switch shareUpdateMode(strings.ToLower(strings.TrimSpace(raw))) {
	case "", shareUpdateAuto:
		return shareUpdateAuto, nil
	case shareUpdateNever:
		return shareUpdateNever, nil
	case shareUpdateForce:
		return shareUpdateForce, nil
	default:
		return "", fmt.Errorf("invalid --update %q; use auto, never, or force", raw)
	}
}

func syncShareUpdateMode(args []string) (shareUpdateMode, error) {
	mode := shareUpdateNever
	sawNoUpdate := false
	sawUpdate := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--no-update":
			sawNoUpdate = true
			mode = shareUpdateNever
		case arg == "--update":
			if i+1 >= len(args) {
				return "", errors.New("--update requires auto, never, or force")
			}
			parsed, err := parseShareUpdateMode(args[i+1])
			if err != nil {
				return "", err
			}
			sawUpdate = true
			mode = parsed
			i++
		case strings.HasPrefix(arg, "--update="):
			parsed, err := parseShareUpdateMode(strings.TrimPrefix(arg, "--update="))
			if err != nil {
				return "", err
			}
			sawUpdate = true
			mode = parsed
		}
	}
	if sawNoUpdate && sawUpdate && mode != shareUpdateNever {
		return "", errors.New("use either --no-update or --update, not both")
	}
	return mode, nil
}

func (r *runtime) shareProgress(progress share.ImportProgress) {
	if progress.Phase == "" {
		return
	}
	phase := "share " + progress.Phase
	if progress.Table != "" {
		phase += " " + progress.Table
	}
	if progress.File != "" {
		phase += " " + progress.File
	}
	r.setSyncLockPhase(phase)
	attrs := []any{"phase", progress.Phase}
	if progress.Table != "" {
		attrs = append(attrs, "table", progress.Table)
	}
	if progress.Rows != 0 {
		attrs = append(attrs, "rows", progress.Rows)
	}
	if progress.TotalRows != 0 {
		attrs = append(attrs, "total_rows", progress.TotalRows)
	}
	if progress.File != "" {
		attrs = append(attrs, "file", progress.File, "file_index", progress.FileIndex, "file_count", progress.FileCount)
	}
	r.logger.Info("share import progress", attrs...)
}

func (r *runtime) nowUTC() time.Time {
	if r.now != nil {
		return r.now().UTC()
	}
	return time.Now().UTC()
}
