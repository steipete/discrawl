package cli

import (
	"flag"
	"fmt"
	"io"

	"github.com/steipete/discrawl/internal/report"
)

func (r *runtime) runReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	readmePath := fs.String("readme", "", "")
	ai := fs.Bool("ai", false, "")
	aiModel := fs.String("ai-model", "", "")
	aiKeyEnv := fs.String("ai-key-env", "", "")
	aiBaseURL := fs.String("ai-base-url", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("report takes no positional arguments"))
	}
	activity, err := report.Build(r.ctx, r.store, report.Options{
		AI: report.AIOptions{
			Enabled:   *ai,
			Model:     *aiModel,
			APIKeyEnv: *aiKeyEnv,
			BaseURL:   *aiBaseURL,
		},
	})
	if err != nil {
		return err
	}
	section, err := report.RenderMarkdown(activity)
	if err != nil {
		return err
	}
	if *readmePath != "" {
		if err := report.WriteReadme(*readmePath, section); err != nil {
			return err
		}
		return r.print(map[string]any{
			"readme":            *readmePath,
			"generated_at":      activity.GeneratedAt,
			"latest_message_at": activity.LatestMessageAt,
			"ai":                *ai,
		})
	}
	_, err = io.WriteString(r.stdout, section)
	return err
}
