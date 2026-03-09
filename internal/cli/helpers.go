package cli

import (
	"flag"
	"strings"
	"time"
)

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

func flagPassed(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
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

func firstNonEmpty(items ...string) string {
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			return item
		}
	}
	return ""
}
