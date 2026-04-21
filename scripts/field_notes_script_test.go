package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscordBackupFieldNotesScriptUsesOpenClawAndReplacesReadmeBlock(t *testing.T) {
	root, err := filepath.Abs("..")
	require.NoError(t, err)

	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	require.NoError(t, os.Mkdir(binDir, 0o755))
	openclawCalls := filepath.Join(tmp, "openclaw-calls.txt")
	writeExecutable(t, filepath.Join(binDir, "go"), `#!/usr/bin/env bash
printf '[{"ok":true}]'
`)
	writeExecutable(t, filepath.Join(binDir, "jq"), `#!/usr/bin/env bash
if [ "${1:-}" = "-r" ]; then
  cat <<'MD'
### Field Notes

Last generated: 2026-04-21 07:00 UTC

#### What People Love
- Fast Git sync.

#### What People Complain About
- Query pain maps to PR #42.

#### Best PR To Watch
- PR #42 because it closes the noisy issue cluster.
MD
else
  cat
fi
`)
	writeExecutable(t, filepath.Join(binDir, "gh"), `#!/usr/bin/env bash
printf '[{"number":42,"title":"Improve query sync","state":"open","url":"https://github.com/openclaw/openclaw/pull/42"}]'
`)
	writeExecutable(t, filepath.Join(binDir, "openclaw"), `#!/usr/bin/env bash
printf '%s\n' "$*" > "$OPENCLAW_CALLS"
printf '{"payloads":[{"text":"unused"}]}'
`)

	backupRepo := filepath.Join(tmp, "backup")
	require.NoError(t, os.Mkdir(backupRepo, 0o755))
	readmePath := filepath.Join(backupRepo, "README.md")
	require.NoError(t, os.WriteFile(readmePath, []byte(`# Discord Backup

<!-- discrawl-report:start -->
## Discord Activity Report
<!-- discrawl-report:end -->

<!-- discrawl-field-notes:start -->
old notes
<!-- discrawl-field-notes:end -->
`), 0o644))

	cmd := exec.Command("bash", "scripts/discord-backup-field-notes.sh", filepath.Join(tmp, "config.toml"), backupRepo)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"OPENCLAW_BIN="+filepath.Join(binDir, "openclaw"),
		"OPENCLAW_CALLS="+openclawCalls,
		"GH_TOKEN=test-token",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	readme, err := os.ReadFile(readmePath)
	require.NoError(t, err)
	text := string(readme)
	require.Contains(t, text, "#### What People Love")
	require.Contains(t, text, "#### What People Complain About")
	require.Contains(t, text, "#### Best PR To Watch")
	require.Contains(t, text, "PR #42")
	require.NotContains(t, text, "old notes")

	calls, err := os.ReadFile(openclawCalls)
	require.NoError(t, err)
	require.Contains(t, string(calls), "agent --local")
	require.Contains(t, string(calls), "--thinking low")
	require.NotContains(t, text, "GitHub posted the most")
}

func TestDiscordBackupFieldNotesScriptFallsBackWhenOpenClawFails(t *testing.T) {
	root, err := filepath.Abs("..")
	require.NoError(t, err)

	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	require.NoError(t, os.Mkdir(binDir, 0o755))
	writeExecutable(t, filepath.Join(binDir, "go"), `#!/usr/bin/env bash
printf '[{"latest_message":"2026-04-21T07:10:46Z","channel":"general","matches":3}]'
`)
	writeExecutable(t, filepath.Join(binDir, "jq"), `#!/usr/bin/env bash
if [ "${1:-}" = "-c" ]; then
  cat
  exit 0
fi
if [ "${1:-}" = "-r" ]; then
  expr="${2:-}"
  cat >/dev/null
  case "$expr" in
    *latest_message*) printf '2026-04-21T07:10:46Z\n' ;;
    *positive*) printf -- '- general: 3 positive mentions in the last 30 days.\n' ;;
    *Issue*) printf -- '- Issue #7 (open): [Fix query pain](https://github.com/openclaw/openclaw/issues/7)\n' ;;
    *PR*) printf -- '- PR #9: [Improve sync](https://github.com/openclaw/openclaw/pull/9) looks like the highest-leverage recent PR because it is active in the same window as the complaint cluster.\n' ;;
    *complaint*) printf -- '- bugs: 2 complaint-flavored mentions in the last 30 days; compare this with the issue/PR cluster below.\n' ;;
    *) cat ;;
  esac
fi
`)
	writeExecutable(t, filepath.Join(binDir, "gh"), `#!/usr/bin/env bash
printf '[{"number":9,"title":"Improve sync","state":"open","url":"https://github.com/openclaw/openclaw/pull/9"}]'
`)
	writeExecutable(t, filepath.Join(binDir, "openclaw"), `#!/usr/bin/env bash
echo "context deadline exceeded" >&2
exit 1
`)

	backupRepo := filepath.Join(tmp, "backup")
	require.NoError(t, os.Mkdir(backupRepo, 0o755))
	readmePath := filepath.Join(backupRepo, "README.md")
	require.NoError(t, os.WriteFile(readmePath, []byte(`# Discord Backup

<!-- discrawl-report:start -->
## Discord Activity Report
<!-- discrawl-report:end -->
`), 0o644))

	cmd := exec.Command("bash", "scripts/discord-backup-field-notes.sh", filepath.Join(tmp, "config.toml"), backupRepo)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"OPENCLAW_BIN="+filepath.Join(binDir, "openclaw"),
		"GH_TOKEN=test-token",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "openclaw field notes failed")

	readme, err := os.ReadFile(readmePath)
	require.NoError(t, err)
	text := string(readme)
	require.Contains(t, text, "OpenClaw agent timed out")
	require.Contains(t, text, "#### What People Love")
	require.Contains(t, text, "#### What People Complain About")
	require.Contains(t, text, "Issue #7")
	require.Contains(t, text, "PR #9")
	require.NotContains(t, text, "GitHub posted the most")
}

func writeExecutable(t *testing.T, path string, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(body), 0o755))
}
