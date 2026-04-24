package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClawhubFieldNotesScriptWritesAgentMarkdownFile(t *testing.T) {
	root, err := filepath.Abs("..")
	require.NoError(t, err)

	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	require.NoError(t, os.Mkdir(binDir, 0o755))
	writeExecutable(t, filepath.Join(binDir, "go"), `#!/usr/bin/env bash
printf '{"columns":["ok"],"rows":[["true"]]}'
`)
	writeExecutable(t, filepath.Join(binDir, "gh"), `#!/usr/bin/env bash
case "$*" in
  *"search issues"*) printf '[{"number":1812,"title":"Skill be flagged as suspicious and being marked hidden","state":"open","url":"https://github.com/openclaw/clawhub/issues/1812"}]' ;;
  *"search prs"*) printf '[{"number":1803,"title":"fix: reduce env scan false positives","state":"open","url":"https://github.com/openclaw/clawhub/pull/1803"}]' ;;
  *) printf '[]' ;;
esac
`)
	writeExecutable(t, filepath.Join(binDir, "openclaw"), `#!/usr/bin/env bash
cat <<'JSON'
[diagnostic] noisy prefix before json
{
  "payloads": [
    {
      "text": "# Clawhub Field Notes\n\nLast generated: 2026-04-24 21:11 UTC\n\n## Current Chatter\n- People are discussing Clawhub skill moderation.\n\n## Friction And Questions\n- False-positive moderation appeals cluster around hidden skills.\n\n## GitHub Issues To Watch\n- Issue #1812 is worth watching because it captures creator-impacting moderation friction."
    }
  ]
}
JSON
`)

	backupRepo := filepath.Join(tmp, "backup")
	require.NoError(t, os.Mkdir(backupRepo, 0o755))
	cmd := exec.Command("bash", "scripts/discord-backup-clawhub-field-notes.sh", filepath.Join(tmp, "config.toml"), backupRepo)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"OPENCLAW_BIN="+filepath.Join(binDir, "openclaw"),
		"OPENCLAW_ATTEMPTS=1",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	notes, err := os.ReadFile(filepath.Join(backupRepo, "clawhub-field-notes.md"))
	require.NoError(t, err)
	text := string(notes)
	require.Contains(t, text, "# Clawhub Field Notes")
	require.Contains(t, text, "## Current Chatter")
	require.Contains(t, text, "Issue #1812")
	require.NotContains(t, text, "deterministic")
}

func TestClawhubFieldNotesScriptRejectsInvalidAgentText(t *testing.T) {
	root, err := filepath.Abs("..")
	require.NoError(t, err)

	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	require.NoError(t, os.Mkdir(binDir, 0o755))
	writeExecutable(t, filepath.Join(binDir, "go"), `#!/usr/bin/env bash
printf '{"columns":["ok"],"rows":[["true"]]}'
`)
	writeExecutable(t, filepath.Join(binDir, "gh"), `#!/usr/bin/env bash
printf '[]'
`)
	writeExecutable(t, filepath.Join(binDir, "openclaw"), `#!/usr/bin/env bash
printf '{"payloads":[{"text":"LLM error server_error: retry later"}]}'
`)

	backupRepo := filepath.Join(tmp, "backup")
	require.NoError(t, os.Mkdir(backupRepo, 0o755))
	cmd := exec.Command("bash", "scripts/discord-backup-clawhub-field-notes.sh", filepath.Join(tmp, "config.toml"), backupRepo)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"OPENCLAW_BIN="+filepath.Join(binDir, "openclaw"),
		"OPENCLAW_ATTEMPTS=1",
	)
	out, err := cmd.CombinedOutput()
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "refusing to write deterministic notes")
	require.NoFileExists(t, filepath.Join(backupRepo, "clawhub-field-notes.md"))
}
