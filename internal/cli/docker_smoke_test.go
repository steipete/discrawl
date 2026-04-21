package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDockerGitSourceSmoke(t *testing.T) {
	if os.Getenv("DISCRAWL_DOCKER_TEST") != "1" {
		t.Skip("set DISCRAWL_DOCKER_TEST=1 to run the Docker git-source smoke test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	root := repoRoot(t)
	cmd := exec.Command("bash", filepath.Join(root, "scripts", "docker-git-source-smoke.sh"))
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

func repoRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	require.NoError(t, err)
	return strings.TrimSpace(string(out))
}
