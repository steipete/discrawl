package main

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestMainHelpAndVersion(t *testing.T) {
	if os.Getenv("DISCRAWL_MAIN_ERROR") == "1" {
		os.Args = []string{"discrawl", "bogus"}
		main()
		return
	}

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })

	os.Args = []string{"discrawl", "help"}
	main()

	os.Args = []string{"discrawl", "--version"}
	main()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=TestMainHelpAndVersion")
	cmd.Env = append(os.Environ(), "DISCRAWL_MAIN_ERROR=1")
	err = cmd.Run()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if exitErr.ExitCode() == 2 {
			return
		}
	}
	t.Fatalf("expected exit code 2, got %v", err)
}
