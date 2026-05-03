package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steipete/discrawl/internal/config"
)

func (r *runtime) withSyncLock(fn func() error) error {
	if r.dbLockHeld {
		return fn()
	}
	lockPath, err := r.syncLockPath()
	if err != nil {
		return err
	}
	release, err := acquireSyncLock(r.ctx, lockPath)
	if err != nil {
		return err
	}
	r.dbLockHeld = true
	r.lockStarted = r.nowUTC()
	r.setSyncLockPhase("locked")
	defer func() {
		r.dbLockHeld = false
		r.lockStarted = time.Time{}
		_ = release()
	}()
	return fn()
}

func (r *runtime) setSyncLockPhase(phase string) {
	if !r.dbLockHeld {
		return
	}
	path, err := r.syncLockPath()
	if err != nil {
		return
	}
	started := r.lockStarted
	if started.IsZero() {
		started = r.nowUTC()
	}
	body := fmt.Sprintf("pid=%d\nstarted_at=%s\nupdated_at=%s\nphase=%s\n",
		os.Getpid(),
		started.Format(time.RFC3339Nano),
		r.nowUTC().Format(time.RFC3339Nano),
		phase,
	)
	_ = os.WriteFile(path, []byte(body), 0o600)
}

func (r *runtime) syncLockPath() (string, error) {
	dbPath, err := config.ExpandPath(r.cfg.DBPath)
	if err != nil {
		return "", configErr(err)
	}
	return filepath.Join(filepath.Dir(dbPath), ".discrawl-sync.lock"), nil
}

func syncLockErr(ctx context.Context, path string) error {
	if ctx.Err() != nil {
		if body, err := os.ReadFile(path); err == nil {
			details := strings.TrimSpace(string(body))
			if details != "" {
				return fmt.Errorf("wait for sync lock %s (%s): %w", path, strings.ReplaceAll(details, "\n", ", "), ctx.Err())
			}
		}
		return fmt.Errorf("wait for sync lock %s: %w", path, ctx.Err())
	}
	return nil
}
