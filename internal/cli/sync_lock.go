package cli

import (
	"context"
	"fmt"
	"path/filepath"

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
	defer func() {
		r.dbLockHeld = false
		_ = release()
	}()
	return fn()
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
		return fmt.Errorf("wait for sync lock %s: %w", path, ctx.Err())
	}
	return nil
}
