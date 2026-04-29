//go:build windows

package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

func acquireSyncLock(ctx context.Context, path string) (func() error, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open sync lock: %w", err)
	}
	locked := false
	defer func() {
		if !locked {
			_ = file.Close()
		}
	}()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	handle := windows.Handle(file.Fd())
	overlapped := &windows.Overlapped{}
	for {
		err = windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
		if err == nil {
			locked = true
			_, _ = file.Seek(0, 0)
			_ = file.Truncate(0)
			_, _ = fmt.Fprintf(file, "pid=%d\n", os.Getpid())
			return func() error {
				unlockErr := windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
				closeErr := file.Close()
				if unlockErr != nil {
					return unlockErr
				}
				return closeErr
			}, nil
		}
		select {
		case <-ctx.Done():
			return nil, syncLockErr(ctx, path)
		case <-ticker.C:
		}
	}
}
