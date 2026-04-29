//go:build unix

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
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
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			locked = true
			_, _ = file.Seek(0, 0)
			_ = file.Truncate(0)
			_, _ = fmt.Fprintf(file, "pid=%d\n", os.Getpid())
			return func() error {
				unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
				closeErr := file.Close()
				if unlockErr != nil {
					return unlockErr
				}
				return closeErr
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("acquire sync lock: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, syncLockErr(ctx, path)
		case <-ticker.C:
		}
	}
}
