//go:build !unix && !windows

package cli

import "context"

func acquireSyncLock(context.Context, string) (func() error, error) {
	return func() error { return nil }, nil
}
