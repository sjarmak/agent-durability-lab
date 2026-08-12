//go:build unix

package lab

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockWorkspaceEffect(path string) (func() error, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open workspace effect lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return nil, errors.Join(fmt.Errorf("lock workspace effect: %w", err), file.Close())
	}
	return func() error {
		return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
	}, nil
}
