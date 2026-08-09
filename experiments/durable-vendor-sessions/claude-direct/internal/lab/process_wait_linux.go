//go:build linux

package lab

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"golang.org/x/sys/unix"
)

func waitForProcessExit(ctx context.Context, process ProcessRecord) error {
	currentIdentity, err := agentprocess.ProcessStartIdentity(process.PID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if currentIdentity != process.StartIdentity {
		return fmt.Errorf("process %d identity changed before exit observation", process.PID)
	}
	pidfd, err := unix.PidfdOpen(process.PID, 0)
	if err != nil {
		if errors.Is(err, unix.ESRCH) {
			return nil
		}
		return fmt.Errorf("open pidfd for process %d: %w", process.PID, err)
	}
	defer func() { _ = unix.Close(pidfd) }()
	pollDescriptors := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, err := unix.Poll(pollDescriptors, 250)
		if err != nil && !errors.Is(err, unix.EINTR) {
			return fmt.Errorf("wait on pidfd for process %d: %w", process.PID, err)
		}
		if count > 0 {
			return nil
		}
	}
}
