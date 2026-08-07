//go:build !linux

package agentprocess

import (
	"fmt"
	"os"
	"time"
)

func CurrentProcessStartIdentity() (string, error) {
	return fmt.Sprintf("pid-%d-observed-%d", os.Getpid(), time.Now().UTC().UnixNano()), nil
}

func ProcessStartIdentity(pid int) (string, error) {
	return fmt.Sprintf("pid-%d-observed-%d", pid, time.Now().UTC().UnixNano()), nil
}

func CurrentProcessGroupID() (int, error) {
	return 0, nil
}

func ProcessGroupID(pid int) (int, error) {
	return 0, nil
}
