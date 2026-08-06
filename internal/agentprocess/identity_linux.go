//go:build linux

package agentprocess

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func CurrentProcessStartIdentity() (string, error) {
	return ProcessStartIdentity(os.Getpid())
}

func ProcessStartIdentity(pid int) (string, error) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", fmt.Errorf("read process %d stat: %w", pid, err)
	}
	closing := strings.LastIndexByte(string(stat), ')')
	if closing < 0 {
		return "", fmt.Errorf("parse process %d stat: missing command terminator", pid)
	}
	fields := strings.Fields(string(stat[closing+1:]))
	const startTimeIndex = 19
	if len(fields) <= startTimeIndex {
		return "", fmt.Errorf("parse process %d stat: only %d fields after command", pid, len(fields))
	}
	if _, err := strconv.ParseUint(fields[startTimeIndex], 10, 64); err != nil {
		return "", fmt.Errorf("parse process %d start time: %w", pid, err)
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot identity: %w", err)
	}
	return strings.TrimSpace(string(bootID)) + ":" + fields[startTimeIndex], nil
}
