//go:build linux

package lab

import (
	"os/exec"
	"syscall"
)

func configureSupervisedProcessPlatform(command *exec.Cmd) error {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}
