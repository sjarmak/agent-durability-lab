//go:build !linux

package lab

import (
	"os/exec"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
)

func configureSupervisedProcessPlatform(*exec.Cmd) error {
	return agentprocess.ErrProcessControlUnsupported
}
