//go:build windows

package agentprocess

import "os/exec"

func configureDetached(_ *exec.Cmd) {}
