//go:build !linux

package lab

import "os/exec"

func configureProcessGroup(_ *exec.Cmd) {}
