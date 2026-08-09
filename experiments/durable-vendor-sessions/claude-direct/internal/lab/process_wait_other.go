//go:build !linux

package lab

import (
	"context"
	"errors"
)

func waitForProcessExit(context.Context, ProcessRecord) error {
	return errors.New("exact external process-exit observation requires Linux pidfd support")
}
