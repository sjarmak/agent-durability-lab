//go:build !unix

package lab

import "errors"

func lockWorkspaceEffect(string) (func() error, error) {
	return nil, errors.New("workspace effect locking is unsupported on this platform")
}
