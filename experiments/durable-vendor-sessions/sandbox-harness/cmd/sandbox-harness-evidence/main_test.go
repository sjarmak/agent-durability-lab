package main

import (
	"context"
	"io"
	"testing"
)

func TestRunRejectsMissingRequiredArguments(t *testing.T) {
	t.Parallel()
	if err := run(context.Background(), nil, io.Discard); err == nil {
		t.Fatal("run() error = nil, want required-arguments error")
	}
}
