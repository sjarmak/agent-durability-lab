package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunRequiresPilotInputs(t *testing.T) {
	err := run(context.Background(), []string{"--evidence-root", t.TempDir()}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "--work-root") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunRejectsNonPositiveDeadline(t *testing.T) {
	err := run(context.Background(), []string{"--evidence-root", "pilot", "--deadline", "0s"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "positive --deadline") {
		t.Fatalf("error = %v", err)
	}
}
