package main

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/transport"
)

func TestRunRoutesPackageVerifyAndRestoreCommands(t *testing.T) {
	t.Parallel()
	var built transport.BuildConfig
	var verified string
	var restored [2]string
	operations := commandOperations{
		build: func(_ context.Context, config transport.BuildConfig) (transport.Index, error) {
			built = config
			return transport.Index{SchemaVersion: transport.SchemaVersion}, nil
		},
		verify: func(_ context.Context, root string) (transport.Index, error) {
			verified = root
			return transport.Index{SchemaVersion: transport.SchemaVersion}, nil
		},
		restore: func(_ context.Context, source, destination string) error {
			restored = [2]string{source, destination}
			return nil
		},
	}

	for _, args := range [][]string{
		{"package", "--source", "raw", "--lineage", "lineage.json", "--output", "packages"},
		{"verify", "--transport", "packages"},
		{"restore", "--transport", "packages", "--output", "restored"},
	} {
		var stdout, stderr bytes.Buffer
		if err := run(context.Background(), args, &stdout, &stderr, operations); err != nil {
			t.Fatalf("run %v: %v (%s)", args, err, stderr.String())
		}
		if stdout.Len() == 0 {
			t.Fatalf("run %v emitted no structured result", args)
		}
	}
	if want := (transport.BuildConfig{SourceRoot: "raw", LineagePath: "lineage.json", OutputRoot: "packages"}); !reflect.DeepEqual(built, want) {
		t.Fatalf("build config = %+v, want %+v", built, want)
	}
	if verified != "packages" {
		t.Fatalf("verified root = %q", verified)
	}
	if restored != [2]string{"packages", "restored"} {
		t.Fatalf("restored paths = %q", restored)
	}
}

func TestRunFailsClosedOnUsageAndOperationErrors(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("sentinel")
	operations := commandOperations{
		build: func(context.Context, transport.BuildConfig) (transport.Index, error) {
			return transport.Index{}, sentinel
		},
		verify:  func(context.Context, string) (transport.Index, error) { return transport.Index{}, sentinel },
		restore: func(context.Context, string, string) error { return sentinel },
	}
	tests := [][]string{
		nil,
		{"unknown"},
		{"package", "--source", "raw"},
		{"package", "--source", "raw", "--lineage", "lineage.json", "--output", "packages"},
		{"verify", "--transport", "packages"},
		{"restore", "--transport", "packages", "--output", "restored"},
		{"verify", "--bad-flag"},
	}
	for _, args := range tests {
		var stdout, stderr bytes.Buffer
		if err := run(context.Background(), args, &stdout, &stderr, operations); err == nil {
			t.Fatalf("run %v unexpectedly passed", args)
		}
	}
}

func TestRunReportsStructuredOutputFailure(t *testing.T) {
	t.Parallel()
	operations := commandOperations{
		verify: func(context.Context, string) (transport.Index, error) {
			return transport.Index{SchemaVersion: transport.SchemaVersion}, nil
		},
	}
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{"verify", "--transport", "packages"}, errorWriter{}, &stderr, operations); err == nil {
		t.Fatal("structured output failure passed")
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
