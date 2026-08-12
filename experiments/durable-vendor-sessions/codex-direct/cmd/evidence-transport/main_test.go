package main

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/transport"
)

func TestRunRoutesTransportOperations(t *testing.T) {
	var built transport.BuildConfig
	var verified, restoredRoot, restoredOutput string
	operation := operations{
		build: func(_ context.Context, config transport.BuildConfig) (transport.Index, error) {
			built = config
			return transport.Index{SchemaVersion: transport.SchemaVersion}, nil
		},
		verify: func(_ context.Context, root string) (transport.Index, error) {
			verified = root
			return transport.Index{SchemaVersion: transport.SchemaVersion}, nil
		},
		restore: func(_ context.Context, root, output string) error {
			restoredRoot, restoredOutput = root, output
			return nil
		},
	}
	for _, args := range [][]string{
		{"package", "--source", "raw", "--lineage", "lineage.json", "--output", "transport"},
		{"verify", "--transport", "transport"},
		{"restore", "--transport", "transport", "--output", "restored"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}, operation); err != nil {
			t.Fatal(err)
		}
	}
	want := transport.BuildConfig{SourceRoot: "raw", LineagePath: "lineage.json", OutputRoot: "transport"}
	if !reflect.DeepEqual(built, want) || verified != "transport" || restoredRoot != "transport" || restoredOutput != "restored" {
		t.Fatalf("unexpected routing: built=%+v verified=%q restored=%q/%q", built, verified, restoredRoot, restoredOutput)
	}
}

func TestRunRejectsIncompleteCommands(t *testing.T) {
	for _, args := range [][]string{nil, {"package"}, {"verify"}, {"restore"}, {"unknown"}} {
		if err := run(context.Background(), args, &bytes.Buffer{}, operations{}); err == nil {
			t.Fatalf("incomplete command %v succeeded", args)
		}
	}
}
