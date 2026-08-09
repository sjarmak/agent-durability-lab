package main

import (
	"testing"
)

func TestParseConfigBuildsCompleteWorkerConfiguration(t *testing.T) {
	t.Parallel()

	config, err := parseConfig([]string{
		"--temporal-address", "127.0.0.1:7233",
		"--task-queue", "claude-direct",
		"--worker-id", "worker-one",
		"--claude-binary", "/opt/claude",
		"--launcher-binary", "/opt/launcher",
		"--fault-boundary", "tool-effect-before-activity-completion",
		"--effect-binary", "/opt/effect",
		"--fixture-dir", "/tmp/fixture",
		"--destination", "/tmp/destination.db",
		"--workspace-effects", "/tmp/fixture/effects.jsonl",
		"--barrier-url", "http://127.0.0.1:8080",
		"--barrier-point", "effect-committed",
		"--run-root", "/tmp/runs",
		"--ready-file", "/tmp/worker-one.ready.json",
		"--model", "haiku",
		"--max-budget-usd", "0.25",
		"--max-turns", "2",
	})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if config.TemporalAddress != "127.0.0.1:7233" || config.TaskQueue != "claude-direct" ||
		config.ReadyFile != "/tmp/worker-one.ready.json" || config.Worker.WorkerID != "worker-one" ||
		config.Worker.Command.Binary != "/opt/claude" || config.Worker.Command.Model != "haiku" ||
		config.Worker.LauncherBinary != "/opt/launcher" || config.Worker.Command.MaxTurns != 2 {
		t.Fatalf("config = %+v", config)
	}
}

func TestParseConfigRejectsMissingRequiredFlags(t *testing.T) {
	t.Parallel()

	if _, err := parseConfig(nil); err == nil {
		t.Fatal("empty flags returned nil error")
	}
}
