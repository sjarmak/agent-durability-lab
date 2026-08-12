package main

import "testing"

func TestParseConfigRequiresPinnedCodexAndExperimentIdentities(t *testing.T) {
	config, err := parseConfig([]string{
		"--temporal-address", "127.0.0.1:7233", "--namespace", "default", "--task-queue", "queue-1",
		"--worker-id", "worker-1", "--ready-file", "/tmp/worker.ready.json",
		"--codex-binary", "/opt/codex", "--codex-home", "/tmp/codex-home",
		"--launcher-binary", "/opt/launcher", "--effect-binary", "/opt/effect",
		"--fixture-dir", "/tmp/fixture", "--destination", "/tmp/destination.db",
		"--workspace-effects", "/tmp/effects.jsonl", "--run-root", "/tmp/attempts",
		"--barrier-url", "http://127.0.0.1:8080", "--barrier-point", "effect",
		"--model", "gpt-5.6-sol", "--reasoning-effort", "low", "--output-schema", "/tmp/schema.json",
		"--fault-boundary", "unfaulted", "--hermetic",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if config.Worker.Command.Model != "gpt-5.6-sol" || !config.Worker.Hermetic || config.Worker.WorkerID != "worker-1" {
		t.Fatalf("config = %+v", config)
	}
	if _, err := parseConfig([]string{"--temporal-address", "127.0.0.1:7233"}); err == nil {
		t.Fatal("incomplete config unexpectedly succeeded")
	}
}
