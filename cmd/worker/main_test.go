package main

import "testing"

func TestWorkerConfigValidation(t *testing.T) {
	valid := workerConfig{
		address: "127.0.0.1:7233", namespace: "default", taskQueue: "queue", storePath: "work.db",
		agentBinary: "agent", barrierURL: "http://127.0.0.1", runDirectory: "runs", workerID: "worker", agentBuild: "build",
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	valid.workerID = ""
	if err := valid.validate(); err == nil {
		t.Fatal("incomplete config returned nil error")
	}
}
