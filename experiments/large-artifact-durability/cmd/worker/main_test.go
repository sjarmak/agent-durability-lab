package main

import (
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/large-artifact-durability/internal/lab"
)

func TestWorkerConfigRequiresExactBoundaryIdentity(t *testing.T) {
	t.Parallel()

	valid := workerConfig{
		address:      "127.0.0.1:7233",
		namespace:    "default",
		taskQueue:    "large-artifact-test",
		workerID:     "worker-1",
		barrierURL:   "http://127.0.0.1:8123",
		sessionID:    "artifact-run-1",
		storeRoot:    "/tmp/artifacts",
		externalRoot: "/tmp/external",
		mode:         lab.ModeProtected,
		boundary:     lab.BoundaryReferencePublished,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for name, mutate := range map[string]func(*workerConfig){
		"worker":   func(config *workerConfig) { config.workerID = "" },
		"barrier":  func(config *workerConfig) { config.barrierURL = "http://example.com" },
		"session":  func(config *workerConfig) { config.sessionID = "" },
		"mode":     func(config *workerConfig) { config.mode = "other" },
		"boundary": func(config *workerConfig) { config.boundary = "other" },
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if err := changed.validate(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
