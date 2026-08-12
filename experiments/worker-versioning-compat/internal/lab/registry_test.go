package lab

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestRegistryStartsAttachesAndRejectsIncompatibleBuild(t *testing.T) {
	registry := Registry{Path: filepath.Join(t.TempDir(), "agent.json")}
	started, err := registry.StartOrAttach(context.Background(), AttachRequest{
		SessionID: "session-1", AgentBuild: "agent-v1", WorkerBuild: "worker-v1",
		CompatibleAgentBuilds: []string{"agent-v1"},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if started.Action != ActionStarted || started.AgentBuild != "agent-v1" {
		t.Fatalf("started = %+v", started)
	}
	attached, err := registry.StartOrAttach(context.Background(), AttachRequest{
		SessionID: "session-1", AgentBuild: "agent-v2", WorkerBuild: "worker-v2",
		CompatibleAgentBuilds: []string{"agent-v1", "agent-v2"},
	})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if attached.Action != ActionAttached || attached.AgentBuild != "agent-v1" || attached.WorkerBuild != "worker-v2" {
		t.Fatalf("attached = %+v", attached)
	}
	_, err = registry.StartOrAttach(context.Background(), AttachRequest{
		SessionID: "session-1", AgentBuild: "agent-v3", WorkerBuild: "worker-v3",
		CompatibleAgentBuilds: []string{"agent-v3"},
	})
	if !errors.Is(err, ErrIncompatibleAgentBuild) {
		t.Fatalf("incompatible attach = %v", err)
	}
	record, err := registry.Read()
	if err != nil {
		t.Fatal(err)
	}
	if record.AgentBuild != "agent-v1" || len(record.Attachments) != 1 {
		t.Fatalf("rejected attach mutated registry: %+v", record)
	}
}

func TestRegistryRejectsSessionSubstitutionAndChangedStart(t *testing.T) {
	registry := Registry{Path: filepath.Join(t.TempDir(), "agent.json")}
	if _, err := registry.StartOrAttach(context.Background(), AttachRequest{
		SessionID: "session-1", AgentBuild: "agent-v1", WorkerBuild: "worker-v1",
		CompatibleAgentBuilds: []string{"agent-v1"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, request := range []AttachRequest{
		{SessionID: "session-2", AgentBuild: "agent-v1", WorkerBuild: "worker-v1", CompatibleAgentBuilds: []string{"agent-v1"}},
		{SessionID: "session-1", AgentBuild: "agent-v2", WorkerBuild: "worker-v2", CompatibleAgentBuilds: []string{"agent-v2"}},
	} {
		if _, err := registry.StartOrAttach(context.Background(), request); err == nil {
			t.Fatalf("request %+v was accepted", request)
		}
	}
}

func TestRegistryRejectsIncompleteAndCanceledRequestsBeforeOpeningStore(t *testing.T) {
	registry := Registry{Path: filepath.Join(t.TempDir(), "agent.db")}
	for _, request := range []AttachRequest{
		{},
		{SessionID: "session-1", AgentBuild: "agent-v1", WorkerBuild: "worker-v1", CompatibleAgentBuilds: []string{"different"}},
	} {
		if _, err := registry.StartOrAttach(context.Background(), request); err == nil {
			t.Fatalf("request %+v accepted", request)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.StartOrAttach(ctx, AttachRequest{
		SessionID: "session-1", AgentBuild: "agent-v1", WorkerBuild: "worker-v1", CompatibleAgentBuilds: []string{"agent-v1"},
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request = %v", err)
	}
}
