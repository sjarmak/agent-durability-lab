package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/agentsim"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestReadRequestAcceptsOneBoundedDocument(t *testing.T) {
	want := agentprocess.LaunchRequest{
		StorePath: "work.db", BarrierURL: "http://127.0.0.1",
		Config: agentsim.Config{Lease: workstore.Lease{SessionID: "session", Generation: 1, OwnerToken: "owner"}},
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	path := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write request: %v", err)
	}
	got, err := readRequest(path)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	if got.StorePath != want.StorePath || got.Config.Lease != want.Config.Lease {
		t.Fatalf("request = %+v; want %+v", got, want)
	}
}

func TestReadRequestRejectsMalformedUnknownAndTrailingData(t *testing.T) {
	for name, content := range map[string]string{
		"malformed": `not-json`,
		"unknown":   `{"store_path":"work.db","barrier_url":"http://127.0.0.1","config":{},"extra":true}`,
		"trailing":  `{} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatalf("write request: %v", err)
			}
			if _, err := readRequest(path); err == nil {
				t.Fatal("readRequest returned nil error")
			}
		})
	}
}

func TestAcknowledgeCommittedCancellationUsesExactProcessIdentity(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	config := agentsim.Config{
		Lease: decision.Lease, ActorID: "agent-1", PID: 101, ProcessStart: "boot:101", ProcessGroupID: 101,
	}
	if err := store.RegisterProcess(context.Background(), decision.Lease, workstore.Process{
		PID: config.PID, StartIdentity: config.ProcessStart, ProcessGroupID: config.ProcessGroupID,
	}); err != nil {
		t.Fatalf("register process: %v", err)
	}
	if _, err := store.CancelSession(context.Background(), workstore.CancelRequest{
		SessionID: "session-1", RequestID: "cancel-1",
	}); err != nil {
		t.Fatalf("cancel session: %v", err)
	}

	acknowledged, err := acknowledgeCommittedCancellation(context.Background(), store, config)
	if err != nil {
		t.Fatalf("acknowledge cancellation: %v", err)
	}
	if !acknowledged {
		t.Fatal("acknowledged = false; want true")
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Cancellation == nil || snapshot.Cancellation.Acknowledgement == nil {
		t.Fatal("cancellation acknowledgement is missing")
	}
}

func TestAcknowledgeCommittedCancellationDoesNothingBeforeRevocation(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	acknowledged, err := acknowledgeCommittedCancellation(context.Background(), store, agentsim.Config{
		Lease: decision.Lease, ActorID: "agent-1", PID: 101, ProcessStart: "boot:101", ProcessGroupID: 101,
	})
	if err != nil {
		t.Fatalf("observe active session: %v", err)
	}
	if acknowledged {
		t.Fatal("acknowledged = true before cancellation")
	}
}
