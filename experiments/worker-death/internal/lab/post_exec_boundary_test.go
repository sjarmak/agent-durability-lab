package lab

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestCapturePostExecBoundaryPreservesPendingStoreAndLiveChild(t *testing.T) {
	t.Parallel()
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "worker-1", AgentBuild: "build", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	startIdentity, err := agentprocess.CurrentProcessStartIdentity()
	if err != nil {
		t.Fatalf("current process identity: %v", err)
	}
	runDirectory := t.TempDir()
	process := observedProcess{
		PID: os.Getpid(), StartIdentity: startIdentity, ActorID: "agent-1",
		OwnerHash: workstore.HashToken(decision.Lease.OwnerToken),
	}
	if err := capturePostExecBoundary(
		context.Background(), runDirectory, store, "session-1", process, 999,
	); err != nil {
		t.Fatalf("capture boundary: %v", err)
	}
	file, err := os.Open(filepath.Join(runDirectory, "pre-kill-state.json"))
	if err != nil {
		t.Fatalf("open boundary evidence: %v", err)
	}
	defer file.Close()
	var evidence PostExecBoundaryEvidence
	if err := json.NewDecoder(file).Decode(&evidence); err != nil {
		t.Fatalf("decode boundary evidence: %v", err)
	}
	if evidence.WorkerPID != 999 || evidence.Child.PID != os.Getpid() ||
		len(evidence.Store.Executors) != 1 || evidence.Store.Executors[0].PID != 0 ||
		evidence.Store.Executors[0].Status != workstore.ExecutorStatusLaunchPending {
		t.Fatalf("boundary evidence = %+v", evidence)
	}
}
