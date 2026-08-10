package agent

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestLauncherRunsIdenticalHermeticProcessAcrossTopologyIdentities(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("detached process identity is currently implemented for Linux")
	}
	temporary := t.TempDir()
	binary := filepath.Join(temporary, "agent-simulator")
	build := exec.Command("go", "build", "-o", binary, "./cmd/agent-simulator")
	build.Dir = repositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}

	for _, topology := range protocol.Topologies() {
		t.Run(string(topology), func(t *testing.T) {
			root := t.TempDir()
			store, err := workstore.Open(filepath.Join(root, "work.db"))
			if err != nil {
				t.Fatal(err)
			}
			ownerToken := "test-owner"
			decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
				SessionID: "operation-1/item-001", Mode: workstore.ModeFenced, CandidateOwner: ownerToken,
				WorkerID: "worker-1", Attempt: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			coordinator := failureinject.NewCoordinator()
			server := httptest.NewServer(coordinator.Handler())
			t.Cleanup(server.Close)
			manifest := protocol.Manifest{
				ProtocolVersion: protocol.PublicationProtocolVersion, RunID: "run-" + string(topology), PairID: "pair-1",
				ScheduleBlockID: "schedule-block/pair-1", TrackerBeadID: "temporal_projects-4ic.1", Topology: topology,
				Case: protocol.CaseJoinBarrier, Boundary: "designated-item-result-observed-before-activity-completion",
				Probe: protocol.ProbeProtected, Fanout: 8, LogicalOperationID: "operation-1", CreatedAtUTC: "2026-08-09T16:00:00Z",
				RequiredEvidence: protocol.RequiredEvidenceFiles(),
			}
			request := Request{
				Manifest: manifest, WorkItemID: "item-001", Lease: decision.Lease,
				ParentWorkflowID: "parent-workflow", ParentRunID: "parent-run", ActivityID: "activity-item-001", ActivityAttempt: 1,
				WorkerID: "worker-1", WorkerPID: os.Getpid(), StorePath: store.Path(), BarrierURL: server.URL,
				EffectValue: "changed", OutcomeValue: "done",
			}
			if topology == protocol.TopologyChildWorkflow {
				request.ChildWorkflowID, request.ChildRunID = "child-item-001", "child-run-item-001"
			}
			launched, err := NewLauncher(binary, filepath.Join(root, "processes"), root).Launch(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = syscall.Kill(launched.Process.PID, syscall.SIGKILL) })
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			arrivals, err := coordinator.WaitForArrivals(ctx, "before-effect/1", 1)
			if err != nil {
				t.Fatal(err)
			}
			arrival := arrivals[0]
			if arrival.PID != launched.Process.PID || arrival.ProcessStart != launched.Process.StartIdentity ||
				arrival.SessionID != decision.Lease.SessionID || arrival.OwnerTokenHash != launched.Identity.CapabilityHash {
				t.Fatalf("exact barrier arrival = %+v, launch = %+v", arrival, launched)
			}
			if launched.Identity.ProcessIdentity == "" || launched.Identity.WorkerPID != os.Getpid() || launched.Identity.ActivityAttempt != 1 {
				t.Fatalf("stable process evidence = %+v", launched.Identity)
			}
			if err := coordinator.Release("before-effect/1"); err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.WaitForArrivals(ctx, "before-completion/1", 1); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Release("before-completion/1"); err != nil {
				t.Fatal(err)
			}
			snapshot := waitForOutcome(t, ctx, store, decision.Lease.SessionID)
			if snapshot.Outcome == nil || snapshot.Outcome.Value != "done" || len(snapshot.Effects) != 1 || snapshot.Effects[0].Value != "changed" {
				t.Fatalf("process result = %+v", snapshot)
			}
			if len(snapshot.Executors) != 1 || snapshot.Executors[0].PID != launched.Process.PID || snapshot.Executors[0].ProcessStart != launched.Process.StartIdentity {
				t.Fatalf("process registration = %+v", snapshot.Executors)
			}
		})
	}
}

func TestLauncherRejectsTopologyIdentityMismatchBeforeProcessStart(t *testing.T) {
	manifest := protocol.Manifest{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: "run-child", PairID: "pair-1", ScheduleBlockID: "schedule-block/pair-1",
		TrackerBeadID: "temporal_projects-4ic.1", Topology: protocol.TopologyChildWorkflow, Case: protocol.CaseJoinBarrier,
		Boundary: "designated-item-result-observed-before-activity-completion", Probe: protocol.ProbeProtected, Fanout: 8,
		LogicalOperationID: "operation-1", CreatedAtUTC: "2026-08-09T16:00:00Z", RequiredEvidence: protocol.RequiredEvidenceFiles(),
	}
	workRoot := t.TempDir()
	_, err := NewLauncher(os.Args[0], filepath.Join(workRoot, "processes"), workRoot).Launch(context.Background(), Request{
		Manifest: manifest, WorkItemID: "item-001", Lease: workstore.Lease{SessionID: "session", Generation: 1, OwnerToken: "owner"},
		ParentWorkflowID: "parent", ParentRunID: "parent-run", ActivityID: "activity", ActivityAttempt: 1,
		WorkerID: "worker", WorkerPID: os.Getpid(), StorePath: "work.db", BarrierURL: "http://127.0.0.1",
		EffectValue: "changed", OutcomeValue: "done",
	})
	if !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("identity mismatch error = %v", err)
	}
}

func TestLauncherRejectsAuthorityBypassOutsideUnsafeControl(t *testing.T) {
	request := Request{
		Manifest: protocol.Manifest{
			ProtocolVersion: protocol.PublicationProtocolVersion, RunID: "run", PairID: "pair", ScheduleBlockID: "block",
			TrackerBeadID: "temporal_projects-4ic.2", Topology: protocol.TopologyDirectActivity, Case: protocol.CaseQueuedExecutingSupersession,
			Boundary: "executing-after-process-start-before-effect", Probe: protocol.ProbeProtected, Fanout: 8,
			LogicalOperationID: "operation", CreatedAtUTC: "2026-08-09T16:00:00Z", RequiredEvidence: protocol.RequiredEvidenceFiles(),
		},
		WorkItemID: "item-001", Lease: workstore.Lease{SessionID: "operation/item-001", Generation: 1, OwnerToken: "owner"},
		ParentWorkflowID: "parent", ParentRunID: "run", ActivityID: "activity", ActivityAttempt: 1,
		WorkerID: "worker", WorkerPID: os.Getpid(), StorePath: "store", BarrierURL: "http://127.0.0.1",
		EffectValue: "effect", OutcomeValue: "outcome", BypassAuthorityForEffect: true,
	}
	if err := request.validate(); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("protected authority bypass error = %v", err)
	}
}

func TestLauncherRejectsAmbientIOOutsideExplicitWorkRoot(t *testing.T) {
	workRoot := t.TempDir()
	store, err := workstore.Open(filepath.Join(workRoot, "work.db"))
	if err != nil {
		t.Fatal(err)
	}
	outsideRoot := t.TempDir()
	outsideStore, err := workstore.Open(filepath.Join(outsideRoot, "work.db"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(failureinject.NewCoordinator().Handler())
	t.Cleanup(server.Close)
	manifest := protocol.Manifest{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: "run-direct", PairID: "pair-1", ScheduleBlockID: "schedule-block/pair-1",
		TrackerBeadID: "temporal_projects-4ic.1", Topology: protocol.TopologyDirectActivity, Case: protocol.CaseJoinBarrier,
		Boundary: "designated-item-result-observed-before-activity-completion", Probe: protocol.ProbeProtected, Fanout: 8,
		LogicalOperationID: "operation-1", CreatedAtUTC: "2026-08-09T16:00:00Z", RequiredEvidence: protocol.RequiredEvidenceFiles(),
	}
	valid := Request{
		Manifest: manifest, WorkItemID: "item-001", Lease: workstore.Lease{SessionID: "operation-1/item-001", Generation: 1, OwnerToken: "owner"},
		ParentWorkflowID: "parent", ParentRunID: "parent-run", ActivityID: "activity", ActivityAttempt: 1,
		WorkerID: "worker", WorkerPID: os.Getpid(), StorePath: store.Path(), BarrierURL: server.URL,
		EffectValue: "changed", OutcomeValue: "done",
	}
	tests := []struct {
		name      string
		launcher  *Launcher
		ctx       context.Context
		configure func(*Request)
	}{
		{name: "non-loopback barrier", launcher: NewLauncher(os.Args[0], filepath.Join(workRoot, "processes-remote"), workRoot), ctx: context.Background(), configure: func(request *Request) {
			request.BarrierURL = "http://198.51.100.10:8080"
		}},
		{name: "store outside root", launcher: NewLauncher(os.Args[0], filepath.Join(workRoot, "processes-store"), workRoot), ctx: context.Background(), configure: func(request *Request) {
			request.StorePath = outsideStore.Path()
		}},
		{name: "run directory outside root", launcher: NewLauncher(os.Args[0], filepath.Join(outsideRoot, "processes"), workRoot), ctx: context.Background(), configure: func(*Request) {}},
		{name: "nil context", launcher: NewLauncher(os.Args[0], filepath.Join(workRoot, "processes-nil"), workRoot), configure: func(*Request) {}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.configure(&request)
			if _, err := test.launcher.Launch(test.ctx, request); !errors.Is(err, protocol.ErrInvalidEvidence) {
				t.Fatalf("Launch() error = %v", err)
			}
		})
	}
}

func waitForOutcome(t *testing.T, ctx context.Context, store *workstore.Store, sessionID string) workstore.Snapshot {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := store.Snapshot(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Outcome != nil {
			return snapshot
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for process outcome: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../../.."))
}
