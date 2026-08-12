package lab

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestRunSupervisedInvocationRegistersProcessBeforeThreadAndEffect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := openCodexSupervisorStore(t)
	decision, err := store.StartOrAttach(ctx, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "supervisor", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	directory := t.TempDir()
	hermetic := buildHermeticCodex(t)
	effect := buildCodexTestBinary(t, "controlled-effect")
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	requestPath := filepath.Join(directory, "request.json")
	threadReceiptPath := filepath.Join(directory, "thread-receipt.json")
	writeTestJSON(t, requestPath, ControlledEffectInput{
		DestinationPath: filepath.Join(directory, "destination.db"), WorkspacePath: filepath.Join(directory, "effects.jsonl"),
		ThreadReceiptPath: threadReceiptPath, Payload: "controlled-edit",
		BarrierURL: server.URL, BarrierPoint: committedEffectBarrier,
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "generation-1", ActorID: "supervisor-g1",
	})
	command := CodexCommand{
		Binary: hermetic, WorkDir: directory, CodexHome: filepath.Join(directory, "codex-home"),
		Model: "gpt-5.6-sol", ReasoningEffort: "low", OutputSchema: filepath.Join(directory, "schema.json"),
		Sandbox: "workspace-write",
	}
	invocation, err := command.InitialInvocation(ControlledEffectPrompt(effect + " --request " + requestPath))
	if err != nil {
		t.Fatalf("invocation: %v", err)
	}
	invocation.Env = append(invocation.Env, "CODEX_HERMETIC_THREAD_ID="+testThreadID)
	threadReached := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, runErr := RunSupervisedInvocation(ctx, store, decision.Lease, invocation, RunInvocationInput{
			Directory: directory, AttemptID: "generation-1", ActorID: "supervisor-g1",
			ThreadReceiptPath: threadReceiptPath, RegistrationGate: true,
		}, StreamHooks{ExpectedCommand: effect + " --request " + requestPath, ThreadStarted: func(string) error {
			snapshot, snapshotErr := store.Snapshot(ctx, "session-1")
			if snapshotErr != nil {
				return snapshotErr
			}
			if snapshot.Executors[0].Status != workstore.ExecutorStatusRunning || snapshot.Executors[0].PID <= 0 {
				return fmt.Errorf("executor at thread boundary = %+v", snapshot.Executors[0])
			}
			close(threadReached)
			return nil
		}})
		finished <- runErr
	}()
	<-threadReached
	if _, err := coordinator.WaitForArrivals(ctx, committedEffectBarrier, 1); err != nil {
		t.Fatalf("wait for effect: %v", err)
	}
	if err := coordinator.Release(committedEffectBarrier); err != nil {
		t.Fatalf("release effect: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("supervised invocation: %v", err)
	}
}

func TestRunSupervisedInvocationBoundsPostKillCompletionAndVerifiesProcessGone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := openCodexSupervisorStore(t)
	decision, err := store.StartOrAttach(ctx, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "supervisor", Attempt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan ProcessRecord, 1)
	releaseHook := make(chan struct{})
	finished := make(chan error, 1)
	workDir := t.TempDir()
	attemptDir := filepath.Join(workDir, "attempt")
	go func() {
		_, runErr := RunSupervisedInvocation(ctx, store, decision.Lease, Invocation{
			Binary: "/bin/sh", Args: []string{"-c", "while :; do :; done"}, WorkDir: workDir, Stdin: "ignored",
		}, RunInvocationInput{Directory: attemptDir, AttemptID: "attempt-1", ActorID: "actor-1",
			ThreadReceiptPath: filepath.Join(attemptDir, "thread-receipt.json")},
			StreamHooks{ProcessStarted: func(process ProcessRecord) error {
				started <- process
				<-releaseHook
				return nil
			}})
		finished <- runErr
	}()
	var process ProcessRecord
	select {
	case process = <-started:
	case runErr := <-finished:
		t.Fatalf("supervised invocation exited before process hook: %v", runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("supervised invocation did not reach process hook")
	}
	cancel()
	select {
	case runErr := <-finished:
		if !errors.Is(runErr, errSupervisedExecutionDidNotExit) {
			t.Fatalf("run error = %v", runErr)
		}
	case <-time.After(supervisedTerminationGrace + 3*time.Second):
		t.Fatal("supervised invocation remained blocked after SIGKILL")
	}
	if disposition, probeErr := agentprocess.Probe(agentprocess.ProcessIdentity{
		PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
	}); disposition == agentprocess.DispositionAlive || (probeErr != nil && !errors.Is(probeErr, agentprocess.ErrProcessIdentityMismatch)) {
		t.Fatalf("process disposition = %q, error %v", disposition, probeErr)
	}
	close(releaseHook)
	completedPath := filepath.Join(attemptDir, "attempt-1.process-completed.json")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, statErr := os.Stat(completedPath); statErr == nil {
			break
		} else if !errors.Is(statErr, os.ErrNotExist) {
			t.Fatal(statErr)
		}
		if time.Now().After(deadline) {
			t.Fatal("runner did not finish after blocked hook was released")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSupervisedTerminationVerificationRejectsReusedOccupiedGroup(t *testing.T) {
	if supervisedProcessGroupTerminationVerified(agentprocess.DispositionReused,
		fmt.Errorf("%w: reused leader", agentprocess.ErrProcessIdentityMismatch)) {
		t.Fatal("reused process-group identity was accepted as verified termination")
	}
	if !supervisedProcessGroupTerminationVerified(agentprocess.DispositionGone, nil) {
		t.Fatal("empty process group was not accepted as verified termination")
	}
}
