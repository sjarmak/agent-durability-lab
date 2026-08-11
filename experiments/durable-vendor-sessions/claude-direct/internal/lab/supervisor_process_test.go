package lab

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestRunSupervisedInvocationHasExactExecBeforeRegistrationBoundary(t *testing.T) {
	store := openSupervisorTestStore(t)
	lease := claimSupervisorTestPendingGeneration(t, store, "session-1", "worker-1", "owner-1", 1)
	directory := t.TempDir()
	binary := writeExecutable(t, directory, "fake-claude", strings.Join([]string{
		"#!/bin/sh",
		`printf '%s\n' '{"type":"system","subtype":"init","session_id":"vendor-session-1"}'`,
		`printf '%s\n' '{"type":"result","subtype":"success","session_id":"vendor-session-1","is_error":false,"structured_output":{"status":"EFFECT_COMPLETE"}}'`,
	}, "\n")+"\n")
	started := make(chan ProcessRecord, 1)
	releaseRegistration := make(chan struct{})

	result := make(chan struct {
		invocation InvocationResult
		err        error
	}, 1)
	go func() {
		invocation, err := RunSupervisedInvocation(context.Background(), store, lease, Invocation{
			Binary: binary, WorkDir: directory, Stdin: "prompt\n",
		}, RunInvocationInput{Directory: directory, AttemptID: "generation-1", ActorID: "supervisor-g1"},
			supervisedInvocationHooks{AfterExecBeforeRegistration: func(_ context.Context, process ProcessRecord) error {
				started <- process
				<-releaseRegistration
				return nil
			}})
		result <- struct {
			invocation InvocationResult
			err        error
		}{invocation: invocation, err: err}
	}()
	process := <-started
	snapshot, err := store.Snapshot(context.Background(), lease.SessionID)
	if err != nil {
		t.Fatalf("snapshot before registration: %v", err)
	}
	if len(snapshot.Executors) != 1 || snapshot.Executors[0].Status != workstore.ExecutorStatusLaunchPending ||
		snapshot.Executors[0].PID != 0 {
		t.Fatalf("pre-registration executor = %+v", snapshot.Executors)
	}
	if process.PID <= 0 || process.StartIdentity == "" || process.ProcessGroupID != process.PID {
		t.Fatalf("started process identity = %+v", process)
	}
	close(releaseRegistration)
	completed := <-result
	if completed.err != nil {
		t.Fatalf("run supervised invocation: %v", completed.err)
	}
	if completed.invocation.Claude.SessionID != "vendor-session-1" ||
		completed.invocation.Claude.Result != "EFFECT_COMPLETE" {
		t.Fatalf("Claude result = %+v", completed.invocation.Claude)
	}
	snapshot, err = store.Snapshot(context.Background(), lease.SessionID)
	if err != nil {
		t.Fatalf("snapshot after registration: %v", err)
	}
	if snapshot.Executors[0].Status != workstore.ExecutorStatusRunning ||
		snapshot.Executors[0].PID != process.PID ||
		snapshot.Executors[0].ProcessStart != process.StartIdentity ||
		snapshot.Executors[0].ProcessGroupID != process.ProcessGroupID {
		t.Fatalf("registered executor = %+v, process = %+v", snapshot.Executors[0], process)
	}
}

func testRunSupervisedInvocationCancellationTargetsExactRegisteredProcess(t *testing.T) {
	if os.Getenv("CLAUDE_DIRECT_SUPERVISOR_BLOCKING_HELPER") == "1" {
		return
	}
	store := openSupervisorTestStore(t)
	lease := claimSupervisorTestPendingGeneration(t, store, "session-1", "worker-1", "owner-1", 1)
	arrived := make(chan struct{})
	releaseBarrier := make(chan struct{})
	barrier := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(arrived)
		select {
		case <-request.Context().Done():
		case <-releaseBarrier:
		}
	}))
	t.Cleanup(barrier.Close)
	t.Cleanup(func() { close(releaseBarrier) })
	directory := t.TempDir()
	started := make(chan ProcessRecord, 1)
	runContext, cancelRun := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := RunSupervisedInvocation(runContext, store, lease, Invocation{
			Binary: os.Args[0], Args: []string{"-test.run=^TestSupervisorInvocationBlockingHelper$"},
			Env: []string{
				"CLAUDE_DIRECT_SUPERVISOR_BLOCKING_HELPER=1",
				"CLAUDE_DIRECT_SUPERVISOR_BARRIER_URL=" + barrier.URL,
			},
			WorkDir: directory, Stdin: "prompt\n",
		}, RunInvocationInput{Directory: directory, AttemptID: "generation-1", ActorID: "supervisor-g1"},
			supervisedInvocationHooks{AfterExecBeforeRegistration: func(_ context.Context, process ProcessRecord) error {
				started <- process
				return nil
			}})
		result <- err
	}()
	process := <-started
	<-arrived
	snapshot, err := store.Snapshot(context.Background(), lease.SessionID)
	if err != nil {
		t.Fatalf("snapshot running process: %v", err)
	}
	if snapshot.Executors[0].PID != process.PID || snapshot.Executors[0].Status != workstore.ExecutorStatusRunning {
		t.Fatalf("running executor = %+v, process = %+v", snapshot.Executors[0], process)
	}
	if _, err := store.CancelSession(context.Background(), workstore.CancelRequest{
		SessionID: lease.SessionID, RequestID: "cancel-1",
	}); err != nil {
		t.Fatalf("commit cancellation: %v", err)
	}
	cancelRun()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled supervised invocation = %v, want context.Canceled", err)
	}
	disposition, err := agentprocess.Probe(agentprocess.ProcessIdentity{
		PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
	})
	if err != nil && !errors.Is(err, agentprocess.ErrProcessIdentityMismatch) {
		t.Fatalf("probe canceled process: %v", err)
	}
	if err == nil && disposition != agentprocess.DispositionGone {
		t.Fatalf("canceled process disposition = %q, want gone", disposition)
	}
}

func testTurnSupervisorReplacementStopsExactOldProcessAfterFenceCommit(t *testing.T) {
	store := openSupervisorTestStore(t)
	arrived := make(chan struct{})
	releaseBarrier := make(chan struct{})
	barrier := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		close(arrived)
		select {
		case <-request.Context().Done():
		case <-releaseBarrier:
		}
	}))
	t.Cleanup(barrier.Close)
	t.Cleanup(func() { close(releaseBarrier) })
	directory := t.TempDir()
	replacementBinary := writeExecutable(t, directory, "replacement-claude", strings.Join([]string{
		"#!/bin/sh",
		`printf '%s\n' '{"type":"system","subtype":"init","session_id":"vendor-replacement"}'`,
		`printf '%s\n' '{"type":"result","subtype":"success","session_id":"vendor-replacement","is_error":false,"structured_output":{"status":"EFFECT_COMPLETE"}}'`,
	}, "\n")+"\n")
	oldProcess := make(chan ProcessRecord, 1)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			invocation := Invocation{WorkDir: directory, Stdin: "prompt\n"}
			if lease.Generation == 1 {
				invocation.Binary = os.Args[0]
				invocation.Args = []string{"-test.run=^TestSupervisorInvocationBlockingHelper$"}
				invocation.Env = []string{
					"CLAUDE_DIRECT_SUPERVISOR_BLOCKING_HELPER=1",
					"CLAUDE_DIRECT_SUPERVISOR_BARRIER_URL=" + barrier.URL,
				}
			} else {
				invocation.Binary = replacementBinary
			}
			result, err := RunSupervisedInvocation(ctx, store, lease, invocation, RunInvocationInput{
				Directory: filepath.Join(directory, fmt.Sprintf("generation-%d", lease.Generation)),
				AttemptID: fmt.Sprintf("generation-%d", lease.Generation),
				ActorID:   fmt.Sprintf("supervisor-g%d", lease.Generation),
			}, supervisedInvocationHooks{AfterExecBeforeRegistration: func(_ context.Context, process ProcessRecord) error {
				if lease.Generation == 1 {
					oldProcess <- process
				}
				return nil
			}})
			if err != nil {
				snapshot, snapshotErr := store.Snapshot(context.Background(), lease.SessionID)
				if snapshotErr == nil && snapshot.ActiveGeneration != lease.Generation {
					return supervisedResult{}, errors.Join(workstore.ErrStaleOwner, err)
				}
				return supervisedResult{}, errors.Join(err, snapshotErr)
			}
			if err := store.CommitEffectOnce(ctx, lease, workstore.Effect{ID: "effect-1", Value: "replacement"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{
				VendorSessionID: result.Claude.SessionID, ProcessIdentity: result.Process.Identity,
				PhysicalAttemptID: fmt.Sprintf("generation-%d", lease.Generation),
				Outcome:           workstore.Outcome{Value: result.Claude.Result},
			}, nil
		}, sequentialCapabilities())

	first := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-a", Attempt: 1,
		})
		first <- err
	}()
	process := <-oldProcess
	<-arrived
	replacement, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-b", Attempt: 2, Replace: true,
	})
	if err != nil {
		t.Fatalf("replace supervised process: %v", err)
	}
	if replacement.Generation != 2 || replacement.Outcome.Value != "EFFECT_COMPLETE" ||
		replacement.VendorSessionID != "vendor-replacement" {
		t.Fatalf("replacement receipt = %+v", replacement)
	}
	if err := <-first; !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("old process result = %v, want ErrStaleOwner", err)
	}
	disposition, err := agentprocess.Probe(agentprocess.ProcessIdentity{
		PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
	})
	if err != nil && !errors.Is(err, agentprocess.ErrProcessIdentityMismatch) {
		t.Fatalf("probe replaced process: %v", err)
	}
	if err == nil && disposition != agentprocess.DispositionGone {
		t.Fatalf("replaced process disposition = %q, want gone", disposition)
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("replacement snapshot: %v", err)
	}
	if snapshot.ActiveGeneration != 2 || len(snapshot.Effects) != 1 ||
		snapshot.Effects[0].Generation != 2 || snapshot.Outcome == nil {
		t.Fatalf("replacement snapshot = %+v", snapshot)
	}
}

func TestSupervisorInvocationBlockingHelper(t *testing.T) {
	if os.Getenv("CLAUDE_DIRECT_SUPERVISOR_BLOCKING_HELPER") != "1" {
		return
	}
	response, err := http.Post(os.Getenv("CLAUDE_DIRECT_SUPERVISOR_BARRIER_URL"), "text/plain", strings.NewReader("arrived"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_ = response.Body.Close()
	os.Exit(0)
}

func claimSupervisorTestPendingGeneration(t *testing.T, store *workstore.Store, sessionID, workerID,
	capability string, attempt int32,
) workstore.Lease {
	t.Helper()
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: sessionID, Mode: workstore.ModeFenced, CandidateOwner: capability,
		WorkerID: workerID, Attempt: attempt,
	})
	if err != nil {
		t.Fatalf("claim pending generation: %v", err)
	}
	if decision.Action != workstore.ActionLaunch {
		t.Fatalf("pending claim action = %q, want launch", decision.Action)
	}
	return decision.Lease
}
