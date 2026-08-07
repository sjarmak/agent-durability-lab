package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/temporalagent"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

type observedProcess struct {
	PID           int
	StartIdentity string
	ActorID       string
	OwnerHash     string
	DiscoveredAt  time.Time
}

func RunPostExecGap(parent context.Context, postExecOptions PostExecGapOptions) (result PostExecGapResult, runErr error) {
	if !postExecOptions.Arm.Valid() {
		return result, fmt.Errorf("post-exec arm %q is invalid", postExecOptions.Arm)
	}
	options := postExecOptions.Options
	if options.Mode != workstore.ModeFenced {
		return result, errors.New("post-exec experiment requires fenced mode")
	}
	if err := validateOptions(options); err != nil {
		return result, err
	}
	if options.Timeout <= 0 {
		options.Timeout = defaultRunTimeout
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()

	runDirectory := filepath.Join(options.OutputRoot, options.RunID)
	if err := os.MkdirAll(options.OutputRoot, 0o750); err != nil {
		return result, fmt.Errorf("create evidence root: %w", err)
	}
	if err := os.Mkdir(runDirectory, 0o750); err != nil {
		return result, fmt.Errorf("create append-only run directory: %w", err)
	}
	result.RunDirectory = runDirectory
	defer func() {
		if runErr != nil {
			_ = writeJSON(filepath.Join(runDirectory, "failure.json"), failureRecord{Time: time.Now().UTC(), Error: runErr.Error()})
		}
	}()

	startedAt := time.Now().UTC()
	sessionID := "session-" + options.RunID
	store, err := workstore.Open(filepath.Join(runDirectory, "application.db"))
	if err != nil {
		return result, err
	}
	var discovered observedProcess
	defer func() {
		cleanupExactProcess(discovered)
		if runErr != nil {
			_ = store.ExportJSONL(context.Background(), sessionID, filepath.Join(runDirectory, "events.partial.jsonl"))
			if snapshot, snapshotErr := store.Snapshot(context.Background(), sessionID); snapshotErr == nil {
				_ = writeJSON(filepath.Join(runDirectory, "application-state.partial.json"), snapshot)
				cleanupAgents(snapshot)
			}
		}
	}()

	barriers, err := startBarrierService()
	if err != nil {
		return result, err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		runErr = errors.Join(runErr, barriers.stop(shutdownCtx))
	}()

	serverLog, err := os.OpenFile(filepath.Join(runDirectory, "temporal-server.log"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return result, fmt.Errorf("create Temporal server log: %w", err)
	}
	temporalServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath,
		DBFilename:   filepath.Join(runDirectory, "temporal.db"),
		LogLevel:     "warn",
		LogFormat:    "pretty",
		Stdout:       serverLog,
		Stderr:       serverLog,
	})
	if err != nil {
		_ = serverLog.Close()
		return result, fmt.Errorf("start Temporal dev server: %w", err)
	}
	defer func() {
		temporalServer.Client().Close()
		runErr = errors.Join(runErr, temporalServer.Stop(), serverLog.Close())
	}()

	taskQueue := "post-exec-gap-" + options.RunID
	worker1, err := startWorkerProcess(
		options, runDirectory, temporalServer.FrontendHostPort(), taskQueue, store.Path(), barriers.URL(), "worker-1",
	)
	if err != nil {
		return result, err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		runErr = errors.Join(runErr, worker1.stop(shutdownCtx))
	}()

	workflowID := "worker-death-post-exec-gap/" + options.RunID
	workflowRun, err := temporalServer.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
	}, temporalagent.WorkflowName, temporalagent.WorkflowInput{
		SessionID: sessionID, Mode: workstore.ModeFenced,
		ReplacePendingLaunchOnRetry:     postExecOptions.Arm == PostExecFencedReplacement,
		BlockAttempt1BeforeRegistration: true,
		BlockAttempt1BeforeHeartbeat:    true,
	})
	if err != nil {
		return result, fmt.Errorf("start post-exec Workflow: %w", err)
	}
	result.WorkflowID = workflowID
	result.WorkflowRunID = workflowRun.GetRunID()

	if err := waitForEvent(ctx, store, sessionID, "executor_launch_decided", 1, 1); err != nil {
		return result, err
	}
	pendingExecutor, err := requirePendingLaunchSnapshot(ctx, store, sessionID)
	if err != nil {
		return result, err
	}
	discovered, err = discoverUnregisteredChild(ctx, barriers, store, sessionID, pendingExecutor)
	if err != nil {
		return result, err
	}
	activityPoint := "activity-before-first-heartbeat/1"
	if err := waitExpectedBarrierAndRecord(ctx, barriers, store, sessionID, failureinject.Arrival{
		ID: "worker-1:" + activityPoint, Point: activityPoint, SessionID: sessionID,
		OwnerTokenHash: pendingExecutor.OwnerTokenHash, Generation: 1, ActorID: "worker-1",
	}); err != nil {
		return result, err
	}
	if err := capturePostExecBoundary(
		ctx, runDirectory, store, sessionID, discovered, worker1.command.Process.Pid,
	); err != nil {
		return result, err
	}
	if err := worker1.killAndWait(); err != nil {
		return result, fmt.Errorf("inject Worker SIGKILL: %w", err)
	}
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: "worker_killed", SessionID: sessionID, Generation: 1,
		WorkerID: "worker-1", Attempt: 1, PID: worker1.command.Process.Pid,
		Details: map[string]string{"signal": "SIGKILL", "boundary": "after-exec-before-process-registration"},
	}); err != nil {
		return result, err
	}
	if err := requireProcessIdentity(discovered); err != nil {
		return result, fmt.Errorf("child did not survive Worker kill: %w", err)
	}
	if err := recordProcessObservation(ctx, store, sessionID, "child_alive_after_worker_kill", discovered); err != nil {
		return result, err
	}

	worker2, err := startWorkerProcess(
		options, runDirectory, temporalServer.FrontendHostPort(), taskQueue, store.Path(), barriers.URL(), "worker-2",
	)
	if err != nil {
		return result, err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		runErr = errors.Join(runErr, worker2.stop(shutdownCtx))
	}()

	var workflowOutcome *workstore.Outcome
	switch postExecOptions.Arm {
	case PostExecAttachControl:
		workflowOutcome, err = runPostExecAttach(ctx, workflowRun, barriers, store, sessionID, discovered)
	case PostExecFencedReplacement:
		workflowOutcome, err = runPostExecFenced(ctx, workflowRun, barriers, store, sessionID, discovered)
	}
	if err != nil {
		return result, err
	}
	result.Outcome = workflowOutcome

	finalSnapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return result, err
	}
	result.Verdict = VerifyPostExecGap(postExecOptions.Arm, finalSnapshot, workflowOutcome)
	if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
		return result, fmt.Errorf("post-exec oracle failed: %s", strings.Join(result.Verdict.Failures, "; "))
	}
	manifest := PostExecGapManifest{
		SchemaVersion: 1, Experiment: "worker-death-post-exec-pre-registration", RunID: options.RunID,
		Arm: postExecOptions.Arm, Mode: workstore.ModeFenced, StartedAt: startedAt, CompletedAt: time.Now().UTC(),
		TemporalCLI:    temporalCLIVersion(ctx, options.TemporalPath),
		TemporalServer: temporalServerVersion(ctx, temporalServer.Client()),
		TemporalAPI:    moduleVersion("go.temporal.io/api"), TemporalSDK: moduleVersion("go.temporal.io/sdk"),
		GoVersion: runtime.Version(), AgentProtocolBuild: agentProtocolBuild,
		FailureBoundary:    "Worker SIGKILL after child exec and exact child-side barrier arrival, before process registration",
		Invariant:          "retry leaves one accepted effect/outcome; reuse does not launch a competitor and replacement rejects obsolete authority",
		Falsifier:          "child identity is absent at the boundary, attach launches a competitor, replacement accepts stale registration, or obsolete identity remains after rejection",
		CredentialBoundary: "the generation capability is rejected by the application store; arbitrary external credentials are not revoked by Temporal or this experiment",
		CleanupBoundary:    "single-host Linux PID plus process-start identity; the cooperative stale child exits after registration rejection",
	}
	if err := preserveEvidence(
		ctx, runDirectory, store, sessionID, finalSnapshot, result.Verdict, manifest,
		temporalServer.Client(), workflowID, workflowRun.GetRunID(),
	); err != nil {
		return result, err
	}
	cleanupAgents(finalSnapshot)
	return result, nil
}

func discoverUnregisteredChild(
	ctx context.Context,
	barriers *barrierService,
	store *workstore.Store,
	sessionID string,
	pending workstore.Executor,
) (observedProcess, error) {
	point := "before-registration/1"
	actorID := fmt.Sprintf("agent/%s/g1/%s", sessionID, pending.OwnerTokenHash[:12])
	arrivals, err := barriers.coordinator.WaitForArrivals(ctx, point, 1)
	if err != nil {
		return observedProcess{}, fmt.Errorf("wait for child pre-registration barrier: %w", err)
	}
	arrival := arrivals[0]
	expected := failureinject.Arrival{
		ID: actorID + ":" + point, Point: point, SessionID: sessionID,
		OwnerTokenHash: pending.OwnerTokenHash, Generation: 1, ActorID: actorID,
		PID: arrival.PID, ProcessStart: arrival.ProcessStart,
	}
	if arrival.PID <= 0 || arrival.ProcessStart == "" || !arrivalMatchesExpected(arrival, expected) {
		return observedProcess{}, fmt.Errorf("pre-registration barrier identity mismatch: %+v", arrival)
	}
	observed := observedProcess{
		PID: arrival.PID, StartIdentity: arrival.ProcessStart, ActorID: actorID,
		OwnerHash: pending.OwnerTokenHash, DiscoveredAt: arrival.Time,
	}
	if err := requireProcessIdentity(observed); err != nil {
		return observedProcess{}, err
	}
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: "barrier_observed", SessionID: sessionID, Generation: 1,
		OwnerTokenHash: pending.OwnerTokenHash, Attempt: 1, PID: observed.PID,
		Details: map[string]string{
			"point": point, "actor_id": actorID, "process_start": observed.StartIdentity,
			"barrier_arrived_at": arrival.Time.Format(time.RFC3339Nano),
		},
	}); err != nil {
		return observedProcess{}, err
	}
	if err := recordProcessObservation(ctx, store, sessionID, "unregistered_child_discovered", observed); err != nil {
		return observedProcess{}, err
	}
	snapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return observedProcess{}, err
	}
	if len(snapshot.Executors) != 1 || snapshot.Executors[0].Status != workstore.ExecutorStatusLaunchPending ||
		snapshot.Executors[0].PID != 0 || snapshot.Executors[0].ProcessStart != "" {
		return observedProcess{}, fmt.Errorf("child registered before injected boundary: %+v", snapshot.Executors)
	}
	return observed, nil
}

func capturePostExecBoundary(
	ctx context.Context,
	runDirectory string,
	store *workstore.Store,
	sessionID string,
	process observedProcess,
	workerPID int,
) error {
	if workerPID <= 0 {
		return errors.New("capture post-exec boundary requires Worker PID")
	}
	if err := requireProcessIdentity(process); err != nil {
		return fmt.Errorf("capture live child identity: %w", err)
	}
	snapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return err
	}
	if len(snapshot.Executors) != 1 || snapshot.ActiveGeneration != 1 ||
		snapshot.ActiveOwnerTokenHash != process.OwnerHash ||
		snapshot.Executors[0].Generation != 1 ||
		snapshot.Executors[0].Status != workstore.ExecutorStatusLaunchPending ||
		snapshot.Executors[0].PID != 0 || snapshot.Executors[0].ProcessStart != "" {
		return fmt.Errorf("post-exec boundary store state is not one unregistered pending executor: %+v", snapshot.Executors)
	}
	evidence := PostExecBoundaryEvidence{
		CapturedAt: time.Now().UTC(), WorkerPID: workerPID,
		Child: PostExecProcessObservation{
			PID: process.PID, ProcessStart: process.StartIdentity, ActorID: process.ActorID,
			OwnerTokenHash: process.OwnerHash, BarrierArrivedAt: process.DiscoveredAt,
		},
		Store: snapshot,
	}
	return writeJSON(filepath.Join(runDirectory, "pre-kill-state.json"), evidence)
}

func runPostExecAttach(
	ctx context.Context,
	workflowRun client.WorkflowRun,
	barriers *barrierService,
	store *workstore.Store,
	sessionID string,
	discovered observedProcess,
) (*workstore.Outcome, error) {
	if err := waitForEvent(ctx, store, sessionID, "activity_reattached", 2, 1); err != nil {
		return nil, fmt.Errorf("wait for post-exec reattachment: %w", err)
	}
	if err := requireProcessIdentity(discovered); err != nil {
		return nil, fmt.Errorf("reattached child identity: %w", err)
	}
	if err := barriers.coordinator.Release("before-registration/1"); err != nil {
		return nil, err
	}
	if err := releaseAgentToOutcome(ctx, barriers, store, sessionID, 1); err != nil {
		return nil, err
	}
	var outcome workstore.Outcome
	if err := workflowRun.Get(ctx, &outcome); err != nil {
		return nil, fmt.Errorf("wait for attached post-exec Workflow: %w", err)
	}
	if err := observeProcessIdentityGone(ctx, store, sessionID, "child_identity_gone", discovered); err != nil {
		return nil, err
	}
	return &outcome, nil
}

func runPostExecFenced(
	ctx context.Context,
	workflowRun client.WorkflowRun,
	barriers *barrierService,
	store *workstore.Store,
	sessionID string,
	discovered observedProcess,
) (*workstore.Outcome, error) {
	if err := waitForEvent(ctx, store, sessionID, "pending_launch_replaced", 2, 2); err != nil {
		return nil, fmt.Errorf("wait for pending launch replacement: %w", err)
	}
	if err := requireProcessIdentity(discovered); err != nil {
		return nil, fmt.Errorf("obsolete child before release: %w", err)
	}
	if err := recordProcessObservation(ctx, store, sessionID, "stale_child_alive_after_replacement", discovered); err != nil {
		return nil, err
	}
	if err := releaseAgentToOutcome(ctx, barriers, store, sessionID, 2); err != nil {
		return nil, err
	}
	var outcome workstore.Outcome
	if err := workflowRun.Get(ctx, &outcome); err != nil {
		return nil, fmt.Errorf("wait for replacement post-exec Workflow: %w", err)
	}
	if err := barriers.coordinator.Release("before-registration/1"); err != nil {
		return nil, err
	}
	if err := waitForEvent(ctx, store, sessionID, "process_registration_rejected_stale", 1, 1); err != nil {
		return nil, fmt.Errorf("wait for stale process registration rejection: %w", err)
	}
	if err := observeProcessIdentityGone(ctx, store, sessionID, "stale_child_identity_gone", discovered); err != nil {
		return nil, err
	}
	return &outcome, nil
}

func releaseAgentToOutcome(
	ctx context.Context,
	barriers *barrierService,
	store *workstore.Store,
	sessionID string,
	generation uint64,
) error {
	for _, phase := range []string{"before-effect", "before-completion"} {
		point := fmt.Sprintf("%s/%d", phase, generation)
		if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, point); err != nil {
			return err
		}
		if err := barriers.coordinator.Release(point); err != nil {
			return err
		}
	}
	return waitForEvent(ctx, store, sessionID, "outcome_accepted", 0, generation)
}

func recordProcessObservation(
	ctx context.Context,
	store *workstore.Store,
	sessionID, kind string,
	process observedProcess,
) error {
	return store.RecordObservation(ctx, workstore.Event{
		Kind: kind, SessionID: sessionID, Generation: 1, OwnerTokenHash: process.OwnerHash,
		WorkerID: "worker-1", Attempt: 1, PID: process.PID,
		Details: map[string]string{"process_start": process.StartIdentity, "actor_id": process.ActorID},
	})
}

func requireProcessIdentity(process observedProcess) error {
	current, err := agentprocess.ProcessStartIdentity(process.PID)
	if err != nil {
		return err
	}
	if current != process.StartIdentity {
		return fmt.Errorf("PID %d start identity = %q, want %q", process.PID, current, process.StartIdentity)
	}
	return nil
}

func observeProcessIdentityGone(
	ctx context.Context,
	store *workstore.Store,
	sessionID, kind string,
	process observedProcess,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(storePollInterval)
	defer ticker.Stop()
	for {
		current, err := agentprocess.ProcessStartIdentity(process.PID)
		if errors.Is(err, os.ErrNotExist) || (err == nil && current != process.StartIdentity) {
			return recordProcessObservation(ctx, store, sessionID, kind, process)
		}
		if err != nil {
			return fmt.Errorf("observe child process identity: %w", err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for child PID %d identity to disappear: %w", process.PID, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func cleanupExactProcess(process observedProcess) {
	if process.PID <= 0 || process.StartIdentity == "" {
		return
	}
	current, err := agentprocess.ProcessStartIdentity(process.PID)
	if err == nil && current == process.StartIdentity {
		_ = syscall.Kill(process.PID, syscall.SIGKILL)
	}
}
