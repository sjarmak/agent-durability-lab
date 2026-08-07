package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/failureinject"
	"github.com/temporalio-labs/agent-durability-lab/internal/temporalagent"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func RunLaunchGap(parent context.Context, launchOptions LaunchGapOptions) (result LaunchGapResult, runErr error) {
	if !launchOptions.Arm.Valid() {
		return result, fmt.Errorf("launch-gap arm %q is invalid", launchOptions.Arm)
	}
	options := launchOptions.Options
	if options.Mode != workstore.ModeFenced {
		return result, errors.New("launch-gap experiment requires fenced mode")
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
	defer func() {
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

	taskQueue := "launch-gap-" + options.RunID
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

	workflowID := "launch-registration-gap/" + options.RunID
	workflowRun, err := temporalServer.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
	}, temporalagent.WorkflowName, temporalagent.WorkflowInput{
		SessionID: sessionID, Mode: workstore.ModeFenced,
		ReplacePendingLaunchOnRetry:      launchOptions.Arm == LaunchGapFencedRecovery,
		BlockAttempt1AfterLaunchDecision: true,
	})
	if err != nil {
		return result, fmt.Errorf("start launch-gap Workflow: %w", err)
	}
	result.WorkflowID = workflowID
	result.WorkflowRunID = workflowRun.GetRunID()

	if err := waitForEvent(ctx, store, sessionID, "activity_after_launch_decision", 1, 1); err != nil {
		return result, err
	}
	pendingExecutor, err := requirePendingLaunchSnapshot(ctx, store, sessionID)
	if err != nil {
		return result, err
	}
	launchPoint := "activity-after-launch-decision/1"
	if err := waitExpectedBarrierAndRecord(ctx, barriers, store, sessionID, failureinject.Arrival{
		ID: "worker-1:" + launchPoint, Point: launchPoint, SessionID: sessionID,
		OwnerTokenHash: pendingExecutor.OwnerTokenHash, Generation: 1, ActorID: "worker-1",
	}); err != nil {
		return result, err
	}
	if err := worker1.killAndWait(); err != nil {
		return result, fmt.Errorf("inject Worker SIGKILL: %w", err)
	}
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: "worker_killed", SessionID: sessionID, Generation: 1,
		WorkerID: "worker-1", Attempt: 1, PID: worker1.command.Process.Pid,
		Details: map[string]string{"signal": "SIGKILL", "boundary": "after-launch-decision-before-process-start"},
	}); err != nil {
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

	workflowCanceled := false
	var workflowOutcome *workstore.Outcome
	switch launchOptions.Arm {
	case LaunchGapControl:
		workflowCanceled, err = runLaunchGapControl(ctx, temporalServer.Client(), workflowRun, store, sessionID)
	case LaunchGapFencedRecovery:
		workflowOutcome, err = runLaunchGapRecovery(ctx, workflowRun, barriers, store, sessionID)
	}
	if err != nil {
		return result, err
	}

	finalSnapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return result, err
	}
	result.Outcome = workflowOutcome
	result.Verdict = VerifyLaunchGap(launchOptions.Arm, finalSnapshot, workflowOutcome, workflowCanceled)
	if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
		return result, fmt.Errorf("launch-gap oracle failed: %s", strings.Join(result.Verdict.Failures, "; "))
	}

	manifest := LaunchGapManifest{
		SchemaVersion: 1, Experiment: "worker-death-launch-registration-gap", RunID: options.RunID,
		Arm: launchOptions.Arm, Mode: workstore.ModeFenced, StartedAt: startedAt, CompletedAt: time.Now().UTC(),
		TemporalCLI: temporalCLIVersion(ctx, options.TemporalPath),
		TemporalSDK: moduleVersion("go.temporal.io/sdk"), GoVersion: runtime.Version(),
		AgentProtocolBuild: agentProtocolBuild,
		FailureBoundary:    "Worker SIGKILL after durable launch decision and before process start or registration",
		Invariant:          "an unregistered launch is not treated as live work; recovery accepts at most one fenced effect and outcome",
		Falsifier:          "control is reported healthy, recovery attaches to the phantom, a registered live process is replaced, or a stale generation mutates state",
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

func requirePendingLaunchSnapshot(
	ctx context.Context,
	store *workstore.Store,
	sessionID string,
) (workstore.Executor, error) {
	snapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return workstore.Executor{}, err
	}
	if len(snapshot.Executors) != 1 {
		return workstore.Executor{}, fmt.Errorf("pre-kill executors = %d; want 1", len(snapshot.Executors))
	}
	executor := snapshot.Executors[0]
	if executor.Generation != 1 || executor.Status != workstore.ExecutorStatusLaunchPending ||
		executor.PID != 0 || executor.ProcessStart != "" {
		return workstore.Executor{}, fmt.Errorf("pre-kill executor = %+v; want unregistered launch_pending generation 1", executor)
	}
	return executor, nil
}

func runLaunchGapControl(
	ctx context.Context,
	temporalClient client.Client,
	workflowRun client.WorkflowRun,
	store *workstore.Store,
	sessionID string,
) (bool, error) {
	if err := waitForEvent(ctx, store, sessionID, "activity_reattached", 2, 1); err != nil {
		return false, fmt.Errorf("wait for phantom reattachment: %w", err)
	}
	snapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return false, err
	}
	if len(snapshot.Executors) != 1 || snapshot.Executors[0].PID != 0 ||
		snapshot.Executors[0].Status != workstore.ExecutorStatusLaunchPending ||
		len(snapshot.Effects) != 0 || snapshot.Outcome != nil {
		return false, fmt.Errorf("control did not expose a PID-less pending phantom: %+v", snapshot)
	}
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: "phantom_launch_observed", SessionID: sessionID, Generation: 1,
		WorkerID: "worker-2", Attempt: 2,
		Details: map[string]string{"executor_status": workstore.ExecutorStatusLaunchPending, "pid": "0"},
	}); err != nil {
		return false, err
	}
	if err := temporalClient.CancelWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID()); err != nil {
		return false, fmt.Errorf("cancel phantom Workflow: %w", err)
	}
	var outcome workstore.Outcome
	if err := workflowRun.Get(ctx, &outcome); err == nil || !temporal.IsCanceledError(err) {
		return false, fmt.Errorf("phantom Workflow result = %+v, error = %v; want cancellation", outcome, err)
	}
	return true, nil
}

func runLaunchGapRecovery(
	ctx context.Context,
	workflowRun client.WorkflowRun,
	barriers *barrierService,
	store *workstore.Store,
	sessionID string,
) (*workstore.Outcome, error) {
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-effect/2"); err != nil {
		return nil, err
	}
	if err := barriers.coordinator.Release("before-effect/2"); err != nil {
		return nil, err
	}
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-completion/2"); err != nil {
		return nil, err
	}
	if err := barriers.coordinator.Release("before-completion/2"); err != nil {
		return nil, err
	}
	var outcome workstore.Outcome
	if err := workflowRun.Get(ctx, &outcome); err != nil {
		return nil, fmt.Errorf("wait for recovered Workflow: %w", err)
	}
	return &outcome, nil
}
