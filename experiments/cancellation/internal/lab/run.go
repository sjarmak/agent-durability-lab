package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/temporalagent"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

const (
	defaultRunTimeout = 60 * time.Second
	pollInterval      = 25 * time.Millisecond
	maxRunIDLength    = 128
)

func Run(parent context.Context, options Options) (result Result, runErr error) {
	if err := validateOptions(options); err != nil {
		return Result{}, err
	}
	if options.Timeout <= 0 {
		options.Timeout = defaultRunTimeout
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()
	startedAt := time.Now().UTC()
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
			_ = writeJSON(filepath.Join(runDirectory, "failure.json"), map[string]any{
				"time": time.Now().UTC(), "error": runErr.Error(),
			})
		}
	}()

	sessionID := "session-" + options.RunID
	store, err := workstore.Open(filepath.Join(runDirectory, "application.db"))
	if err != nil {
		return result, err
	}
	defer func() {
		if runErr != nil {
			_ = store.ExportJSONL(context.Background(), sessionID, filepath.Join(runDirectory, "events.partial.jsonl"))
		}
	}()
	barriers, err := startBarrierService()
	if err != nil {
		return result, err
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		runErr = errors.Join(runErr, barriers.stop(stopCtx))
	}()

	serverLog, err := os.OpenFile(filepath.Join(runDirectory, "temporal-server.log"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return result, fmt.Errorf("create Temporal server log: %w", err)
	}
	temporalServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath, DBFilename: filepath.Join(runDirectory, "temporal.db"),
		LogLevel: "warn", LogFormat: "pretty", Stdout: serverLog, Stderr: serverLog,
	})
	if err != nil {
		_ = serverLog.Close()
		return result, fmt.Errorf("start Temporal dev server: %w", err)
	}
	defer func() {
		temporalServer.Client().Close()
		runErr = errors.Join(runErr, temporalServer.Stop(), serverLog.Close())
	}()

	taskQueue := "cancellation-" + options.RunID
	worker1, err := startWorker(
		options, runDirectory, temporalServer.FrontendHostPort(), taskQueue, store.Path(), barriers.URL(), "worker-1",
	)
	if err != nil {
		return result, err
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		runErr = errors.Join(runErr, worker1.stop(stopCtx))
	}()

	workflowID := "cancellation/" + options.RunID
	workflowRun, err := temporalServer.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
	}, temporalagent.WorkflowName, temporalagent.WorkflowInput{
		SessionID: sessionID, Mode: workstore.ModeFenced,
		BlockAttempt1BeforeHeartbeat: !options.WaitForCancellation,
		EnableCancellationCleanup:    options.Scenario.Safe(),
		WaitForCancellation:          options.WaitForCancellation,
		SpawnToolChild:               options.Scenario.Safe(),
	})
	if err != nil {
		return result, fmt.Errorf("start cancellation Workflow: %w", err)
	}
	result.WorkflowID = workflowID
	result.WorkflowRunID = workflowRun.GetRunID()

	if _, err := waitBarrier(ctx, barriers, store, sessionID, "before-effect/1"); err != nil {
		return result, err
	}
	if options.Scenario.Safe() {
		if _, err := waitBarrier(ctx, barriers, store, sessionID, "tool-child-alive/1"); err != nil {
			return result, err
		}
	}
	if options.WaitForCancellation {
		if _, err := waitForSnapshot(ctx, store, sessionID, func(snapshot workstore.Snapshot) bool {
			return hasEvent(snapshot.Events, "activity_heartbeat_recorded")
		}); err != nil {
			return result, fmt.Errorf("wait for Activity heartbeat evidence: %w", err)
		}
	} else {
		if _, err := waitBarrier(ctx, barriers, store, sessionID, "activity-before-first-heartbeat/1"); err != nil {
			return result, err
		}
	}
	boundarySnapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return result, err
	}
	boundary := BoundaryEvidence{
		CapturedAt: time.Now().UTC(), WorkerPID: worker1.command.Process.Pid, Store: boundarySnapshot,
	}
	target, err := controlTarget(boundarySnapshot)
	if err != nil {
		return result, err
	}
	defer cleanupProcessTree(target)

	var worker2 *managedProcess
	if options.Scenario == ScenarioWorkerDeathSafe {
		if err := worker1.killAndWait(); err != nil {
			return result, err
		}
		if err := store.RecordObservation(ctx, workstore.Event{
			Kind: "worker_killed", SessionID: sessionID, Generation: target.Generation,
			PID: boundary.WorkerPID, Details: map[string]string{"signal": "SIGKILL"},
		}); err != nil {
			return result, err
		}
		worker2, err = startWorker(
			options, runDirectory, temporalServer.FrontendHostPort(), taskQueue, store.Path(), barriers.URL(), "worker-2",
		)
		if err != nil {
			return result, err
		}
		defer func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			runErr = errors.Join(runErr, worker2.stop(stopCtx))
		}()
	}
	if options.Scenario == ScenarioFrozenSafe {
		if _, err := agentprocess.Signal(agentprocess.ControlRequest{
			Target: target, Scope: agentprocess.ScopeProcessTree, Signal: agentprocess.SignalStop,
		}); err != nil {
			return result, fmt.Errorf("freeze exact process group %d: %w", target.Leader.ProcessGroupID, err)
		}
		if err := recordControlObservation(ctx, store, "process_tree_frozen", target, "SIGSTOP"); err != nil {
			return result, err
		}
	}

	if err := temporalServer.Client().CancelWorkflow(ctx, workflowID, workflowRun.GetRunID()); err != nil {
		return result, fmt.Errorf("request Workflow cancellation: %w", err)
	}
	var ignored workstore.Outcome
	if err := workflowRun.Get(ctx, &ignored); err == nil || !temporal.IsCanceledError(err) {
		return result, fmt.Errorf("workflow result = %v; want canceled", err)
	}

	if options.Scenario == ScenarioTemporalControl {
		if err := barriers.coordinator.Release("before-effect/1"); err != nil {
			return result, err
		}
		if _, err := waitBarrier(ctx, barriers, store, sessionID, "before-completion/1"); err != nil {
			return result, err
		}
		if err := barriers.coordinator.Release("before-completion/1"); err != nil {
			return result, err
		}
		if _, err := waitForSnapshot(ctx, store, sessionID, func(snapshot workstore.Snapshot) bool {
			return len(snapshot.Effects) == 1 && snapshot.Outcome != nil
		}); err != nil {
			return result, fmt.Errorf("wait for control mutation: %w", err)
		}
	} else {
		canceledSnapshot, err := waitForSnapshot(ctx, store, sessionID, func(snapshot workstore.Snapshot) bool {
			return snapshot.Cancellation != nil && hasEvent(snapshot.Events, "executor_stop_delivery_sent")
		})
		if err != nil {
			return result, fmt.Errorf("wait for durable cancellation: %w", err)
		}
		if options.Scenario == ScenarioFrozenSafe {
			if canceledSnapshot.Cancellation.Acknowledgement != nil {
				return result, errors.New("frozen executor acknowledged before resume")
			}
			if _, err := agentprocess.Signal(agentprocess.ControlRequest{
				Target: target, Scope: agentprocess.ScopeProcessTree, Signal: agentprocess.SignalContinue,
			}); err != nil && !errors.Is(err, agentprocess.ErrProcessGone) {
				return result, fmt.Errorf("resume exact process tree: %w", err)
			}
			if err := recordControlObservation(ctx, store, "process_tree_resumed", target, "SIGCONT"); err != nil {
				return result, err
			}
		}
		if _, err := waitForSnapshot(ctx, store, sessionID, func(snapshot workstore.Snapshot) bool {
			return snapshot.Cancellation != nil && snapshot.Cancellation.Acknowledgement != nil &&
				hasEvent(snapshot.Events, "tool_child_stop_received")
		}); err != nil {
			return result, fmt.Errorf("wait for cooperative process-tree stop: %w", err)
		}
		if err := waitForProcessTreeGone(ctx, target); err != nil {
			return result, err
		}
		for _, member := range target.Members {
			if err := store.RecordObservation(ctx, workstore.Event{
				Kind: "process_disposition_observed", SessionID: sessionID, Generation: target.Generation,
				OwnerTokenHash: target.OwnerTokenHash, PID: member.PID,
				Details: map[string]string{"process_start": member.StartIdentity, "disposition": "gone"},
			}); err != nil {
				return result, err
			}
		}
	}

	finalSnapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return result, err
	}
	result.Verdict = Verify(options.Scenario, finalSnapshot)
	history, err := readHistory(ctx, temporalServer.Client(), workflowID, workflowRun.GetRunID())
	if err != nil {
		return result, err
	}
	historyObservation, historyFailures := VerifyHistory(options.Scenario, options.WaitForCancellation, history)
	result.Verdict.History = historyObservation
	if len(historyFailures) > 0 {
		result.Verdict.RunValid = false
		result.Verdict.ExpectedObservation = false
		result.Verdict.Failures = append(result.Verdict.Failures, historyFailures...)
	}
	if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
		return result, fmt.Errorf("cancellation oracle failed: %s", strings.Join(result.Verdict.Failures, "; "))
	}
	manifest := Manifest{
		SchemaVersion: 1, Experiment: "precise-agent-cancellation", RunID: options.RunID,
		Scenario: options.Scenario, WaitForCancellation: options.WaitForCancellation,
		StartedAt: startedAt, CompletedAt: time.Now().UTC(), TemporalCLI: temporalCLIVersion(ctx, options.TemporalPath),
		TemporalServer: temporalServerVersion(ctx, temporalServer.Client()),
		TemporalSDK:    moduleVersion("go.temporal.io/sdk"), GoVersion: runtime.Version(),
		FailureBoundary: failureBoundary(options.Scenario),
		Invariant:       "after logical cancellation commits, no later progress, effect, outcome, or replacement is accepted",
		Falsifier:       "a safe arm accepts a post-cancel mutation, loses exact stop identity, signals a replacement, or claims acknowledgement without evidence",
	}
	if err := preserveEvidence(
		ctx, runDirectory, store, sessionID, finalSnapshot, boundary, result.Verdict, manifest,
		history,
	); err != nil {
		return result, err
	}
	return result, nil
}

func validateOptions(options Options) error {
	if !options.Scenario.Valid() || options.TemporalPath == "" || options.WorkerBinary == "" ||
		options.AgentBinary == "" || options.OutputRoot == "" {
		return errors.New("scenario, Temporal CLI, Worker, agent, output, and run identities are required")
	}
	if !validRunID(options.RunID) {
		return fmt.Errorf("run ID %q must contain 1-%d ASCII letters, digits, dots, underscores, or hyphens", options.RunID, maxRunIDLength)
	}
	return nil
}

func validRunID(runID string) bool {
	if len(runID) == 0 || len(runID) > maxRunIDLength || runID == "." || runID == ".." {
		return false
	}
	for _, character := range runID {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func waitBarrier(
	ctx context.Context,
	barriers *barrierService,
	store *workstore.Store,
	sessionID, point string,
) (failureinject.Arrival, error) {
	arrivals, err := barriers.coordinator.WaitForArrivals(ctx, point, 1)
	if err != nil {
		return failureinject.Arrival{}, fmt.Errorf("wait for barrier %q: %w", point, err)
	}
	arrival := arrivals[len(arrivals)-1]
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: "barrier_observed", SessionID: sessionID, Generation: arrival.Generation,
		OwnerTokenHash: arrival.OwnerTokenHash, PID: arrival.PID,
		Details: map[string]string{"point": point, "process_start": arrival.ProcessStart},
	}); err != nil {
		return failureinject.Arrival{}, err
	}
	return arrival, nil
}

func waitForSnapshot(
	ctx context.Context,
	store *workstore.Store,
	sessionID string,
	ready func(workstore.Snapshot) bool,
) (workstore.Snapshot, error) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := store.Snapshot(ctx, sessionID)
		if err != nil {
			return workstore.Snapshot{}, err
		}
		if ready(snapshot) {
			return snapshot, nil
		}
		select {
		case <-ctx.Done():
			return workstore.Snapshot{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func controlTarget(snapshot workstore.Snapshot) (agentprocess.ControlTarget, error) {
	if len(snapshot.Executors) != 1 {
		return agentprocess.ControlTarget{}, fmt.Errorf("boundary executors = %d; want 1", len(snapshot.Executors))
	}
	executor := snapshot.Executors[0]
	if executor.PID <= 0 || executor.ProcessStart == "" || executor.ProcessGroupID <= 0 {
		return agentprocess.ControlTarget{}, fmt.Errorf("incomplete boundary executor: %+v", executor)
	}
	leader := agentprocess.ProcessIdentity{
		PID: executor.PID, StartIdentity: executor.ProcessStart, ProcessGroupID: executor.ProcessGroupID,
	}
	target := agentprocess.ControlTarget{
		SessionID: snapshot.SessionID, Generation: executor.Generation,
		OwnerTokenHash: executor.OwnerTokenHash, Leader: leader, Members: []agentprocess.ProcessIdentity{leader},
	}
	for _, event := range snapshot.Events {
		if event.Kind != "tool_child_registered" || event.Generation != executor.Generation ||
			event.OwnerTokenHash != executor.OwnerTokenHash {
			continue
		}
		processGroupID, err := strconv.Atoi(event.Details["process_group_id"])
		if err != nil || event.PID <= 0 || event.Details["process_start"] == "" {
			return agentprocess.ControlTarget{}, fmt.Errorf("invalid tool-child event %d", event.Sequence)
		}
		target.Members = append(target.Members, agentprocess.ProcessIdentity{
			PID: event.PID, StartIdentity: event.Details["process_start"], ProcessGroupID: processGroupID,
		})
	}
	return target, nil
}

func waitForProcessTreeGone(ctx context.Context, target agentprocess.ControlTarget) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		allGone := true
		for _, member := range target.Members {
			disposition, err := agentprocess.Probe(member)
			if err != nil && !errors.Is(err, agentprocess.ErrProcessIdentityMismatch) {
				return err
			}
			if disposition != agentprocess.DispositionGone {
				allGone = false
			}
		}
		if allGone {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for process tree to exit: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func recordControlObservation(
	ctx context.Context,
	store *workstore.Store,
	kind string,
	target agentprocess.ControlTarget,
	signal string,
) error {
	return store.RecordObservation(ctx, workstore.Event{
		Kind: kind, SessionID: target.SessionID, Generation: target.Generation,
		OwnerTokenHash: target.OwnerTokenHash, PID: target.Leader.PID,
		Details: map[string]string{
			"signal": signal, "process_start": target.Leader.StartIdentity,
			"process_group_id": strconv.Itoa(target.Leader.ProcessGroupID),
		},
	})
}

func cleanupProcessTree(target agentprocess.ControlTarget) {
	_, _ = agentprocess.Signal(agentprocess.ControlRequest{
		Target: target, Scope: agentprocess.ScopeProcessTree, Signal: agentprocess.SignalKill,
	})
}

func failureBoundary(scenario Scenario) string {
	switch scenario {
	case ScenarioTemporalControl:
		return "Workflow canceled while detached child is blocked immediately before its first effect"
	case ScenarioHealthySafe:
		return "healthy Worker receives Workflow cancellation while leader and tool child are blocked"
	case ScenarioWorkerDeathSafe:
		return "Worker receives SIGKILL after leader and tool child register; Workflow is then canceled"
	case ScenarioFrozenSafe:
		return "agent process group receives SIGSTOP before Workflow cancellation and resumes after durable revocation"
	default:
		return "unknown"
	}
}

func temporalCLIVersion(ctx context.Context, path string) string {
	output, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return "unknown: " + err.Error()
	}
	return strings.TrimSpace(string(output))
}
