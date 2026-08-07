package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/agentprocess"
	"github.com/temporalio-labs/agent-durability-lab/internal/failureinject"
	"github.com/temporalio-labs/agent-durability-lab/internal/temporalagent"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
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

	runDirectory := filepath.Join(options.OutputRoot, options.RunID)
	if err := os.MkdirAll(options.OutputRoot, 0o750); err != nil {
		return Result{}, fmt.Errorf("create evidence root: %w", err)
	}
	if err := os.Mkdir(runDirectory, 0o750); err != nil {
		return Result{}, fmt.Errorf("create append-only run directory: %w", err)
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
			if snapshot, err := store.Snapshot(context.Background(), sessionID); err == nil {
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

	taskQueue := "worker-death-" + options.RunID
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

	workflowID := "worker-death/" + options.RunID
	workflowRun, err := temporalServer.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
	}, temporalagent.WorkflowName, temporalagent.WorkflowInput{
		SessionID:                    sessionID,
		Mode:                         options.Mode,
		ReplaceOwnerOnRetry:          options.Mode == workstore.ModeFenced,
		BlockAttempt1BeforeHeartbeat: true,
	})
	if err != nil {
		return result, fmt.Errorf("start experiment Workflow: %w", err)
	}
	result.WorkflowID = workflowID
	result.WorkflowRunID = workflowRun.GetRunID()

	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-effect/1"); err != nil {
		return result, err
	}
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "activity-before-first-heartbeat/1"); err != nil {
		return result, err
	}
	beforeKill, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return result, err
	}
	oldExecutor, err := executorByGeneration(beforeKill, 1)
	if err != nil {
		return result, err
	}
	if err := worker1.killAndWait(); err != nil {
		return result, fmt.Errorf("inject Worker SIGKILL: %w", err)
	}
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: "worker_killed", SessionID: sessionID, Generation: 1,
		WorkerID: "worker-1", Attempt: 1, PID: worker1.command.Process.Pid,
		Details: map[string]string{"signal": "SIGKILL", "boundary": "before-first-heartbeat"},
	}); err != nil {
		return result, err
	}
	currentIdentity, err := agentprocess.ProcessStartIdentity(oldExecutor.PID)
	if err != nil || currentIdentity != oldExecutor.ProcessStart {
		return result, fmt.Errorf("surviving child identity mismatch: observed %q, recorded %q: %w", currentIdentity, oldExecutor.ProcessStart, err)
	}
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: "child_alive_after_worker_kill", SessionID: sessionID, Generation: 1,
		PID: oldExecutor.PID, Details: map[string]string{"process_start": currentIdentity},
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

	switch options.Mode {
	case workstore.ModeUnsafe:
		runErr = runUnsafeSequence(ctx, barriers, store, sessionID)
	case workstore.ModeReattach:
		runErr = runReattachSequence(ctx, barriers, store, sessionID)
	case workstore.ModeFenced:
		runErr = runFencedSequence(ctx, barriers, store, sessionID)
	}
	if runErr != nil {
		return result, runErr
	}

	if err := workflowRun.Get(ctx, &result.Outcome); err != nil {
		return result, fmt.Errorf("wait for experiment Workflow: %w", err)
	}
	finalSnapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return result, err
	}
	result.Verdict = Verify(options.Mode, finalSnapshot, result.Outcome)
	if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
		return result, fmt.Errorf("experiment oracle failed: %s", strings.Join(result.Verdict.Failures, "; "))
	}

	manifest := Manifest{
		SchemaVersion: 1, Experiment: "worker-death-surviving-agent", RunID: options.RunID,
		Mode: options.Mode, StartedAt: startedAt, CompletedAt: time.Now().UTC(),
		TemporalCLI: temporalCLIVersion(ctx, options.TemporalPath),
		TemporalSDK: moduleVersion("go.temporal.io/sdk"), GoVersion: runtime.Version(),
		AgentProtocolBuild: agentProtocolBuild,
		FailureBoundary:    "Worker SIGKILL after child progress and before the first Activity heartbeat",
		Invariant:          "retry does not create an unauthorized competing writer and only one fenced outcome is accepted",
		Falsifier:          "safe mode launches a second session without explicit replacement, loses the child, accepts a stale write/outcome, or changes the accepted outcome",
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

func runUnsafeSequence(ctx context.Context, barriers *barrierService, store *workstore.Store, sessionID string) error {
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-effect/2"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-effect/1"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-effect/2"); err != nil {
		return err
	}
	if err := waitForSnapshot(ctx, store, sessionID, func(snapshot workstore.Snapshot) bool {
		return len(snapshot.Effects) == 2
	}); err != nil {
		return fmt.Errorf("wait for duplicate unsafe effects: %w", err)
	}
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-completion/1"); err != nil {
		return err
	}
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-completion/2"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-completion/1"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-completion/2"); err != nil {
		return err
	}
	return waitForSnapshot(ctx, store, sessionID, func(snapshot workstore.Snapshot) bool {
		return snapshot.Outcome != nil && eventCount(snapshot.Events, "completion_rejected_terminal") == 1
	})
}

func runReattachSequence(ctx context.Context, barriers *barrierService, store *workstore.Store, sessionID string) error {
	if err := waitForEvent(ctx, store, sessionID, "activity_reattached", 2, 1); err != nil {
		return fmt.Errorf("wait for retry reattachment: %w", err)
	}
	if err := barriers.coordinator.Release("before-effect/1"); err != nil {
		return err
	}
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-completion/1"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-completion/1"); err != nil {
		return err
	}
	return waitForEvent(ctx, store, sessionID, "outcome_accepted", 0, 1)
}

func runFencedSequence(ctx context.Context, barriers *barrierService, store *workstore.Store, sessionID string) error {
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-effect/2"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-effect/2"); err != nil {
		return err
	}
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-completion/2"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-completion/2"); err != nil {
		return err
	}
	if err := waitForEvent(ctx, store, sessionID, "outcome_accepted", 0, 2); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-effect/1"); err != nil {
		return err
	}
	if err := waitBarrierAndRecord(ctx, barriers, store, sessionID, "before-completion/1"); err != nil {
		return err
	}
	if err := barriers.coordinator.Release("before-completion/1"); err != nil {
		return err
	}
	if err := waitForEvent(ctx, store, sessionID, "effect_rejected_stale", 0, 1); err != nil {
		return err
	}
	return waitForEvent(ctx, store, sessionID, "completion_rejected_stale", 0, 1)
}

func waitBarrierAndRecord(
	ctx context.Context,
	barriers *barrierService,
	store *workstore.Store,
	sessionID, point string,
) error {
	arrivals, err := barriers.coordinator.WaitForArrivals(ctx, point, 1)
	if err != nil {
		return fmt.Errorf("wait for barrier %q: %w", point, err)
	}
	arrival := arrivals[len(arrivals)-1]
	return store.RecordObservation(ctx, workstore.Event{
		Kind: "barrier_observed", SessionID: sessionID, Generation: arrival.Generation,
		OwnerTokenHash: arrival.OwnerTokenHash, PID: arrival.PID,
		Details: map[string]string{"point": point, "actor_id": arrival.ActorID},
	})
}

func waitExpectedBarrierAndRecord(
	ctx context.Context,
	barriers *barrierService,
	store *workstore.Store,
	sessionID string,
	expected failureinject.Arrival,
) error {
	checked := 0
	for {
		arrivals, err := barriers.coordinator.WaitForArrivals(ctx, expected.Point, checked+1)
		if err != nil {
			return fmt.Errorf("wait for expected barrier %q: %w", expected.Point, err)
		}
		for _, arrival := range arrivals[checked:] {
			if arrivalMatchesExpected(arrival, expected) {
				return store.RecordObservation(ctx, workstore.Event{
					Kind: "barrier_observed", SessionID: sessionID, Generation: arrival.Generation,
					OwnerTokenHash: arrival.OwnerTokenHash, PID: arrival.PID,
					Details: map[string]string{"point": arrival.Point, "actor_id": arrival.ActorID},
				})
			}
			if err := store.RecordObservation(ctx, workstore.Event{
				Kind: "barrier_arrival_ignored", SessionID: sessionID, Generation: arrival.Generation,
				OwnerTokenHash: arrival.OwnerTokenHash, PID: arrival.PID,
				Details: map[string]string{"point": arrival.Point, "actor_id": arrival.ActorID, "reason": "identity_mismatch"},
			}); err != nil {
				return err
			}
		}
		checked = len(arrivals)
	}
}

func waitForEvent(
	ctx context.Context,
	store *workstore.Store,
	sessionID, kind string,
	attempt int32,
	generation uint64,
) error {
	return waitForSnapshot(ctx, store, sessionID, func(snapshot workstore.Snapshot) bool {
		return hasEvent(snapshot.Events, kind, attempt, generation)
	})
}

func waitForSnapshot(
	ctx context.Context,
	store *workstore.Store,
	sessionID string,
	ready func(workstore.Snapshot) bool,
) error {
	ticker := time.NewTicker(storePollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := store.Snapshot(ctx, sessionID)
		if err != nil && !errors.Is(err, workstore.ErrSessionNotFound) {
			return err
		}
		if err == nil && ready(snapshot) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func executorByGeneration(snapshot workstore.Snapshot, generation uint64) (workstore.Executor, error) {
	for _, executor := range snapshot.Executors {
		if executor.Generation == generation {
			if executor.PID <= 0 || executor.ProcessStart == "" {
				return workstore.Executor{}, fmt.Errorf("generation %d process identity is incomplete", generation)
			}
			return executor, nil
		}
	}
	return workstore.Executor{}, fmt.Errorf("generation %d executor not found", generation)
}

func eventCount(events []workstore.Event, kind string) int {
	count := 0
	for _, event := range events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func cleanupAgents(snapshot workstore.Snapshot) {
	for _, executor := range snapshot.Executors {
		if executor.PID <= 0 || executor.ProcessStart == "" {
			continue
		}
		identity, err := agentprocess.ProcessStartIdentity(executor.PID)
		if err == nil && identity == executor.ProcessStart {
			_ = syscall.Kill(executor.PID, syscall.SIGKILL)
		}
	}
}

func temporalCLIVersion(ctx context.Context, path string) string {
	output, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return "unknown: " + err.Error()
	}
	return strings.TrimSpace(string(output))
}

func validateOptions(options Options) error {
	if !options.Mode.Valid() || options.TemporalPath == "" || options.WorkerBinary == "" ||
		options.AgentBinary == "" || options.OutputRoot == "" || options.RunID == "" {
		return errors.New("run requires valid mode, Temporal/Worker/agent binaries, output root, and run ID")
	}
	if !safePathComponent(options.RunID) {
		return errors.New("run ID must contain only ASCII letters, digits, dot, underscore, or hyphen")
	}
	for _, path := range []string{options.TemporalPath, options.WorkerBinary, options.AgentBinary} {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("inspect binary %q: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			return fmt.Errorf("binary %q is not executable", path)
		}
	}
	return nil
}

func safePathComponent(value string) bool {
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return value != "." && value != ".."
}
