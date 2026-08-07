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
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

const defaultRunTimeout = 45 * time.Second

type failureRecord struct {
	Time  time.Time `json:"time"`
	Error string    `json:"error"`
}

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
			_ = writeJSON(filepath.Join(runDirectory, "failure.json"), failureRecord{
				Time: time.Now().UTC(), Error: runErr.Error(),
			})
		}
	}()

	startedAt := time.Now().UTC()
	runtimeDirectory := filepath.Join(runDirectory, "runtime")
	if err := os.Mkdir(runtimeDirectory, 0o750); err != nil {
		return result, fmt.Errorf("create runtime directory: %w", err)
	}
	configuration := DestinationConfig{
		DatabasePath: filepath.Join(runtimeDirectory, "effects.db"),
		GitPath:      filepath.Join(runtimeDirectory, "repository"),
		ArtifactPath: filepath.Join(runDirectory, "artifacts"),
	}
	if usesHTTPDestination(options.Destination) {
		httpDestination, err := StartHTTPDestination()
		if err != nil {
			return result, err
		}
		defer httpDestination.Close()
		configuration.HTTPURL = httpDestination.URL()
	}
	if err := prepareDestination(ctx, options.Destination, configuration); err != nil {
		return result, err
	}

	barriers, err := startBarrierService()
	if err != nil {
		return result, err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		runErr = errors.Join(runErr, barriers.stop(shutdownCtx))
	}()

	serverLog, err := os.OpenFile(
		filepath.Join(runDirectory, "temporal-server.log"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600,
	)
	if err != nil {
		return result, fmt.Errorf("create Temporal server log: %w", err)
	}
	temporalServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath,
		DBFilename:   filepath.Join(runtimeDirectory, "temporal.db"),
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

	taskQueue := "external-effects-" + options.RunID
	worker1, err := startWorkerProcess(
		options.WorkerBinary, runDirectory, temporalServer.FrontendHostPort(), taskQueue, "worker-1",
	)
	if err != nil {
		return result, err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		runErr = errors.Join(runErr, worker1.stop(shutdownCtx))
	}()

	effectID := "effect-" + options.RunID
	workflowID := "external-effects/" + options.RunID
	observationStorePath := filepath.Join(runtimeDirectory, "observations.db")
	workflowRun, err := temporalServer.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
	}, workflowName, WorkflowInput{
		Destination: options.Destination,
		Mode:        options.Mode,
		EffectID:    effectID,
		Payload:     "agent-output-v1",
		Config:      configuration,
		BarrierURL:  barriers.URL(),
		StorePath:   observationStorePath,
	})
	if err != nil {
		return result, fmt.Errorf("start external-effect Workflow: %w", err)
	}
	result.WorkflowID = workflowID
	result.WorkflowRunID = workflowRun.GetRunID()
	evidence := Evidence{Destination: options.Destination, Mode: options.Mode, EffectID: effectID}
	defer func() {
		if runErr == nil {
			return
		}
		captureCtx, captureCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer captureCancel()
		var captureErrors []string
		if attempts, err := readAttempts(observationStorePath); err == nil {
			evidence.Attempts = attempts
		} else {
			captureErrors = append(captureErrors, "attempts: "+err.Error())
		}
		if state, err := snapshotDestination(captureCtx, options.Destination, configuration); err == nil {
			evidence.DestinationState = state
		} else {
			captureErrors = append(captureErrors, "destination: "+err.Error())
		}
		if history, err := readHistory(
			captureCtx, temporalServer.Client(), workflowID, workflowRun.GetRunID(),
		); err == nil {
			evidence.History = summarizeHistory(history)
			if err := writeHistory(filepath.Join(runDirectory, "temporal-history.partial.json"), history); err != nil {
				captureErrors = append(captureErrors, "history write: "+err.Error())
			}
		} else {
			captureErrors = append(captureErrors, "history: "+err.Error())
		}
		if err := writeJSON(filepath.Join(runDirectory, "observations.partial.json"), evidence); err != nil {
			captureErrors = append(captureErrors, "observations write: "+err.Error())
		}
		if len(captureErrors) > 0 {
			_ = writeJSON(filepath.Join(runDirectory, "evidence-capture-errors.json"), captureErrors)
		}
	}()

	arrival, err := awaitPostEffectBarrier(ctx, barriers, effectID, worker1.PID())
	if err != nil {
		return result, err
	}
	preKillState, err := snapshotDestination(ctx, options.Destination, configuration)
	if err != nil {
		return result, fmt.Errorf("snapshot destination before Worker kill: %w", err)
	}
	preKillAttempts, err := readAttempts(observationStorePath)
	if err != nil {
		return result, fmt.Errorf("read attempt 1 before Worker kill: %w", err)
	}
	if len(preKillState.PhysicalEffects) != 1 || len(preKillAttempts) != 1 ||
		preKillAttempts[0].EffectRespondedAt.IsZero() {
		return result, fmt.Errorf(
			"post-effect barrier lacked durable effect evidence: effects=%d attempts=%d",
			len(preKillState.PhysicalEffects), len(preKillAttempts),
		)
	}
	exitStatus, err := worker1.killAndWait()
	if err != nil {
		return result, fmt.Errorf("inject Worker SIGKILL: %w", err)
	}
	kill := KillObservation{
		BarrierObservedAt: arrival.Time, KilledAt: time.Now().UTC(), WorkerID: "worker-1",
		PID: worker1.PID(), Signal: "SIGKILL", ExitStatus: exitStatus,
	}
	evidence.Kill = kill

	worker2, err := startWorkerProcess(
		options.WorkerBinary, runDirectory, temporalServer.FrontendHostPort(), taskQueue, "worker-2",
	)
	if err != nil {
		return result, err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		runErr = errors.Join(runErr, worker2.stop(shutdownCtx))
	}()

	if err := workflowRun.Get(ctx, &evidence.WorkflowOutcome); err != nil {
		return result, fmt.Errorf("wait for external-effect Workflow: %w", err)
	}
	evidence.Attempts, err = readAttempts(observationStorePath)
	if err != nil {
		return result, fmt.Errorf("read attempt observations: %w", err)
	}
	evidence.DestinationState, err = snapshotDestination(ctx, options.Destination, configuration)
	if err != nil {
		return result, fmt.Errorf("snapshot final destination state: %w", err)
	}
	history, err := readHistory(ctx, temporalServer.Client(), workflowID, workflowRun.GetRunID())
	if err != nil {
		return result, err
	}
	evidence.History = summarizeHistory(history)
	result.Verdict = Verify(evidence)

	if options.Destination == DestinationGit {
		if err := exportGitBundle(ctx, configuration.GitPath, filepath.Join(runDirectory, "destination.git.bundle")); err != nil {
			return result, err
		}
	}
	manifest := Manifest{
		SchemaVersion: 1, Experiment: "external-effect-ambiguity", RunID: options.RunID,
		Destination: options.Destination, Mode: options.Mode, WorkflowID: workflowID,
		WorkflowRunID: workflowRun.GetRunID(), TaskQueue: taskQueue, ActivityID: activityID,
		EffectID: effectID, StartedAt: startedAt, CompletedAt: time.Now().UTC(),
		TemporalCLI:    temporalCLIVersion(ctx, options.TemporalPath),
		TemporalServer: temporalServerVersion(ctx, temporalServer.Client()),
		TemporalAPI:    moduleVersion("go.temporal.io/api"), TemporalSDK: moduleVersion("go.temporal.io/sdk"),
		GoVersion:       runtime.Version(),
		FailureBoundary: "Worker 1 SIGKILL after destination effect and exact barrier arrival, before Activity return",
		Invariant:       "one logical effect ID leaves one physical effect and one receipt after Activity retry",
		Falsifier:       destinationFalsifier(options.Destination),
	}
	if err := preserveEvidence(runDirectory, evidence, result.Verdict, manifest, history); err != nil {
		return result, err
	}
	if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
		return result, fmt.Errorf("experiment oracle failed: %s", strings.Join(result.Verdict.Failures, "; "))
	}
	return result, nil
}

func awaitPostEffectBarrier(
	ctx context.Context,
	barriers *barrierService,
	effectID string,
	workerPID int,
) (failureinject.Arrival, error) {
	arrivals, err := barriers.coordinator.WaitForArrivals(ctx, "after-effect/attempt-1", 1)
	if err != nil {
		return failureinject.Arrival{}, fmt.Errorf("wait for post-effect barrier: %w", err)
	}
	arrival := arrivals[0]
	if arrival.ID != effectID+"/attempt-1" || arrival.SessionID != effectID ||
		arrival.ActorID != "worker-1" || arrival.PID != workerPID {
		return failureinject.Arrival{}, fmt.Errorf("post-effect barrier identity mismatch: %+v", arrival)
	}
	return arrival, nil
}

func validateOptions(options Options) error {
	if !options.Destination.Valid() || !options.Mode.Valid() || options.TemporalPath == "" ||
		options.WorkerBinary == "" || options.OutputRoot == "" || !safePathComponent(options.RunID) {
		return errors.New("run requires valid destination, mode, Temporal/Worker binaries, output root, and safe run ID")
	}
	for _, path := range []string{options.TemporalPath, options.WorkerBinary} {
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

func usesHTTPDestination(destination Destination) bool {
	return destination == DestinationIdempotentAPI || destination == DestinationNonIdempotentAPI ||
		destination == DestinationMessage
}

func destinationFalsifier(destination Destination) string {
	switch destination {
	case DestinationNonIdempotentAPI:
		return "retry issues a second POST despite a visible correlation receipt, or concurrent same-ID callers can pass the lookup"
	case DestinationArtifact:
		return "retry creates a second blob/reference or accepts conflicting content; blob/reference partial publication remains out of scope"
	case DestinationGit:
		return "retry creates a second effect commit or accepts conflicting marker content; concurrent worktree writers remain out of scope"
	default:
		return "retry creates a second physical effect or the destination returns a different receipt for the stable logical ID"
	}
}
