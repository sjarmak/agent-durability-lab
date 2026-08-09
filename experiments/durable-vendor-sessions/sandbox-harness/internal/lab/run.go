package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/sandbox-harness/evidenceadapter"
	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/sandbox-harness/internal/provider"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	sandboxworkflow "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	workflowSDK "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	upstreamCommit  = "e8a88540d9523a3d9070860913567670194bacc1"
	upstreamVersion = "v0.0.0-20260804043157-e8a88540d952"
	parentWorkflow  = "SandboxHarnessParentWorkflow"
)

var ambiguousOperations = []provider.Operation{
	provider.OperationStart,
	provider.OperationCommand,
	provider.OperationSnapshot,
	provider.OperationStop,
}

type Options struct {
	EvidenceRoot string
	TemporalPath string
	Trials       int
	Timeout      time.Duration
}

type Result struct {
	EvidenceRoot string   `json:"evidence_root"`
	RunDirs      []string `json:"run_directories"`
}

type liveEnvironment struct {
	client       client.Client
	taskQueue    string
	evidenceRoot string
	sourceSHA    string
}

type actionResult struct {
	snapshot *compute.ProviderSnapshot
	err      error
}

func Run(parent context.Context, options Options) (result Result, runErr error) {
	if err := validateOptions(options); err != nil {
		return Result{}, err
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()
	if err := createEvidenceRoot(options.EvidenceRoot); err != nil {
		return Result{}, err
	}
	result.EvidenceRoot = options.EvidenceRoot
	defer func() {
		if runErr == nil {
			return
		}
		encoded, marshalErr := json.MarshalIndent(map[string]any{
			"classification": "suite_failure", "preserved": true,
			"error": runErr.Error(), "recorded_at": time.Now().UTC(),
		}, "", "  ")
		if marshalErr == nil {
			_ = writeExclusive(filepath.Join(options.EvidenceRoot, "failure.json"), encoded)
		}
	}()

	serverLog, err := os.OpenFile(filepath.Join(options.EvidenceRoot, "temporal-server.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return result, fmt.Errorf("create Temporal server log: %w", err)
	}
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath, LogLevel: "warn", LogFormat: "pretty",
		Stdout: serverLog, Stderr: serverLog,
	})
	if err != nil {
		_ = serverLog.Close()
		return result, fmt.Errorf("start Temporal dev server: %w", err)
	}
	defer func() {
		server.Client().Close()
		runErr = errors.Join(runErr, server.Stop(), serverLog.Close())
	}()

	taskQueue := "sandbox-harness-live"
	temporalWorker := worker.New(server.Client(), taskQueue, worker.Options{Identity: "sandbox-harness-worker"})
	registerWorker(temporalWorker)
	if err := temporalWorker.Start(); err != nil {
		return result, fmt.Errorf("start sandbox-harness Worker: %w", err)
	}
	defer temporalWorker.Stop()
	sourceSHA, err := executableSHA256()
	if err != nil {
		return result, err
	}
	environment := liveEnvironment{
		client: server.Client(), taskQueue: taskQueue, evidenceRoot: options.EvidenceRoot, sourceSHA: sourceSHA,
	}

	for _, operation := range ambiguousOperations {
		for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
			for trial := 1; trial <= options.Trials; trial++ {
				runDir, err := environment.runAmbiguousEffect(ctx, operation, probe, trial)
				if err != nil {
					return result, fmt.Errorf("run %s/%s trial %d: %w", operation, probe, trial, err)
				}
				result.RunDirs = append(result.RunDirs, runDir)
			}
		}
	}
	for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
		for trial := 1; trial <= options.Trials; trial++ {
			runDir, err := environment.runAttachedWriter(ctx, probe, trial)
			if err != nil {
				return result, fmt.Errorf("run attached-writer/%s trial %d: %w", probe, trial, err)
			}
			result.RunDirs = append(result.RunDirs, runDir)
		}
	}
	for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
		for trial := 1; trial <= options.Trials; trial++ {
			runDir, err := environment.runParentCloseDuringInit(ctx, probe, trial)
			if err != nil {
				return result, fmt.Errorf("run parent-close/%s trial %d: %w", probe, trial, err)
			}
			result.RunDirs = append(result.RunDirs, runDir)
		}
	}
	return result, nil
}

func (e liveEnvironment) runAttachedWriter(ctx context.Context, probe protocol.Probe, trial int) (string, error) {
	startedAt := time.Now().UTC()
	workDirectory, err := os.MkdirTemp("", "sandbox-attached-writer-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(workDirectory) }()
	mode := provider.ModeUnsafe
	if probe == protocol.ProbeProtected {
		mode = provider.ModeFenced
	}
	store, err := provider.Create(filepath.Join(workDirectory, "provider.db"), mode)
	if err != nil {
		return "", err
	}
	coordinator := failureinject.NewCoordinator()
	barrierServer := httptest.NewServer(coordinator.Handler())
	defer barrierServer.Close()
	sessionID := fmt.Sprintf("sandbox-attached-writer-%s-trial-%d", probe, trial)
	config := provider.Config{
		DatabasePath: filepath.Join(workDirectory, "provider.db"), Mode: mode,
		BarrierURL: barrierServer.URL, FaultOperation: provider.OperationCommand,
		SessionID: sessionID, WorkerIdentity: "sandbox-harness-worker",
		Generation: 2, Capability: "current-owner-capability",
	}
	run, err := e.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: sessionID, TaskQueue: e.taskQueue},
		sandboxworkflow.SandboxWorkflow, sandboxworkflow.SandboxLocalState{})
	if err != nil {
		return "", err
	}
	if err := e.initialize(ctx, run, config, nil); err != nil {
		return "", err
	}
	authorityState, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	replacementAt := time.Now().UTC()
	if mode == provider.ModeFenced && authorityState.Authority.Generation != 2 {
		return "", fmt.Errorf("authority generation = %d, want 2 before attached writes", authorityState.Authority.Generation)
	}
	boundaryAt := time.Now().UTC()

	staleResult := make(chan error, 1)
	currentResult := make(chan error, 1)
	go func() {
		staleResult <- e.command(ctx, run, "stale-attached", "stale", 1, "old-owner-capability")
	}()
	go func() {
		currentResult <- e.command(ctx, run, "current-attached", "current", 2, "current-owner-capability")
	}()
	wantArrivals := 2
	if probe == protocol.ProbeProtected {
		wantArrivals = 1
	}
	point := "provider-command-effect-committed"
	if _, err := coordinator.WaitForArrivals(ctx, point, wantArrivals); err != nil {
		return "", err
	}
	defer func() { _ = coordinator.Release(point) }()
	stalePoint := "provider-command-stale-rejected"
	if probe == protocol.ProbeProtected {
		if _, err := coordinator.WaitForArrivals(ctx, stalePoint, 1); err != nil {
			return "", err
		}
		defer func() { _ = coordinator.Release(stalePoint) }()
	}
	atBarrier, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	staleAtBarrier := attemptsForEffect(atBarrier.Attempts, "stale-attached")
	if len(staleAtBarrier) != 1 || staleAtBarrier[0].Applied != (probe == protocol.ProbeUnsafe) {
		return "", fmt.Errorf("stale attempt at barrier = %+v", staleAtBarrier)
	}
	if err := coordinator.Release(point); err != nil {
		return "", err
	}
	if probe == protocol.ProbeProtected {
		if err := coordinator.Release(stalePoint); err != nil {
			return "", err
		}
	}
	staleErr, currentErr := <-staleResult, <-currentResult
	if currentErr != nil {
		return "", fmt.Errorf("current attached writer: %w", currentErr)
	}
	if probe == protocol.ProbeUnsafe && staleErr != nil {
		return "", fmt.Errorf("unsafe stale attached writer unexpectedly failed: %w", staleErr)
	}
	if probe == protocol.ProbeProtected && staleErr == nil {
		return "", errors.New("fenced stale attached writer unexpectedly succeeded")
	}
	if err := e.stop(ctx, run); err != nil {
		return "", err
	}
	if err := run.Get(ctx, nil); err != nil {
		return "", err
	}
	completedAt := time.Now().UTC()
	state, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	staleAttempts := attemptsForEffect(state.Attempts, "stale-attached")
	if len(staleAttempts) == 0 {
		return "", errors.New("provider journal lacks stale attached attempt")
	}
	historyJSON, err := readHistoryJSON(ctx, e.client, run.GetID(), run.GetRunID())
	if err != nil {
		return "", err
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	capture := evidenceadapter.AttachedWriterCapture{
		AdapterVersion: e.sourceSHA, AgentSourceSHA256: e.sourceSHA, Trial: trial, Probe: probe,
		SessionID: sessionID, DestinationID: "hermetic-provider:" + sessionID,
		LogicalEffectID: "stale-attached", Runtime: runtime.Version(), StartedAt: startedAt,
		OldOwner:      evidenceadapter.ProcessCapture{ActorID: "attached-reference-old", Identity: "opaque-reference/generation-1"},
		CurrentOwner:  evidenceadapter.ProcessCapture{ActorID: "attached-reference-current", Identity: "opaque-reference/generation-2"},
		ReplacementAt: replacementAt, BoundaryAt: boundaryAt,
		StaleAttempt: evidenceadapter.AttemptCapture{
			PhysicalAttemptID: staleAttempts[0].PhysicalAttemptID,
			Applied:           staleAttempts[0].Applied, ObservedAt: staleAttempts[0].ObservedAt,
		},
		CompletedAt: completedAt,
		Native: []evidenceadapter.NativeCapture{
			{Kind: "provider_journal", Detail: string(stateJSON)},
			{Kind: "temporal_history", Detail: string(historyJSON)},
		},
		Settings: map[string]string{
			"provider_mode": string(mode), "upstream_commit": upstreamCommit,
			"upstream_version": upstreamVersion, "concurrency_barrier": point,
		},
	}
	bundle, err := evidenceadapter.BuildAttachedWriterBundle(capture)
	if err != nil {
		return "", err
	}
	runDir, err := evidence.WriteRun(ctx, e.evidenceRoot, bundle)
	if err != nil {
		return "", err
	}
	verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
	if err != nil {
		return runDir, err
	}
	want := protocol.VerdictValidFail
	if probe == protocol.ProbeProtected {
		want = protocol.VerdictValidPass
	}
	if verdict.Class != want {
		return runDir, fmt.Errorf("attached-writer oracle verdict = %s (%v), want %s", verdict.Class, verdict.ReasonCodes, want)
	}
	if err := writeExclusive(filepath.Join(runDir, "provider-state.json"), stateJSON); err != nil {
		return runDir, err
	}
	if err := writeExclusive(filepath.Join(runDir, "temporal-history.json"), historyJSON); err != nil {
		return runDir, err
	}
	return runDir, nil
}

func (e liveEnvironment) runAmbiguousEffect(
	ctx context.Context,
	operation provider.Operation,
	probe protocol.Probe,
	trial int,
) (string, error) {
	startedAt := time.Now().UTC()
	workDirectory, err := os.MkdirTemp("", "sandbox-harness-trial-")
	if err != nil {
		return "", fmt.Errorf("create trial work directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(workDirectory) }()
	mode := provider.ModeUnsafe
	if probe == protocol.ProbeProtected {
		mode = provider.ModeIdempotent
	}
	store, err := provider.Create(filepath.Join(workDirectory, "provider.db"), mode)
	if err != nil {
		return "", err
	}
	coordinator := failureinject.NewCoordinator()
	barrierServer := httptest.NewServer(coordinator.Handler())
	defer barrierServer.Close()
	sessionID := fmt.Sprintf("sandbox-%s-%s-trial-%d", operation, probe, trial)
	config := provider.Config{
		DatabasePath: filepath.Join(workDirectory, "provider.db"), Mode: mode,
		BarrierURL: barrierServer.URL, FaultOperation: operation,
		SessionID: sessionID, WorkerIdentity: "sandbox-harness-worker", Generation: 1,
	}
	workflowRun, err := e.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: sessionID, TaskQueue: e.taskQueue,
	}, sandboxworkflow.SandboxWorkflow, sandboxworkflow.SandboxLocalState{})
	if err != nil {
		return "", fmt.Errorf("start SandboxWorkflow: %w", err)
	}

	action := make(chan actionResult, 1)
	if operation == provider.OperationStart {
		go func() { action <- actionResult{err: e.initialize(ctx, workflowRun, config, nil)} }()
	} else {
		if err := e.initialize(ctx, workflowRun, config, nil); err != nil {
			return "", err
		}
		if operation == provider.OperationSnapshot {
			if err := e.command(ctx, workflowRun, "snapshot-prefix", "prefix", 0, ""); err != nil {
				return "", err
			}
		}
		e.startOperation(ctx, workflowRun, operation, action)
	}

	point := "provider-" + string(operation) + "-effect-committed"
	arrivals, err := coordinator.WaitForArrivals(ctx, point, 1)
	if err != nil {
		return "", fmt.Errorf("wait for exact provider barrier: %w", err)
	}
	defer func() { _ = coordinator.Release(point) }()
	atBarrier, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	if got := operationAttempts(atBarrier.Attempts, operation); len(got) != 1 || !got[0].Applied {
		return "", fmt.Errorf("provider state did not prove first %s effect before release: %+v", operation, got)
	}
	if err := coordinator.Release(point); err != nil {
		return "", err
	}
	completed := <-action
	if completed.err != nil {
		return "", fmt.Errorf("complete %s operation: %w", operation, completed.err)
	}

	if operation == provider.OperationSnapshot {
		if err := e.verifyFork(ctx, workflowRun, config, completed.snapshot); err != nil {
			return "", err
		}
	}
	if operation != provider.OperationStop {
		if err := e.stop(ctx, workflowRun); err != nil {
			return "", err
		}
	}
	if err := workflowRun.Get(ctx, nil); err != nil {
		return "", fmt.Errorf("wait for SandboxWorkflow completion: %w", err)
	}
	completedAt := time.Now().UTC()
	state, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	attempts := operationAttempts(state.Attempts, operation)
	if err := verifyAmbiguousAttempts(probe, attempts); err != nil {
		return "", err
	}
	historyJSON, err := readHistoryJSON(ctx, e.client, workflowRun.GetID(), workflowRun.GetRunID())
	if err != nil {
		return "", err
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("encode provider state: %w", err)
	}
	capture := evidenceadapter.AmbiguousEffectCapture{
		AdapterVersion: e.sourceSHA, AgentSourceSHA256: e.sourceSHA,
		Operation: string(operation), Trial: trial, Probe: probe, SessionID: sessionID,
		DestinationID:   "hermetic-provider:" + sessionID,
		LogicalEffectID: string(operation) + ":" + attempts[0].OperationID,
		Generation:      1, Runtime: runtime.Version(), StartedAt: startedAt,
		FirstWorker:    evidenceadapter.ProcessCapture{ActorID: "provider-attempt-1", Identity: attempts[0].WorkerIdentity},
		RecoveryWorker: evidenceadapter.ProcessCapture{ActorID: "provider-attempt-2", Identity: attempts[1].WorkerIdentity},
		Attempts: []evidenceadapter.AttemptCapture{
			{PhysicalAttemptID: attempts[0].PhysicalAttemptID, Applied: attempts[0].Applied, ObservedAt: attempts[0].ObservedAt},
			{PhysicalAttemptID: attempts[1].PhysicalAttemptID, Applied: attempts[1].Applied, ObservedAt: attempts[1].ObservedAt},
		},
		Fault:       evidenceadapter.FaultCapture{Point: "provider-effect-committed", TriggeredAt: arrivals[0].Time},
		CompletedAt: completedAt,
		Native: []evidenceadapter.NativeCapture{
			{Kind: "provider_journal", Detail: string(stateJSON)},
			{Kind: "temporal_history", Detail: string(historyJSON)},
		},
		Settings: map[string]string{
			"provider_mode": string(mode), "upstream_commit": upstreamCommit,
			"upstream_version": upstreamVersion, "fault_boundary": point,
			"fault_mechanism": "retryable-error-after-committed-effect",
		},
	}
	bundle, err := evidenceadapter.BuildAmbiguousEffectBundle(capture)
	if err != nil {
		return "", err
	}
	runDir, err := evidence.WriteRun(ctx, e.evidenceRoot, bundle)
	if err != nil {
		return "", err
	}
	verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
	if err != nil {
		return runDir, err
	}
	want := protocol.VerdictValidFail
	if probe == protocol.ProbeProtected {
		want = protocol.VerdictValidPass
	}
	if verdict.Class != want {
		return runDir, fmt.Errorf("oracle verdict = %s (%v), want %s", verdict.Class, verdict.ReasonCodes, want)
	}
	if err := writeExclusive(filepath.Join(runDir, "provider-state.json"), stateJSON); err != nil {
		return runDir, err
	}
	if err := writeExclusive(filepath.Join(runDir, "temporal-history.json"), historyJSON); err != nil {
		return runDir, err
	}
	return runDir, nil
}

func (e liveEnvironment) startOperation(
	ctx context.Context,
	run client.WorkflowRun,
	operation provider.Operation,
	result chan<- actionResult,
) {
	go func() {
		switch operation {
		case provider.OperationCommand:
			result <- actionResult{err: e.command(ctx, run, "command-effect", "fixture", 0, "")}
		case provider.OperationSnapshot:
			var snapshot compute.ProviderSnapshot
			err := e.update(ctx, run, sandboxworkflow.SandboxSnapshotUpdate, "snapshot-update", nil, &snapshot)
			result <- actionResult{snapshot: &snapshot, err: err}
		case provider.OperationStop:
			err := e.client.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), sandboxworkflow.SandboxStopSignal, nil)
			if err == nil {
				err = run.Get(ctx, nil)
			}
			result <- actionResult{err: err}
		default:
			result <- actionResult{err: fmt.Errorf("unsupported live operation %q", operation)}
		}
	}()
}

func (e liveEnvironment) initialize(
	ctx context.Context,
	run client.WorkflowRun,
	config provider.Config,
	snapshot *compute.ProviderSnapshot,
) error {
	input := sandboxworkflow.SandboxInitInput{
		ComputeProvider: config.ProviderDetails(), IdleTimeout: compute.NoIdleTimeout, Snapshot: snapshot,
	}
	return e.update(ctx, run, sandboxworkflow.SandboxInitUpdate, "init-update", input, nil)
}

func (e liveEnvironment) command(
	ctx context.Context,
	run client.WorkflowRun,
	effectID string,
	payload string,
	generation uint64,
	capability string,
) error {
	command, err := provider.EncodeCommand(provider.CommandEnvelope{
		LogicalEffectID: effectID, Payload: payload, Generation: generation, Capability: capability,
	})
	if err != nil {
		return err
	}
	var result compute.CommandResult
	return e.update(
		ctx, run, sandboxworkflow.SandboxExecuteCommandUpdate, "command-update-"+effectID,
		sandboxworkflow.SandboxExecuteCommandInput{Command: command}, &result,
	)
}

func (e liveEnvironment) update(
	ctx context.Context,
	run client.WorkflowRun,
	name string,
	id string,
	argument any,
	result any,
) error {
	arguments := []interface{}{}
	if argument != nil {
		arguments = append(arguments, argument)
	}
	handle, err := e.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		UpdateID: id, WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: name,
		Args: arguments, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return fmt.Errorf("send Update %s/%s: %w", name, id, err)
	}
	if err := handle.Get(ctx, result); err != nil {
		return fmt.Errorf("complete Update %s/%s: %w", name, id, err)
	}
	return nil
}

func (e liveEnvironment) stop(ctx context.Context, run client.WorkflowRun) error {
	if err := e.client.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), sandboxworkflow.SandboxStopSignal, nil); err != nil {
		return fmt.Errorf("signal SandboxWorkflow stop: %w", err)
	}
	return nil
}

func (e liveEnvironment) verifyFork(
	ctx context.Context,
	origin client.WorkflowRun,
	config provider.Config,
	snapshot *compute.ProviderSnapshot,
) error {
	if snapshot == nil || snapshot.SnapshotID == "" {
		return errors.New("snapshot Update returned no snapshot identity")
	}
	if err := e.command(ctx, origin, "post-snapshot", "post", 0, ""); err != nil {
		return err
	}
	forkID := origin.GetID() + "/fork"
	fork, err := e.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: forkID, TaskQueue: e.taskQueue},
		sandboxworkflow.SandboxWorkflow, sandboxworkflow.SandboxLocalState{})
	if err != nil {
		return fmt.Errorf("start snapshot fork: %w", err)
	}
	if err := e.initialize(ctx, fork, config, snapshot); err != nil {
		return fmt.Errorf("initialize snapshot fork: %w", err)
	}
	var state sandboxworkflow.SandboxState
	encoded, err := e.client.QueryWorkflow(ctx, fork.GetID(), fork.GetRunID(), sandboxworkflow.SandboxStateQuery)
	if err != nil {
		return fmt.Errorf("query snapshot fork: %w", err)
	}
	if err := encoded.Get(&state); err != nil {
		return fmt.Errorf("decode snapshot fork state: %w", err)
	}
	store, err := provider.Open(config.DatabasePath)
	if err != nil {
		return err
	}
	providerState, err := store.Snapshot(ctx)
	if err != nil {
		return err
	}
	forkState := providerState.Instance(state.Status.InstanceID)
	if len(forkState.Effects) != 1 || forkState.Effects[0] != "snapshot-prefix" || forkState.ParentSnapshotID != snapshot.SnapshotID {
		return fmt.Errorf("fork lineage = %+v, want exact snapshot prefix", forkState)
	}
	if err := e.stop(ctx, fork); err != nil {
		return err
	}
	if err := fork.Get(ctx, nil); err != nil {
		return fmt.Errorf("wait for snapshot fork: %w", err)
	}
	return nil
}

func registerWorker(temporalWorker worker.Worker) {
	temporalWorker.RegisterWorkflowWithOptions(
		sandboxworkflow.SandboxWorkflow,
		workflowSDK.RegisterOptions{Name: sandboxworkflow.SandboxWorkflowType},
	)
	temporalWorker.RegisterActivity(sandboxworkflow.StartSandbox)
	temporalWorker.RegisterActivity(sandboxworkflow.StopSandbox)
	temporalWorker.RegisterActivity(sandboxworkflow.SuspendSandbox)
	temporalWorker.RegisterActivity(sandboxworkflow.ResumeSandbox)
	temporalWorker.RegisterActivity(sandboxworkflow.SnapshotSandbox)
	temporalWorker.RegisterActivity(sandboxworkflow.StartSandboxFromSnapshot)
	temporalWorker.RegisterActivity(sandboxworkflow.DeleteSnapshot)
	temporalWorker.RegisterActivity(sandboxworkflow.ExecuteCommand)
	temporalWorker.RegisterWorkflowWithOptions(parentSandboxWorkflow, workflowSDK.RegisterOptions{Name: parentWorkflow})
	temporalWorker.RegisterActivity(notifyChildStarted)
}

func validateOptions(options Options) error {
	if options.EvidenceRoot == "" || options.TemporalPath == "" || options.Trials < 3 || options.Timeout <= 0 {
		return errors.New("evidence root, Temporal path, at least three trials, and positive timeout are required")
	}
	return nil
}

func createEvidenceRoot(path string) error {
	resolved, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve evidence root: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0o750); err != nil {
		return fmt.Errorf("create evidence parent: %w", err)
	}
	if err := os.Mkdir(resolved, 0o750); err != nil {
		return fmt.Errorf("create append-only evidence root: %w", err)
	}
	return nil
}

func operationAttempts(attempts []provider.Attempt, operation provider.Operation) []provider.Attempt {
	selected := make([]provider.Attempt, 0, len(attempts))
	for _, attempt := range attempts {
		if attempt.Kind == operation {
			selected = append(selected, attempt)
		}
	}
	return selected
}

func attemptsForEffect(attempts []provider.Attempt, effectID string) []provider.Attempt {
	selected := make([]provider.Attempt, 0, len(attempts))
	for _, attempt := range attempts {
		if attempt.LogicalEffectID == effectID {
			selected = append(selected, attempt)
		}
	}
	return selected
}

func verifyAmbiguousAttempts(probe protocol.Probe, attempts []provider.Attempt) error {
	if len(attempts) != 2 {
		return fmt.Errorf("provider attempts = %d, want 2", len(attempts))
	}
	if attempts[0].OperationID != attempts[1].OperationID ||
		attempts[0].PhysicalAttemptID == attempts[1].PhysicalAttemptID || !attempts[0].Applied {
		return fmt.Errorf("provider attempt identities are invalid: %+v", attempts)
	}
	wantSecondApplied := probe == protocol.ProbeUnsafe
	if attempts[1].Applied != wantSecondApplied {
		return fmt.Errorf("second provider attempt applied = %v, want %v", attempts[1].Applied, wantSecondApplied)
	}
	return nil
}

func readHistoryJSON(ctx context.Context, temporalClient client.Client, workflowID, runID string) ([]byte, error) {
	iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	historyValue := &history.History{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, fmt.Errorf("read Temporal history: %w", err)
		}
		historyValue.Events = append(historyValue.Events, event)
	}
	encoded, err := protojson.MarshalOptions{Indent: "  "}.Marshal(historyValue)
	if err != nil {
		return nil, fmt.Errorf("encode Temporal history: %w", err)
	}
	return encoded, nil
}

func executableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve experiment executable: %w", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read experiment executable: %w", err)
	}
	digest := sha256.Sum256(contents)
	return hex.EncodeToString(digest[:]), nil
}

func writeExclusive(path string, contents []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create evidence file %s: %w", path, err)
	}
	if _, err := file.Write(contents); err != nil {
		_ = file.Close()
		return fmt.Errorf("write evidence file %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close evidence file %s: %w", path, err)
	}
	return nil
}
