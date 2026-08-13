package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

const defaultRunTimeout = 60 * time.Second

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
	if err := os.Mkdir(runDirectory, 0o750); err != nil {
		return Result{}, fmt.Errorf("create append-only run directory: %w", err)
	}
	result.RunDirectory = runDirectory
	defer func() {
		if runErr != nil {
			_ = writeJSONExclusive(filepath.Join(runDirectory, "failure.json"), map[string]any{
				"time": time.Now().UTC(), "error": runErr.Error(),
			})
		}
	}()

	runtimeDirectory, err := os.MkdirTemp("", "large-artifact-runtime-*")
	if err != nil {
		return result, fmt.Errorf("create runtime directory: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, os.RemoveAll(runtimeDirectory)) }()
	sourcePath := filepath.Join(runDirectory, "source-artifact.bin")
	content := largeArtifactContent()
	if err := writeFileAtomically(sourcePath, content); err != nil {
		return result, err
	}
	storeRoot := filepath.Join(runDirectory, "artifact-store")
	externalRoot := filepath.Join(runDirectory, "external-store")
	artifactID := "artifact-" + options.RunID
	credential, err := failureinject.NewCredential()
	if err != nil {
		return result, err
	}
	barriers, err := startBarrierService(credential, failureinject.Expectation{
		Point: string(options.Boundary), SessionID: artifactID, Generation: 1, ActorID: "worker-1",
	})
	if err != nil {
		return result, err
	}
	barrierStopped := false
	defer func() {
		if !barrierStopped {
			runErr = errors.Join(runErr, barriers.stop(context.Background()))
		}
	}()

	serverLog, err := os.OpenFile(filepath.Join(runDirectory, "temporal-server.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return result, err
	}
	temporalServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath,
		DBFilename:   filepath.Join(runtimeDirectory, "temporal.db"),
		LogLevel:     "warn", LogFormat: "pretty", Stdout: serverLog, Stderr: serverLog,
	})
	if err != nil {
		_ = serverLog.Close()
		return result, fmt.Errorf("start Temporal dev server: %w", err)
	}
	serverStopped := false
	defer func() {
		if !serverStopped {
			temporalServer.Client().Close()
			runErr = errors.Join(runErr, temporalServer.Stop(), serverLog.Close())
		}
	}()

	clientDriver, err := NewFileStorageDriver(externalRoot, options.Mode, nil)
	if err != nil {
		return result, err
	}
	temporalClient, err := client.Dial(client.Options{
		HostPort: temporalServer.FrontendHostPort(), Namespace: "default",
		ExternalStorage: converter.ExternalStorage{
			Drivers: []converter.StorageDriver{clientDriver}, PayloadSizeThreshold: ExternalStorageThreshold,
		},
	})
	if err != nil {
		return result, fmt.Errorf("connect experiment client: %w", err)
	}
	clientClosed := false
	defer func() {
		if !clientClosed {
			temporalClient.Close()
		}
	}()

	taskQueue := "large-artifact-" + options.RunID
	processConfig := workerProcessConfig{
		Binary: options.WorkerBinary, RunDirectory: runDirectory,
		Address: temporalServer.FrontendHostPort(), TaskQueue: taskQueue,
		BarrierURL: barriers.URL(), SessionID: artifactID,
		StoreRoot: storeRoot, ExternalRoot: externalRoot,
		CoverageRoot: options.CoverageRoot,
		Mode:         options.Mode, Boundary: options.Boundary, Credential: credential,
	}
	processConfig.WorkerID = "worker-1"
	worker1, err := startWorkerProcess(processConfig)
	if err != nil {
		return result, err
	}
	worker1Stopped := false
	defer func() {
		if !worker1Stopped {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			runErr = errors.Join(runErr, worker1.stop(shutdownCtx))
		}
	}()

	workflowID := "large-artifact/" + options.RunID
	var workflowRun client.WorkflowRun
	if options.Boundary == BoundaryExternalStorageStored {
		workflowRun, err = temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
		}, externalWorkflowName, ExternalWorkflowInput{SourcePath: sourcePath})
	} else {
		workflowRun, err = temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
		}, workflowName, WorkflowInput{
			StoreRoot: storeRoot, SourcePath: sourcePath, LogicalID: artifactID,
			ConsumerID: "consumer-1", Mode: options.Mode, FailureBoundary: options.Boundary,
		})
	}
	if err != nil {
		return result, fmt.Errorf("start large-artifact Workflow: %w", err)
	}
	result.WorkflowID = workflowID
	result.WorkflowRunID = workflowRun.GetRunID()

	arrivals, err := barriers.coordinator.WaitForArrivals(ctx, string(options.Boundary), 1)
	if err != nil {
		return result, fmt.Errorf("wait for exact artifact boundary: %w", err)
	}
	arrival := arrivals[0]
	evidence := Evidence{Boundary: options.Boundary, Mode: options.Mode, Barrier: arrival}
	if options.Boundary == BoundaryExternalStorageStored {
		evidence.PreFaultExternal, err = clientDriver.Snapshot(ctx)
	} else {
		store, storeErr := NewArtifactStore(storeRoot)
		if storeErr != nil {
			return result, storeErr
		}
		evidence.PreFaultStore, err = store.Snapshot(ctx)
	}
	if err != nil {
		return result, err
	}
	exitStatus, err := worker1.killAndWait()
	worker1Stopped = true
	if err != nil {
		return result, err
	}
	evidence.Kill = KillObservation{
		WorkerID: "worker-1", PID: worker1.PID(), Signal: "SIGKILL",
		ExitStatus: exitStatus, KilledAt: time.Now().UTC(),
	}
	processConfig.WorkerID = "worker-2"
	worker2, err := startWorkerProcess(processConfig)
	if err != nil {
		return result, err
	}
	worker2Stopped := false
	defer func() {
		if !worker2Stopped {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			runErr = errors.Join(runErr, worker2.stop(shutdownCtx))
		}
	}()

	if options.Boundary == BoundaryExternalStorageStored {
		if err := workflowRun.Get(ctx, &evidence.ExternalResult); err != nil {
			return result, fmt.Errorf("wait for external-storage Workflow: %w", err)
		}
		evidence.FinalExternalStore, err = clientDriver.Snapshot(ctx)
	} else {
		if err := workflowRun.Get(ctx, &evidence.WorkflowResult); err != nil {
			return result, fmt.Errorf("wait for artifact Workflow: %w", err)
		}
		store, storeErr := NewArtifactStore(storeRoot)
		if storeErr != nil {
			return result, storeErr
		}
		evidence.BeforeReconcile, err = store.Snapshot(ctx)
		if err == nil {
			evidence.Reconciliation, err = store.Reconcile(ctx)
		}
		if err == nil {
			evidence.FinalStore, err = store.Snapshot(ctx)
		}
	}
	if err != nil {
		return result, err
	}
	history, err := readHistory(ctx, temporalClient, workflowID, workflowRun.GetRunID())
	if err != nil {
		return result, err
	}
	evidence.History = summarizeHistory(history)
	if err := replayHistory(history, externalReplayRoot(options, externalRoot), options.Mode); err != nil {
		return result, err
	}
	result.Verdict = Verify(evidence)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := worker2.stop(shutdownCtx); err != nil {
		shutdownCancel()
		return result, err
	}
	shutdownCancel()
	worker2Stopped = true
	temporalClient.Close()
	clientClosed = true
	temporalServer.Client().Close()
	if err := temporalServer.Stop(); err != nil {
		return result, err
	}
	if err := serverLog.Close(); err != nil {
		return result, err
	}
	serverStopped = true
	if err := barriers.stop(context.Background()); err != nil {
		return result, err
	}
	barrierStopped = true
	if err := writeHistory(filepath.Join(runDirectory, "temporal-history.json"), history); err != nil {
		return result, err
	}
	manifest := Manifest{
		SchemaVersion: 1, Experiment: "large-artifact-durability", RunID: options.RunID,
		Boundary: options.Boundary, Mode: options.Mode,
		WorkflowID: workflowID, WorkflowRunID: workflowRun.GetRunID(), ArtifactID: artifactID,
		ArtifactSHA256: digestBytes(content), ArtifactSize: int64(len(content)),
		StartedAt: startedAt, CompletedAt: time.Now().UTC(),
		FailureBoundary: failureBoundaryDescription(options.Boundary),
		Invariant:       "protected redelivery converges to one verified blob, durable reference, compact history receipt, and consumer acknowledgement",
		Falsifier:       "loss, conflicting content, duplicate durable reference/acknowledgement, reachable-blob cleanup, inline artifact bytes, or replay failure",
		Runtime:         options.Provenance,
		SourcePins:      options.SourcePins,
	}
	if err := preserveEvidence(runDirectory, evidence, result.Verdict, manifest); err != nil {
		return result, err
	}
	if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
		return result, fmt.Errorf("experiment oracle failed: %s", strings.Join(result.Verdict.Failures, "; "))
	}
	return result, nil
}

func validateOptions(options Options) error {
	if !options.Boundary.Valid() || !options.Mode.valid() || options.TemporalPath == "" ||
		options.WorkerBinary == "" || !filepath.IsAbs(options.OutputRoot) || !safeComponent(options.RunID) {
		return errors.New("run requires boundary, mode, Temporal/Worker binaries, output root, and safe run ID")
	}
	if err := ValidateCurrentRuntimeProvenance(options.Provenance); err != nil {
		return fmt.Errorf("validate runtime provenance: %w", err)
	}
	if err := ValidateCurrentSourcePins(options.SourcePins); err != nil {
		return fmt.Errorf("validate source pins: %w", err)
	}
	if options.CoverageRoot != "" {
		coverageInfo, err := os.Lstat(options.CoverageRoot)
		if err != nil || !filepath.IsAbs(options.CoverageRoot) || !coverageInfo.IsDir() || coverageInfo.Mode()&os.ModeSymlink != 0 {
			return errors.New("coverage root must be an existing absolute real directory")
		}
	}
	outputInfo, err := os.Lstat(options.OutputRoot)
	if err != nil || !outputInfo.IsDir() || outputInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("output root must be an existing real directory")
	}
	for _, binary := range []string{options.TemporalPath, options.WorkerBinary} {
		info, err := os.Stat(binary)
		if err != nil {
			return fmt.Errorf("inspect binary %q: %w", binary, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			return fmt.Errorf("binary %q is not executable", binary)
		}
	}
	return nil
}

func largeArtifactContent() []byte {
	return []byte(strings.Repeat("large-agent-artifact-v1\n", 16*1024))
}

func externalReplayRoot(options Options, root string) string {
	if options.Boundary == BoundaryExternalStorageStored {
		return root
	}
	return ""
}

func failureBoundaryDescription(boundary Boundary) string {
	if boundary == BoundaryExternalStorageStored {
		return "Worker 1 SIGKILL after SDK StorageDriver bytes are durable but before its claim is returned"
	}
	return "Worker 1 SIGKILL at exact authenticated " + string(boundary) + " application-artifact boundary"
}
