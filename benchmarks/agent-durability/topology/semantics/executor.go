package semantics

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

type ExecutorConfig struct {
	TemporalPath string
	WorkRoot     string
	AgentBinary  string
}

type TemporalExecutor struct {
	config ExecutorConfig
	server *testsuite.DevServer
	logger temporallog.Logger
	mu     sync.Mutex
	closed bool
}

const (
	workflowTaskConcurrency          = 128
	topologyWorkflowExecutionTimeout = 5 * time.Minute
)

type RunRequest struct {
	PairID          string
	ScheduleBlockID string
	TrackerBeadID   string
	Topology        protocol.Topology
	Case            protocol.CaseID
	Boundary        string
	Probe           protocol.Probe
	Fanout          int
}

type EpisodeResult struct {
	Output        ParentOutput
	WorkflowError string
	NativeHistory protocol.NativeHistory
	Bundle        protocol.EvidenceBundle
}

func OpenTemporalExecutor(ctx context.Context, config ExecutorConfig) (*TemporalExecutor, error) {
	if config.TemporalPath == "" || config.WorkRoot == "" || config.AgentBinary == "" {
		return nil, fmt.Errorf("%w: Temporal semantics executor configuration", protocol.ErrInvalidEvidence)
	}
	for _, path := range []string{config.TemporalPath, config.AgentBinary} {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: executable path %q", protocol.ErrInvalidEvidence, path)
		}
	}
	if err := os.MkdirAll(config.WorkRoot, 0o750); err != nil {
		return nil, err
	}
	logger := temporallog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: config.TemporalPath, DBFilename: filepath.Join(config.WorkRoot, "temporal.db"),
		LogLevel: "error", LogFormat: "pretty", Stdout: io.Discard, Stderr: io.Discard,
		ClientOptions: &client.Options{Logger: logger},
	})
	if err != nil {
		return nil, fmt.Errorf("start Temporal semantics server: %w", err)
	}
	return &TemporalExecutor{config: config, server: server, logger: logger}, nil
}

func (e *TemporalExecutor) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	e.server.Client().Close()
	return e.server.Stop()
}

func (e *TemporalExecutor) Ready(ctx context.Context) error {
	if e == nil || e.server == nil || ctx == nil {
		return fmt.Errorf("%w: closed Temporal semantics executor", protocol.ErrInvalidEvidence)
	}
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return fmt.Errorf("%w: closed Temporal semantics executor", protocol.ErrInvalidEvidence)
	}
	_, err := e.server.Client().WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	return err
}

func (e *TemporalExecutor) ServerVersion(ctx context.Context) (string, error) {
	if err := e.Ready(ctx); err != nil {
		return "", err
	}
	info, err := e.server.Client().WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err != nil {
		return "", err
	}
	if info.ServerVersion == "" {
		return "", fmt.Errorf("%w: empty Temporal server version", protocol.ErrInvalidEvidence)
	}
	return info.ServerVersion, nil
}

func (e *TemporalExecutor) Run(ctx context.Context, request RunRequest) (_ EpisodeResult, returnErr error) {
	if e == nil || e.server == nil {
		return EpisodeResult{}, fmt.Errorf("%w: closed Temporal semantics executor", protocol.ErrInvalidEvidence)
	}
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return EpisodeResult{}, fmt.Errorf("%w: closed Temporal semantics executor", protocol.ErrInvalidEvidence)
	}
	if request.PairID == "" || request.ScheduleBlockID == "" {
		return EpisodeResult{}, fmt.Errorf("%w: paired run identity", protocol.ErrInvalidEvidence)
	}
	runID := request.PairID + "/" + string(request.Topology)
	trackerBeadID := request.TrackerBeadID
	if trackerBeadID == "" {
		trackerBeadID = "temporal_projects-4ic.2"
		if request.Case.Suite() == protocol.SuiteRecoveryDynamics {
			trackerBeadID = "temporal_projects-4ic.3"
		}
	}
	prefix := "topology-" + shortDigest(runID)
	spec := EpisodeSpec{
		RunID: runID, PairID: request.PairID, ScheduleBlockID: request.ScheduleBlockID,
		TrackerBeadID: trackerBeadID,
		Topology:      request.Topology, Case: request.Case, Boundary: request.Boundary, Probe: request.Probe,
		Fanout: request.Fanout, ParentTaskQueue: prefix + "-parent", WorkTaskQueue: prefix + "-work",
		EffectTaskQueue: prefix + "-effect", LogicalOperationID: request.PairID + "/operation",
	}
	runtime, err := NewEpisodeRuntime(spec, e.config.WorkRoot, e.config.AgentBinary)
	if err != nil {
		return EpisodeResult{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, runtime.Close()) }()

	parentWorker := worker.New(e.server.Client(), spec.ParentTaskQueue, worker.Options{
		Identity: prefix + "-parent-worker", MaxConcurrentWorkflowTaskExecutionSize: workflowTaskConcurrency,
	})
	parentWorker.RegisterWorkflowWithOptions(ParentWorkflow, workflow.RegisterOptions{Name: ParentWorkflowName})
	parentWorker.RegisterWorkflowWithOptions(ItemWorkflow, workflow.RegisterOptions{Name: ItemWorkflowName})
	parentWorker.RegisterWorkflowWithOptions(RecoveryItemWorkflow, workflow.RegisterOptions{Name: RecoveryItemWorkflowName})
	if err := parentWorker.Start(); err != nil {
		return EpisodeResult{}, err
	}
	defer parentWorker.Stop()

	effectWorkers := newRestartableActivityWorker(e.server.Client(), spec.EffectTaskQueue, prefix+"-effect", func(handler *ActivityHandler, value worker.Worker) {
		value.RegisterActivityWithOptions(handler.Work, activity.RegisterOptions{Name: WorkActivityName})
		value.RegisterActivityWithOptions(handler.RecoveryAdmission, activity.RegisterOptions{Name: RecoveryAdmissionActivityName})
		value.RegisterActivityWithOptions(handler.RecoveryCohort, activity.RegisterOptions{Name: RecoveryCohortActivityName})
		value.RegisterActivityWithOptions(handler.RecoveryWork, activity.RegisterOptions{Name: RecoveryWorkActivityName})
		value.RegisterActivityWithOptions(handler.Checkpoint, activity.RegisterOptions{Name: CheckpointActivityName})
		value.RegisterActivityWithOptions(handler.Continue, activity.RegisterOptions{Name: ContinuationActivityName})
		value.RegisterActivityWithOptions(handler.Supersede, activity.RegisterOptions{Name: SupersedeActivityName})
		value.RegisterActivityWithOptions(handler.Cancellation, activity.RegisterOptions{Name: CancellationActivityName})
		value.RegisterActivityWithOptions(handler.Destructive, activity.RegisterOptions{Name: DestructiveActivityName})
	}, runtime)
	if err := effectWorkers.start(); err != nil {
		return EpisodeResult{}, err
	}
	defer effectWorkers.stop()

	workWorkers := newRestartableActivityWorker(e.server.Client(), spec.WorkTaskQueue, prefix+"-work", func(handler *ActivityHandler, value worker.Worker) {
		value.RegisterActivityWithOptions(handler.Work, activity.RegisterOptions{Name: WorkActivityName})
		value.RegisterActivityWithOptions(handler.RecoveryWork, activity.RegisterOptions{Name: RecoveryWorkActivityName})
	}, runtime)
	queuedBoundary := request.Case == protocol.CaseQueuedExecutingSupersession && request.Boundary == "queued-before-activity-start"
	if !queuedBoundary {
		if err := workWorkers.start(); err != nil {
			return EpisodeResult{}, err
		}
	}
	defer workWorkers.stop()

	workflowID := prefix + "-parent"
	run, err := e.server.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: spec.ParentTaskQueue, WorkflowExecutionTimeout: topologyWorkflowExecutionTimeout,
	}, ParentWorkflowName, runtime.Input())
	if err != nil {
		return EpisodeResult{}, err
	}
	if err := runtime.Start(workflowID, run.GetRunID()); err != nil {
		return EpisodeResult{}, err
	}
	type workflowResult struct {
		output ParentOutput
		err    error
	}
	workflowDone := make(chan workflowResult, 1)
	go func() {
		var output ParentOutput
		workflowDone <- workflowResult{output: output, err: run.Get(ctx, &output)}
	}()
	workStarted := !queuedBoundary
	var completed workflowResult
	for {
		select {
		case <-ctx.Done():
			return EpisodeResult{}, ctx.Err()
		case <-runtime.supersessionDone:
			if !workStarted {
				if err := workWorkers.start(); err != nil {
					return EpisodeResult{}, err
				}
				workStarted = true
			}
			runtime.supersessionDone = nil
		case fault := <-runtime.FaultRequests():
			switch fault.Target {
			case WorkerTargetWork:
				if err := workWorkers.restart(); err != nil {
					return EpisodeResult{}, err
				}
			case WorkerTargetEffect:
				if err := effectWorkers.restart(); err != nil {
					return EpisodeResult{}, err
				}
			case WorkerTargetNone:
			default:
				return EpisodeResult{}, fmt.Errorf("%w: unknown fault target", protocol.ErrInvalidEvidence)
			}
			if err := runtime.CommitFault(fault); err != nil {
				return EpisodeResult{}, err
			}
		case completed = <-workflowDone:
			goto workflowComplete
		}
	}

workflowComplete:
	expectedWorkflowFailure := request.Case == protocol.CaseJoinBarrier &&
		request.Boundary == "required-item-terminal-failure-before-join" && request.Probe != protocol.ProbeUnsafe
	if completed.err != nil && !expectedWorkflowFailure {
		return EpisodeResult{}, completed.err
	}
	if completed.err == nil && expectedWorkflowFailure {
		return EpisodeResult{}, fmt.Errorf("%w: terminal join unexpectedly completed", protocol.ErrInvalidEvidence)
	}
	workflowError := ""
	if completed.err != nil {
		workflowError = completed.err.Error()
	}
	if err := runtime.WaitIdle(ctx); err != nil {
		return EpisodeResult{}, err
	}
	native, err := e.captureAndReplay(ctx, workflowID, run.GetRunID())
	if err != nil {
		return EpisodeResult{}, err
	}
	bundle, err := runtime.BuildBundle(completed.output, workflowError, native)
	if err != nil {
		return EpisodeResult{}, err
	}
	return EpisodeResult{Output: completed.output, WorkflowError: workflowError, NativeHistory: bundle.NativeHistory, Bundle: bundle}, nil
}

type registerActivities func(*ActivityHandler, worker.Worker)

type restartableActivityWorker struct {
	client   client.Client
	queue    string
	prefix   string
	register registerActivities
	runtime  *EpisodeRuntime
	mu       sync.Mutex
	worker   worker.Worker
	epoch    int
}

func newRestartableActivityWorker(temporalClient client.Client, queue, prefix string, register registerActivities, runtime *EpisodeRuntime) *restartableActivityWorker {
	return &restartableActivityWorker{client: temporalClient, queue: queue, prefix: prefix, register: register, runtime: runtime}
}

func (w *restartableActivityWorker) start() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.worker != nil {
		return nil
	}
	w.epoch++
	workerID := fmt.Sprintf("%s-worker-%d", w.prefix, w.epoch)
	value := worker.New(w.client, w.queue, worker.Options{
		Identity: workerID, MaxConcurrentActivityExecutionSize: workerActivityConcurrency, WorkerStopTimeout: 100 * time.Millisecond,
	})
	w.register(&ActivityHandler{Runtime: w.runtime, WorkerID: workerID}, value)
	if err := value.Start(); err != nil {
		return err
	}
	w.worker = value
	return nil
}

func (w *restartableActivityWorker) stop() {
	w.mu.Lock()
	value := w.worker
	w.worker = nil
	w.mu.Unlock()
	if value != nil {
		value.Stop()
	}
}

func (w *restartableActivityWorker) restart() error {
	w.stop()
	return w.start()
}

func (e *TemporalExecutor) captureAndReplay(ctx context.Context, parentWorkflowID, parentRunID string) (protocol.NativeHistory, error) {
	parent, err := readHistory(ctx, e.server.Client(), parentWorkflowID, parentRunID)
	if err != nil {
		return protocol.NativeHistory{}, err
	}
	parentReplayer := worker.NewWorkflowReplayer()
	parentReplayer.RegisterWorkflowWithOptions(ParentWorkflow, workflow.RegisterOptions{Name: ParentWorkflowName})
	if err := parentReplayer.ReplayWorkflowHistory(e.logger, parent); err != nil {
		return protocol.NativeHistory{}, fmt.Errorf("replay parent topology history: %w", err)
	}
	type capturedHistory struct {
		WorkflowID string          `json:"workflow_id"`
		RunID      string          `json:"run_id"`
		History    json.RawMessage `json:"history"`
	}
	parentJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(parent)
	if err != nil {
		return protocol.NativeHistory{}, err
	}
	export := struct {
		Parent   capturedHistory   `json:"parent"`
		Children []capturedHistory `json:"children"`
	}{Parent: capturedHistory{WorkflowID: parentWorkflowID, RunID: parentRunID, History: parentJSON}}
	eventCount := len(parent.Events)
	childReplayer := worker.NewWorkflowReplayer()
	childReplayer.RegisterWorkflowWithOptions(ItemWorkflow, workflow.RegisterOptions{Name: ItemWorkflowName})
	childReplayer.RegisterWorkflowWithOptions(RecoveryItemWorkflow, workflow.RegisterOptions{Name: RecoveryItemWorkflowName})
	for _, child := range childExecutions(parent) {
		history, historyErr := readHistory(ctx, e.server.Client(), child.workflowID, child.runID)
		if historyErr != nil {
			return protocol.NativeHistory{}, historyErr
		}
		if replayErr := childReplayer.ReplayWorkflowHistory(e.logger, history); replayErr != nil {
			return protocol.NativeHistory{}, fmt.Errorf("replay child topology history %q: %w", child.workflowID, replayErr)
		}
		historyJSON, marshalErr := protojson.MarshalOptions{UseProtoNames: true}.Marshal(history)
		if marshalErr != nil {
			return protocol.NativeHistory{}, marshalErr
		}
		export.Children = append(export.Children, capturedHistory{WorkflowID: child.workflowID, RunID: child.runID, History: historyJSON})
		eventCount += len(history.Events)
	}
	exportJSON, err := json.Marshal(export)
	if err != nil {
		return protocol.NativeHistory{}, err
	}
	hash, err := protocol.NativeExportSHA256(exportJSON)
	if err != nil {
		return protocol.NativeHistory{}, err
	}
	replayWorkerSHA256, err := runningExecutableSHA256()
	if err != nil {
		return protocol.NativeHistory{}, fmt.Errorf("hash replay worker executable: %w", err)
	}
	return protocol.NativeHistory{
		Captured: true, EventCount: eventCount, Export: exportJSON, HistorySHA256: hash,
		ReplayCompatible: true, ReplayWorkerSHA256: replayWorkerSHA256,
	}, nil
}

type childExecution struct{ workflowID, runID string }

func childExecutions(history *historypb.History) []childExecution {
	result := make([]childExecution, 0)
	for _, event := range history.Events {
		attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
		if attributes == nil || attributes.WorkflowExecution == nil {
			continue
		}
		result = append(result, childExecution{
			workflowID: attributes.WorkflowExecution.WorkflowId, runID: attributes.WorkflowExecution.RunId,
		})
	}
	return result
}

func readHistory(ctx context.Context, temporalClient client.Client, workflowID, runID string) (*historypb.History, error) {
	iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		history.Events = append(history.Events, event)
	}
	if len(history.Events) == 0 {
		return nil, fmt.Errorf("%w: empty Temporal history", protocol.ErrInvalidEvidence)
	}
	return history, nil
}

func runningExecutableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	return fileSHA256(path)
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256Sum(data)
	return digest, nil
}

func sha256Sum(data []byte) string {
	return fmt.Sprintf("%x", sha256Bytes(data))
}

func sha256Bytes(data []byte) [32]byte {
	return sha256.Sum256(data)
}
