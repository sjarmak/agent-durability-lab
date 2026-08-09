package publication

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

const temporalPublicationWarmupName = "AgentDurabilityPublicationWarmupV1"

type TemporalExecutorConfig struct {
	TemporalPath      string
	WorkRoot          string
	EvidenceRoot      string
	AdapterVersion    string
	AgentBinarySHA256 string
}

type TemporalTimedExecutor struct {
	config        TemporalExecutorConfig
	workDir       string
	server        *testsuite.DevServer
	worker        worker.Worker
	taskQueue     string
	serverLog     *os.File
	logger        temporallog.Logger
	serverVersion string
	sdkVersion    string
	mu            sync.Mutex
	runtimes      map[string]*EpisodeRuntime
	closed        bool
}

func OpenTemporalTimedExecutor(ctx context.Context, config TemporalExecutorConfig) (_ *TemporalTimedExecutor, returnErr error) {
	if config.TemporalPath == "" || config.WorkRoot == "" || config.EvidenceRoot == "" ||
		!validSourceHash(config.AdapterVersion) || !validDigest(config.AgentBinarySHA256) {
		return nil, invalid("Temporal timed executor configuration")
	}
	if err := os.MkdirAll(config.WorkRoot, 0o750); err != nil {
		return nil, err
	}
	workDir, err := os.MkdirTemp(config.WorkRoot, ".temporal-publication-")
	if err != nil {
		return nil, err
	}
	serverLog, err := os.OpenFile(filepath.Join(workDir, "temporal-server.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	logger := temporallog.NewStructuredLogger(slog.New(slog.NewTextHandler(serverLog, &slog.HandlerOptions{Level: slog.LevelWarn})))
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: config.TemporalPath, DBFilename: filepath.Join(workDir, "temporal.db"),
		LogLevel: "warn", LogFormat: "pretty", Stdout: serverLog, Stderr: serverLog,
		ClientOptions: &client.Options{Logger: logger},
	})
	if err != nil {
		_ = serverLog.Close()
		return nil, fmt.Errorf("start Temporal publication server: %w", err)
	}
	executor := &TemporalTimedExecutor{
		config: config, workDir: workDir, server: server, taskQueue: "adl-publication-" + randomHex(),
		serverLog: serverLog, logger: logger, serverVersion: binaryVersion(config.TemporalPath),
		sdkVersion: dependencyVersion("go.temporal.io/sdk"), runtimes: make(map[string]*EpisodeRuntime),
	}
	workerID := "publication-worker-" + randomHex()
	activityHandler := &temporalActivityHandler{lookup: executor.runtime, workerID: workerID}
	temporalWorker := worker.New(server.Client(), executor.taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: 8,
		Identity:                           workerID,
	})
	temporalWorker.RegisterWorkflowWithOptions(TemporalPublicationWorkflow, workflow.RegisterOptions{Name: TemporalPublicationWorkflowName})
	temporalWorker.RegisterWorkflowWithOptions(temporalPublicationWarmup, workflow.RegisterOptions{Name: temporalPublicationWarmupName})
	temporalWorker.RegisterActivityWithOptions(activityHandler.Execute, activity.RegisterOptions{Name: TemporalPublicationActivityName})
	if err := temporalWorker.Start(); err != nil {
		server.Client().Close()
		_ = server.Stop()
		_ = serverLog.Close()
		return nil, fmt.Errorf("start Temporal publication Worker: %w", err)
	}
	executor.worker = temporalWorker
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, executor.Close())
		}
	}()
	if err := executor.prewarm(ctx); err != nil {
		return nil, err
	}
	return executor, nil
}

func (e *TemporalTimedExecutor) SystemID() string { return SystemTemporal }

func (e *TemporalTimedExecutor) Ready(ctx context.Context) error {
	e.mu.Lock()
	active, closed := len(e.runtimes), e.closed
	e.mu.Unlock()
	if closed || active != 0 {
		return fmt.Errorf("Temporal publication executor is not idle: closed=%t active=%d", closed, active)
	}
	_, err := e.server.Client().CheckHealth(ctx, &client.CheckHealthRequest{})
	return err
}

func (e *TemporalTimedExecutor) ExecuteTimed(ctx context.Context, request EpisodeRequest, timing *TimingRecorder) (TimedResult, error) {
	plan, err := BuildEpisodePlan(request)
	if err != nil {
		return TimedResult{}, err
	}
	episode, err := NewEpisodeRuntime(EpisodeRuntimeConfig{
		Request: request, Plan: plan, SystemID: e.SystemID(), AdapterID: "temporal-publication-v1",
		AdapterVersion: e.config.AdapterVersion, AgentBinarySHA256: e.config.AgentBinarySHA256,
		Clock: wallClock{}, Timing: timing, Settings: map[string]string{
			"temporal_cli": e.serverVersion, "temporal_sdk": e.sdkVersion,
			"workflow": TemporalPublicationWorkflowName, "activity_concurrency": "8", "history_replay": "passed-before-evidence-admission",
		},
	})
	if err != nil {
		return TimedResult{}, err
	}
	executionKey := episode.runID
	if err := e.addRuntime(executionKey, episode); err != nil {
		return TimedResult{}, err
	}
	defer e.removeRuntime(executionKey)
	workflowID := "adl-publication-" + digest(request.PairID)[:32]
	run, err := e.server.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: e.taskQueue, WorkflowExecutionTimeout: 2 * time.Minute,
	}, TemporalPublicationWorkflowName, TemporalWorkflowInput{ExecutionKey: executionKey, Plan: plan})
	if err != nil {
		return TimedResult{ExecutionID: workflowID, EvidenceDir: e.workDir}, fmt.Errorf("start Temporal publication Workflow: %w", err)
	}
	if err := run.Get(ctx, nil); err != nil {
		return TimedResult{ExecutionID: workflowID + "/" + run.GetRunID(), EvidenceDir: e.workDir}, fmt.Errorf("complete Temporal publication Workflow: %w", err)
	}
	episode.Acknowledge()
	history, err := readTemporalPublicationHistory(ctx, e.server.Client(), workflowID, run.GetRunID())
	if err != nil {
		return TimedResult{ExecutionID: workflowID + "/" + run.GetRunID(), EvidenceDir: e.workDir}, err
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(TemporalPublicationWorkflow, workflow.RegisterOptions{Name: TemporalPublicationWorkflowName})
	if err := replayer.ReplayWorkflowHistory(e.logger, history); err != nil {
		return TimedResult{ExecutionID: workflowID + "/" + run.GetRunID(), EvidenceDir: e.workDir}, fmt.Errorf("replay Temporal publication history: %w", err)
	}
	native, err := temporalPublicationHistoryRecords(history)
	if err != nil {
		return TimedResult{ExecutionID: workflowID + "/" + run.GetRunID(), EvidenceDir: e.workDir}, err
	}
	result, err := episode.Finish(ctx, e.config.EvidenceRoot, native)
	result.ExecutionID = workflowID + "/" + run.GetRunID()
	return result, err
}

func (e *TemporalTimedExecutor) Close() error {
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
	e.worker.Stop()
	e.server.Client().Close()
	return errors.Join(e.server.Stop(), e.serverLog.Close())
}

func (e *TemporalTimedExecutor) addRuntime(key string, episode *EpisodeRuntime) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.runtimes[key] != nil {
		return invalid("Temporal publication runtime registration")
	}
	e.runtimes[key] = episode
	return nil
}

func (e *TemporalTimedExecutor) removeRuntime(key string) {
	e.mu.Lock()
	delete(e.runtimes, key)
	e.mu.Unlock()
}

func (e *TemporalTimedExecutor) runtime(key string) (*EpisodeRuntime, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	episode, ok := e.runtimes[key]
	return episode, ok
}

func (e *TemporalTimedExecutor) prewarm(ctx context.Context) error {
	if _, err := e.server.Client().CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
		return err
	}
	run, err := e.server.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "adl-publication-warmup-" + randomHex(), TaskQueue: e.taskQueue, WorkflowExecutionTimeout: time.Minute,
	}, temporalPublicationWarmupName)
	if err != nil {
		return err
	}
	return run.Get(ctx, nil)
}

func temporalPublicationWarmup(ctx workflow.Context) error {
	return workflow.Sleep(ctx, time.Millisecond)
}

func readTemporalPublicationHistory(ctx context.Context, temporalClient client.Client, workflowID, runID string) (*historypb.History, error) {
	iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{Events: make([]*historypb.HistoryEvent, 0)}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		history.Events = append(history.Events, event)
	}
	if len(history.Events) == 0 {
		return nil, invalid("Temporal publication history")
	}
	return history, nil
}

func temporalPublicationHistoryRecords(history *historypb.History) ([]protocol.NativeRecord, error) {
	records := make([]protocol.NativeRecord, 0, len(history.Events))
	for _, event := range history.Events {
		detail, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(event)
		if err != nil {
			return nil, err
		}
		records = append(records, protocol.NativeRecord{
			Sequence: uint64(len(records) + 1), Time: event.GetEventTime().AsTime().UTC().Format(time.RFC3339Nano),
			Kind: event.GetEventType().String(), Detail: string(detail),
		})
	}
	return records, nil
}

func randomHex() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func binaryVersion(path string) string {
	output, err := exec.Command(path, "--version").CombinedOutput()
	if err != nil {
		return "unknown: " + err.Error()
	}
	return strings.TrimSpace(string(output))
}

func dependencyVersion(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dependency := range info.Deps {
		if dependency.Path == path {
			return dependency.Version
		}
	}
	return "unknown"
}
