package temporaladapter

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
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

type Config struct {
	TemporalPath   string
	WorkRoot       string
	AdapterVersion string
}

type Execution = systemplan.Execution

type Session struct {
	config        Config
	workDir       string
	server        *testsuite.DevServer
	worker        worker.Worker
	taskQueue     string
	serverLog     *os.File
	serverVersion string
	sdkVersion    string
	logger        temporallog.Logger
}

func Open(ctx context.Context, config Config) (_ *Session, err error) {
	if config.TemporalPath == "" || config.WorkRoot == "" || !validSourceVersion(config.AdapterVersion) {
		return nil, fmt.Errorf("%w: Temporal path, work root, and source adapter hash are required", protocol.ErrInvalidEvidence)
	}
	if err := os.MkdirAll(config.WorkRoot, 0o750); err != nil {
		return nil, err
	}
	workDir, err := os.MkdirTemp(config.WorkRoot, ".temporal-v2-adapter-")
	if err != nil {
		return nil, err
	}
	serverLog, err := os.OpenFile(filepath.Join(workDir, "temporal-server.log"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	sdkLogger := temporallog.NewStructuredLogger(slog.New(slog.NewTextHandler(serverLog, &slog.HandlerOptions{Level: slog.LevelWarn})))
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: config.TemporalPath, DBFilename: filepath.Join(workDir, "temporal.db"),
		LogLevel: "warn", LogFormat: "pretty", Stdout: serverLog, Stderr: serverLog,
		ClientOptions: &client.Options{Logger: sdkLogger},
	})
	if err != nil {
		_ = serverLog.Close()
		return nil, fmt.Errorf("start Temporal v2 adapter server: %w", err)
	}
	taskQueue := "agent-durability-v2-" + randomSuffix()
	temporalWorker := worker.New(server.Client(), taskQueue, worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(Workflow, workflow.RegisterOptions{Name: WorkflowName})
	temporalWorker.RegisterActivity(RecordStep)
	if err := temporalWorker.Start(); err != nil {
		server.Client().Close()
		_ = server.Stop()
		_ = serverLog.Close()
		return nil, fmt.Errorf("start Temporal v2 adapter worker: %w", err)
	}
	return &Session{
		config: config, workDir: workDir, server: server, worker: temporalWorker, taskQueue: taskQueue, serverLog: serverLog,
		serverVersion: commandVersion(config.TemporalPath), sdkVersion: moduleVersion("go.temporal.io/sdk"), logger: sdkLogger,
	}, nil
}

func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.worker.Stop()
	s.server.Client().Close()
	return errors.Join(s.server.Stop(), s.serverLog.Close())
}

func (s *Session) WorkDir() string { return s.workDir }

func (s *Session) Execute(ctx context.Context, plan systemplan.Plan) (Execution, error) {
	if err := plan.Validate(); err != nil {
		return Execution{}, err
	}
	workflowID := fmt.Sprintf("adl-v2/%s/%s/%d/%s", plan.Case, plan.Probe, plan.Trial, randomSuffix())
	run, err := s.server.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: s.taskQueue, WorkflowExecutionTimeout: time.Minute,
	}, WorkflowName, plan)
	if err != nil {
		return Execution{}, fmt.Errorf("start Temporal v2 Workflow: %w", err)
	}
	var result Result
	if err := run.Get(ctx, &result); err != nil {
		return Execution{}, fmt.Errorf("complete Temporal v2 Workflow: %w", err)
	}
	if err := validateReceipts(plan, result); err != nil {
		return Execution{}, err
	}
	history, err := readHistory(ctx, s.server.Client(), workflowID, run.GetRunID())
	if err != nil {
		return Execution{}, err
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(Workflow, workflow.RegisterOptions{Name: WorkflowName})
	if err := replayer.ReplayWorkflowHistory(s.logger, history); err != nil {
		return Execution{}, fmt.Errorf("replay Temporal v2 history: %w", err)
	}
	native, err := historyRecords(history)
	if err != nil {
		return Execution{}, err
	}
	return Execution{
		SystemID: "temporal", AdapterID: "temporal-v2", AdapterVersion: s.config.AdapterVersion,
		ExecutionID: workflowID + "/" + run.GetRunID(), Native: native, ReplayVerified: true,
		Settings: map[string]string{
			"track": track(plan.Probe), "temporal_cli": s.serverVersion, "temporal_sdk": s.sdkVersion,
			"workflow": WorkflowName, "activity_retry_max_attempts": "2", "history_replay": "passed",
		},
	}, nil
}

func validateReceipts(plan systemplan.Plan, result Result) error {
	if len(result.Receipts) != len(plan.Steps) {
		return fmt.Errorf("%w: Temporal receipt count differs from plan", protocol.ErrInvalidEvidence)
	}
	for index, receipt := range result.Receipts {
		wantAttempt := int32(1)
		if plan.Steps[index].FailureOnce {
			wantAttempt = 2
		}
		if receipt.StepID != plan.Steps[index].ID || receipt.Kind != plan.Steps[index].Kind || receipt.Attempt != wantAttempt {
			return fmt.Errorf("%w: Temporal receipt %d differs from durable plan", protocol.ErrInvalidEvidence, index)
		}
	}
	return nil
}

func readHistory(ctx context.Context, temporalClient client.Client, workflowID, runID string) (*historypb.History, error) {
	iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{Events: make([]*historypb.HistoryEvent, 0)}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, fmt.Errorf("read Temporal v2 history: %w", err)
		}
		history.Events = append(history.Events, event)
	}
	if len(history.Events) == 0 {
		return nil, fmt.Errorf("%w: Temporal history is empty", protocol.ErrInvalidEvidence)
	}
	return history, nil
}

func historyRecords(history *historypb.History) ([]protocol.NativeRecord, error) {
	records := make([]protocol.NativeRecord, 0, len(history.Events))
	for _, event := range history.Events {
		data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("encode Temporal history event: %w", err)
		}
		at := event.GetEventTime().AsTime().UTC()
		records = append(records, protocol.NativeRecord{
			Sequence: uint64(len(records) + 1), Time: at.Format(time.RFC3339Nano),
			Kind: event.GetEventType().String(), Detail: string(data),
		})
	}
	return records, nil
}

func track(probe protocol.Probe) string {
	if probe == protocol.ProbeProtected {
		return "portable-safety"
	}
	return "native-minimum"
}

func validSourceVersion(value string) bool {
	encoded := strings.TrimPrefix(value, "source-sha256:")
	decoded, err := hex.DecodeString(encoded)
	return value != encoded && err == nil && len(decoded) == 32
}

func randomSuffix() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func commandVersion(path string) string {
	output, err := exec.Command(path, "--version").CombinedOutput()
	if err != nil {
		return "unknown: " + err.Error()
	}
	return strings.TrimSpace(string(output))
}

func moduleVersion(path string) string {
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

func MarshalHistory(history *historypb.History) ([]byte, error) {
	data, err := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}.Marshal(history)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
