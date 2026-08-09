package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/sandbox-harness/internal/provider"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	sandboxworkflow "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const childStartedPoint = "parent-child-workflow-started"

type parentInput struct {
	ChildID    string
	BarrierURL string
	SessionID  string
}

type childStartedInput struct {
	BarrierURL string
	SessionID  string
	ChildID    string
}

type parentCloseManifest struct {
	SchemaVersion    int            `json:"schema_version"`
	RunID            string         `json:"run_id"`
	Probe            protocol.Probe `json:"probe"`
	Trial            int            `json:"trial"`
	UpstreamCommit   string         `json:"upstream_commit"`
	UpstreamVersion  string         `json:"upstream_version"`
	SourceSHA256     string         `json:"source_sha256"`
	ParentWorkflowID string         `json:"parent_workflow_id"`
	ChildWorkflowID  string         `json:"child_workflow_id"`
	ChildRunID       string         `json:"child_run_id"`
	FaultBoundary    string         `json:"fault_boundary"`
	Invariant        string         `json:"invariant"`
	Falsifier        string         `json:"falsifier"`
	CanceledAt       time.Time      `json:"canceled_at"`
	CompletedAt      time.Time      `json:"completed_at"`
}

type parentCloseVerdict struct {
	Class               string `json:"class"`
	ExpectedObservation bool   `json:"expected_observation"`
	ActiveAtClose       int    `json:"active_instances_at_close"`
	ActiveAfterRecovery int    `json:"active_instances_after_recovery"`
	CleanupReceipt      string `json:"cleanup_receipt,omitempty"`
	Responsibility      string `json:"responsibility"`
}

func parentSandboxWorkflow(ctx workflow.Context, input parentInput) error {
	childOptions := workflow.ChildWorkflowOptions{
		WorkflowID:        input.ChildID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}
	child := workflow.ExecuteChildWorkflow(
		workflow.WithChildOptions(ctx, childOptions),
		sandboxworkflow.SandboxWorkflowType,
		sandboxworkflow.SandboxLocalState{},
	)
	var execution workflow.Execution
	if err := child.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
		return err
	}
	notifyContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := workflow.ExecuteActivity(notifyContext, notifyChildStarted, childStartedInput{
		BarrierURL: input.BarrierURL, SessionID: input.SessionID, ChildID: execution.ID,
	}).Get(ctx, nil); err != nil {
		return err
	}
	return child.Get(ctx, nil)
}

func notifyChildStarted(ctx context.Context, input childStartedInput) error {
	return failureinject.NewClient(input.BarrierURL).Arrive(ctx, failureinject.Arrival{
		ID: input.ChildID, Point: childStartedPoint, SessionID: input.SessionID,
		ActorID: "parent-workflow", ProcessStart: "child-start-notifier",
	})
}

func (e liveEnvironment) runParentCloseDuringInit(
	ctx context.Context,
	probe protocol.Probe,
	trial int,
) (string, error) {
	workDirectory, err := os.MkdirTemp("", "sandbox-parent-close-")
	if err != nil {
		return "", err
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
	barrierServer := newBarrierServer(coordinator)
	defer barrierServer.Close()
	runID := fmt.Sprintf("sandbox-harness-parent-close-%s-trial-%d", probe, trial)
	parentID, childID := runID+"/parent", runID+"/child"
	parentRun, err := e.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: parentID, TaskQueue: e.taskQueue},
		parentWorkflow, parentInput{ChildID: childID, BarrierURL: barrierServer.URL, SessionID: runID})
	if err != nil {
		return "", err
	}
	if _, err := coordinator.WaitForArrivals(ctx, childStartedPoint, 1); err != nil {
		return "", err
	}
	defer func() { _ = coordinator.Release(childStartedPoint) }()
	description, err := e.client.DescribeWorkflowExecution(ctx, childID, "")
	if err != nil {
		return "", fmt.Errorf("describe started child: %w", err)
	}
	childRunID := description.WorkflowExecutionInfo.Execution.RunId
	childRun := e.client.GetWorkflow(ctx, childID, childRunID)
	if err := coordinator.Release(childStartedPoint); err != nil {
		return "", err
	}
	config := provider.Config{
		DatabasePath: filepath.Join(workDirectory, "provider.db"), Mode: mode,
		BarrierURL: barrierServer.URL, FaultOperation: provider.OperationStart,
		SessionID: runID, WorkerIdentity: "sandbox-harness-worker", Generation: 1,
	}
	initResult := make(chan error, 1)
	go func() { initResult <- e.initialize(ctx, childRun, config, nil) }()
	providerPoint := "provider-start-effect-committed"
	if _, err := coordinator.WaitForArrivals(ctx, providerPoint, 1); err != nil {
		return "", err
	}
	defer func() { _ = coordinator.Release(providerPoint) }()
	atClose, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	if activeInstanceCount(atClose) != 1 {
		return "", fmt.Errorf("active instances before parent close = %d, want 1", activeInstanceCount(atClose))
	}
	canceledAt := time.Now().UTC()
	if err := e.client.CancelWorkflow(ctx, parentRun.GetID(), parentRun.GetRunID()); err != nil {
		return "", fmt.Errorf("cancel parent Workflow: %w", err)
	}
	if err := waitForHistoryEvent(ctx, e.client, childID, childRunID, enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED); err != nil {
		return "", fmt.Errorf("wait for child cancellation request: %w", err)
	}
	if err := coordinator.Release(providerPoint); err != nil {
		return "", err
	}
	if err := <-initResult; err == nil {
		return "", errors.New("init Update unexpectedly completed after parent cancellation")
	}
	if err := waitForCanceledOrCompleted(ctx, parentRun); err != nil {
		return "", fmt.Errorf("parent terminal result: %w", err)
	}
	if err := waitForCanceledOrCompleted(ctx, childRun); err != nil {
		return "", fmt.Errorf("child terminal result: %w", err)
	}
	afterClose, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	cleanupReceipt := ""
	if probe == protocol.ProbeProtected {
		cleanupReceipt, err = reconcileActiveInstances(ctx, store, runID, afterClose)
		if err != nil {
			return "", err
		}
	}
	finalState, err := store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	activeAfter := activeInstanceCount(finalState)
	if probe == protocol.ProbeUnsafe && activeAfter == 0 {
		return "", errors.New("unsafe parent-close control did not preserve an orphan")
	}
	if probe == protocol.ProbeProtected && activeAfter != 0 {
		return "", fmt.Errorf("reconciliation left %d active instances", activeAfter)
	}
	return e.writeParentCloseEvidence(ctx, parentCloseEvidenceInput{
		RunID: runID, Probe: probe, Trial: trial, ParentRun: parentRun, ChildRunID: childRunID,
		ChildID: childID, AtClose: atClose, Final: finalState, CleanupReceipt: cleanupReceipt,
		CanceledAt: canceledAt, CompletedAt: time.Now().UTC(),
	})
}

type parentCloseEvidenceInput struct {
	RunID          string
	Probe          protocol.Probe
	Trial          int
	ParentRun      client.WorkflowRun
	ChildID        string
	ChildRunID     string
	AtClose        provider.State
	Final          provider.State
	CleanupReceipt string
	CanceledAt     time.Time
	CompletedAt    time.Time
}

func (e liveEnvironment) writeParentCloseEvidence(ctx context.Context, input parentCloseEvidenceInput) (string, error) {
	runDir := filepath.Join(e.evidenceRoot, input.RunID)
	if err := os.Mkdir(runDir, 0o750); err != nil {
		return runDir, fmt.Errorf("create parent-close evidence directory: %w", err)
	}
	parentHistory, err := readHistoryJSON(ctx, e.client, input.ParentRun.GetID(), input.ParentRun.GetRunID())
	if err != nil {
		return runDir, err
	}
	childHistory, err := readHistoryJSON(ctx, e.client, input.ChildID, input.ChildRunID)
	if err != nil {
		return runDir, err
	}
	manifest := parentCloseManifest{
		SchemaVersion: 1, RunID: input.RunID, Probe: input.Probe, Trial: input.Trial,
		UpstreamCommit: upstreamCommit, UpstreamVersion: upstreamVersion,
		SourceSHA256:     e.sourceSHA,
		ParentWorkflowID: input.ParentRun.GetID(), ChildWorkflowID: input.ChildID, ChildRunID: input.ChildRunID,
		FaultBoundary: "provider create committed; child cancellation requested before init Activity completion",
		Invariant:     "parent close cannot leave a live provider instance without a durable owner or cleanup disposition",
		Falsifier:     "protected reconciliation leaves an active instance, lacks a cleanup receipt, or no child cancellation event brackets the fault",
		CanceledAt:    input.CanceledAt, CompletedAt: input.CompletedAt,
	}
	class := "valid-fail"
	responsibility := "harness cleanup skipped because provider status was never durably accepted"
	if input.Probe == protocol.ProbeProtected {
		class = "valid-pass"
		responsibility = "external provider reconciliation stopped the unaccepted instance"
	}
	verdict := parentCloseVerdict{
		Class: class, ExpectedObservation: true, ActiveAtClose: activeInstanceCount(input.AtClose),
		ActiveAfterRecovery: activeInstanceCount(input.Final), CleanupReceipt: input.CleanupReceipt,
		Responsibility: responsibility,
	}
	files := map[string]any{
		"manifest.json": manifest, "provider-state-at-close.json": input.AtClose,
		"provider-state-final.json": input.Final, "verdict.json": verdict,
	}
	for name, value := range files {
		encoded, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return runDir, err
		}
		if err := writeExclusive(filepath.Join(runDir, name), encoded); err != nil {
			return runDir, err
		}
	}
	if err := writeExclusive(filepath.Join(runDir, "parent-history.json"), parentHistory); err != nil {
		return runDir, err
	}
	if err := writeExclusive(filepath.Join(runDir, "child-history.json"), childHistory); err != nil {
		return runDir, err
	}
	return runDir, nil
}

func reconcileActiveInstances(ctx context.Context, store *provider.Store, sessionID string, state provider.State) (string, error) {
	receipt := ""
	for _, instance := range state.Instances {
		if !instance.Active {
			continue
		}
		result, err := store.Apply(ctx, provider.Request{
			Kind: provider.OperationStop, OperationID: "reconcile/" + sessionID + "/" + instance.InstanceID,
			PhysicalAttemptID: "reconciler/" + sessionID + "/" + instance.InstanceID,
			InstanceID:        instance.InstanceID, WorkerIdentity: "external-provider-reconciler",
		})
		if err != nil {
			return "", err
		}
		receipt = result.ReceiptID
	}
	if receipt == "" {
		return "", errors.New("reconciler found no active provider instance")
	}
	return receipt, nil
}

func activeInstanceCount(state provider.State) int {
	count := 0
	for _, instance := range state.Instances {
		if instance.Active {
			count++
		}
	}
	return count
}

func waitForCanceledOrCompleted(ctx context.Context, run client.WorkflowRun) error {
	err := run.Get(ctx, nil)
	if err == nil || temporal.IsCanceledError(err) {
		return nil
	}
	return err
}

func waitForHistoryEvent(
	ctx context.Context,
	temporalClient client.Client,
	workflowID string,
	runID string,
	want enums.EventType,
) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iterator.HasNext() {
			event, err := iterator.Next()
			if err != nil {
				return err
			}
			if event.EventType == want {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func newBarrierServer(coordinator *failureinject.Coordinator) *httptest.Server {
	return httptest.NewServer(coordinator.Handler())
}
