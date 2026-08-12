package temporalagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestWorkflowPreservesOneLogicalActivityIdentityAcrossRetry(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	attempts := make([]int32, 0, 2)
	activityIDs := make([]string, 0, 2)
	environment.RegisterActivityWithOptions(
		func(ctx context.Context, _ ActivityInput) (workstore.Outcome, error) {
			info := activity.GetInfo(ctx)
			attempts = append(attempts, info.Attempt)
			activityIDs = append(activityIDs, info.ActivityID)
			if info.Attempt == 1 {
				return workstore.Outcome{}, errors.New("injected first attempt failure")
			}
			return workstore.Outcome{Value: "accepted"}, nil
		},
		activity.RegisterOptions{Name: ActivityName},
	)

	environment.ExecuteWorkflow(WorkerDeathWorkflow, WorkflowInput{
		SessionID: "session-1", Mode: workstore.ModeReattach,
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	var outcome workstore.Outcome
	if err := environment.GetWorkflowResult(&outcome); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if outcome.Value != "accepted" {
		t.Fatalf("outcome = %+v; want accepted", outcome)
	}
	if !reflect.DeepEqual(attempts, []int32{1, 2}) {
		t.Fatalf("attempts = %v; want [1 2]", attempts)
	}
	wantActivityID := ActivityID("session-1")
	if !reflect.DeepEqual(activityIDs, []string{wantActivityID, wantActivityID}) {
		t.Fatalf("activity IDs = %v; want stable %q", activityIDs, wantActivityID)
	}
}

func TestWorkflowConfiguresFailureDetectionAndBoundedRetry(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	infoChannel := make(chan activity.Info, 1)
	environment.RegisterActivityWithOptions(
		func(ctx context.Context, _ ActivityInput) (workstore.Outcome, error) {
			infoChannel <- activity.GetInfo(ctx)
			return workstore.Outcome{Value: "done"}, nil
		},
		activity.RegisterOptions{Name: ActivityName},
	)
	environment.ExecuteWorkflow(WorkerDeathWorkflow, WorkflowInput{
		SessionID: "session-1", Mode: workstore.ModeFenced, ReplaceOwnerOnRetry: true,
	})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	info := <-infoChannel
	if info.HeartbeatTimeout != AgentHeartbeatTimeout {
		t.Fatalf("heartbeat timeout = %s; want %s", info.HeartbeatTimeout, AgentHeartbeatTimeout)
	}
	if info.HeartbeatTimeout < 10*time.Second {
		t.Fatalf("heartbeat timeout = %s; want at least 10s dispatch margin", info.HeartbeatTimeout)
	}
	if info.StartToCloseTimeout != AgentStartToCloseTimeout {
		t.Fatalf("start-to-close timeout = %s; want %s", info.StartToCloseTimeout, AgentStartToCloseTimeout)
	}
}

func TestWorkflowCancellationRunsDisconnectedApplicationCleanup(t *testing.T) {
	for _, waitForCancellation := range []bool{false, true} {
		t.Run(map[bool]string{false: "do-not-wait", true: "wait"}[waitForCancellation], func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			environment := suite.NewTestWorkflowEnvironment()
			cleanupInputs := make(chan CancelActivityInput, 1)
			environment.RegisterActivityWithOptions(
				func(ctx context.Context, _ ActivityInput) (workstore.Outcome, error) {
					<-ctx.Done()
					return workstore.Outcome{}, context.Cause(ctx)
				},
				activity.RegisterOptions{Name: ActivityName},
			)
			environment.RegisterActivityWithOptions(
				func(_ context.Context, input CancelActivityInput) (CancelActivityResult, error) {
					cleanupInputs <- input
					return CancelActivityResult{Action: workstore.CancelActionCommitted}, nil
				},
				activity.RegisterOptions{Name: CancelActivityName},
			)
			environment.RegisterDelayedCallback(environment.CancelWorkflow, time.Second)
			environment.ExecuteWorkflow(WorkerDeathWorkflow, WorkflowInput{
				SessionID: "session-1", Mode: workstore.ModeFenced,
				EnableCancellationCleanup: true, WaitForCancellation: waitForCancellation,
			})
			if err := environment.GetWorkflowError(); err == nil {
				t.Fatal("canceled Workflow returned nil error")
			}
			select {
			case input := <-cleanupInputs:
				if input.SessionID != "session-1" || input.RequestID == "" {
					t.Fatalf("cleanup input = %+v", input)
				}
			default:
				t.Fatal("disconnected cancellation cleanup did not run")
			}
		})
	}
}

func TestCancellationErrorPrefersWorkflowCancellationOverActivityFailure(t *testing.T) {
	workflowErr := temporal.NewCanceledError()
	activityErr := errors.New("heartbeat timeout won the Activity race")
	if err := cancellationError(workflowErr, activityErr); !temporal.IsCanceledError(err) {
		t.Fatalf("cancellationError = %v; want Workflow cancellation", err)
	}
	if err := cancellationError(nil, activityErr); err != nil {
		t.Fatalf("uncanceled workflow returned cancellation cause %v", err)
	}
}

func TestAgentActivityOptionsExposeCancellationWaitPolicy(t *testing.T) {
	for _, wait := range []bool{false, true} {
		options := agentActivityOptions("session-1", wait)
		if options.WaitForCancellation != wait {
			t.Fatalf("WaitForCancellation = %v; want %v", options.WaitForCancellation, wait)
		}
		if options.ActivityID != ActivityID("session-1") {
			t.Fatalf("ActivityID = %q", options.ActivityID)
		}
	}
}

func TestWorkflowRejectsInvalidInputBeforeSchedulingActivity(t *testing.T) {
	tests := []WorkflowInput{
		{},
		{SessionID: "session", Mode: workstore.ModeReattach, ReplaceOwnerOnRetry: true},
		{SessionID: "session", Mode: workstore.ModeReattach, ReplacePendingLaunchOnRetry: true},
		{
			SessionID: "session", Mode: workstore.ModeFenced,
			ReplaceOwnerOnRetry: true, ReplacePendingLaunchOnRetry: true,
		},
	}
	for _, input := range tests {
		var suite testsuite.WorkflowTestSuite
		environment := suite.NewTestWorkflowEnvironment()
		environment.ExecuteWorkflow(WorkerDeathWorkflow, input)
		if err := environment.GetWorkflowError(); err == nil {
			t.Fatalf("input %+v returned nil error", input)
		}
	}
}

func TestCurrentWorkflowReplaysCapturedHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(WorkerDeathWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, capturedHistoryPath(t)); err != nil {
		t.Fatalf("replay current workflow: %v", err)
	}
}

func TestCurrentWorkflowReplaysPostExecGapHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(WorkerDeathWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, postExecCapturedHistoryPath(t)); err != nil {
		t.Fatalf("replay post-exec workflow: %v", err)
	}
}

func TestDeliberatelyNondeterministicWorkflowChangeIsRejected(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(nondeterministicWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, capturedHistoryPath(t))
	if err == nil {
		t.Fatal("replay accepted a deliberately incompatible Workflow change")
	}
	if !strings.Contains(err.Error(), "TMPRL1100") {
		t.Fatalf("replay error = %v; want nondeterminism rejection", err)
	}
}

func TestCapturedHistoryShowsCompactedActivityRetry(t *testing.T) {
	file, err := os.Open(capturedHistoryPath(t))
	if err != nil {
		t.Fatalf("open captured history: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close captured history: %v", err)
		}
	}()
	var history struct {
		Events []struct {
			EventType                     string `json:"event_type"`
			ActivityTaskStartedAttributes struct {
				Attempt     int32 `json:"attempt"`
				LastFailure struct {
					Message string `json:"message"`
				} `json:"last_failure"`
			} `json:"activity_task_started_event_attributes"`
		} `json:"events"`
	}
	if err := json.NewDecoder(file).Decode(&history); err != nil {
		t.Fatalf("decode captured history: %v", err)
	}
	counts := make(map[string]int)
	var retryAttempt int32
	var lastFailure string
	for _, event := range history.Events {
		counts[event.EventType]++
		if event.EventType == "EVENT_TYPE_ACTIVITY_TASK_STARTED" {
			retryAttempt = event.ActivityTaskStartedAttributes.Attempt
			lastFailure = event.ActivityTaskStartedAttributes.LastFailure.Message
		}
	}
	if counts["EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"] != 1 || counts["EVENT_TYPE_ACTIVITY_TASK_STARTED"] != 1 ||
		counts["EVENT_TYPE_ACTIVITY_TASK_COMPLETED"] != 1 {
		t.Fatalf("Activity history counts = %+v; want one compacted schedule/start/completion", counts)
	}
	if retryAttempt != 2 || !strings.Contains(lastFailure, "Heartbeat timeout") {
		t.Fatalf("started attempt = %d, last failure = %q; want attempt 2 after Heartbeat timeout", retryAttempt, lastFailure)
	}
}

const replayTestTimer = time.Hour

func nondeterministicWorkflow(ctx workflow.Context, input WorkflowInput) (workstore.Outcome, error) {
	if err := workflow.Sleep(ctx, replayTestTimer); err != nil {
		return workstore.Outcome{}, err
	}
	return WorkerDeathWorkflow(ctx, input)
}

func capturedHistoryPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate replay test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	return filepath.Join(
		root, "experiments", "worker-death", "evidence", "milestone1-20260806-v3-reattach", "temporal-history.json",
	)
}

func postExecCapturedHistoryPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate replay test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	return filepath.Join(
		root, "experiments", "worker-death", "evidence",
		"post-exec-gap-20260806-v3-fenced-replacement-trial-1", "temporal-history.json",
	)
}
