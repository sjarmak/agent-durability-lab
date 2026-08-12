package lab

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestReplayRejectsMalformedCodexWorkflowHistory(t *testing.T) {
	if err := replayWorkflowHistory([]byte(`{"events":[`)); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("replay error = %v", err)
	}
}

func TestValidatePreservedCodexHistoryBindsIdentityResultAndAcceptedAttempt(t *testing.T) {
	input := CodexActivityInput{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		RecoveryMode: RecoveryModeResumeOnly,
	}
	result := CodexActivityResult{
		TemporalAttempt: 2, PhysicalAttemptID: "attempt-2", ThreadID: testThreadID,
		Result: "EFFECT_COMPLETE", ProcessIdentity: "pid:2:start:test",
	}
	inputPayloads, err := converter.GetDefaultDataConverter().ToPayloads(input)
	if err != nil {
		t.Fatal(err)
	}
	resultPayloads, err := converter.GetDefaultDataConverter().ToPayloads(result)
	if err != nil {
		t.Fatal(err)
	}
	value := &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: CodexWorkflowName}, Input: inputPayloads,
					WorkflowId: "codex-direct/session-1", OriginalExecutionRunId: "run-1",
				},
			}},
		{EventId: 5, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId: codexActivityID, ActivityType: &commonpb.ActivityType{Name: RunCodexActivityName},
					Input: inputPayloads,
				},
			}},
		{EventId: 6, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: 5, Attempt: 1},
			}},
		{EventId: 8, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					ScheduledEventId: 5, Attempt: 2, LastFailure: &failurepb.Failure{
						Message: "attempt 1 heartbeat timeout",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
						}},
					},
				},
			}},
		{EventId: 11, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: resultPayloads},
			}},
	}}
	summary := trialSummary{
		Mode: RecoveryModeResumeOnly, LogicalSessionID: "session-1", LogicalTurnID: "turn-1",
		LogicalEffectID: "effect-1", WorkflowID: "codex-direct/session-1", WorkflowRunID: "run-1",
		WorkflowResult: result,
	}
	if err := validatePreservedCodexHistory(value, summary); err != nil {
		t.Fatalf("matching history: %v", err)
	}
	compactRetry := &historypb.History{Events: []*historypb.HistoryEvent{
		value.Events[0], value.Events[1], value.Events[3], value.Events[4],
	}}
	if err := validatePreservedCodexHistory(compactRetry, summary); err != nil {
		t.Fatalf("Temporal compact retry history: %v", err)
	}
	applicationRetry := proto.Clone(value).(*historypb.History)
	applicationRetry.Events = append(applicationRetry.Events[:2], applicationRetry.Events[3:]...)
	applicationRetry.Events[2].GetActivityTaskStartedEventAttributes().LastFailure = &failurepb.Failure{
		Message: "supervisor request failed",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
		},
	}
	replacementSummary := summary
	replacementSummary.FaultBoundary = FaultProcessFailureReplacement
	if err := validatePreservedCodexHistory(applicationRetry, replacementSummary); err != nil {
		t.Fatalf("replacement compact retry history: %v", err)
	}
	if err := validatePreservedCodexHistory(applicationRetry, summary); err == nil {
		t.Fatal("replacement application failure was accepted for a Worker-loss boundary")
	}
	mutations := []func(*historypb.History){
		func(changed *historypb.History) {
			changed.Events[0].GetWorkflowExecutionStartedEventAttributes().WorkflowId = "codex-direct/other"
		},
		func(changed *historypb.History) {
			changed.Events[1].GetActivityTaskScheduledEventAttributes().ActivityId = "other"
		},
		func(changed *historypb.History) {
			changed.Events[3].GetActivityTaskStartedEventAttributes().ScheduledEventId = 99
		},
		func(changed *historypb.History) {
			changed.Events[3].GetActivityTaskStartedEventAttributes().Attempt = 3
		},
		func(changed *historypb.History) {
			changed.Events[3].GetActivityTaskStartedEventAttributes().LastFailure = nil
		},
		func(changed *historypb.History) {
			changed.Events[4].GetWorkflowExecutionCompletedEventAttributes().Result = inputPayloads
		},
	}
	for index, mutate := range mutations {
		encoded, marshalErr := protojson.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		changed := &historypb.History{}
		if unmarshalErr := protojson.Unmarshal(encoded, changed); unmarshalErr != nil {
			t.Fatal(unmarshalErr)
		}
		mutate(changed)
		if err := validatePreservedCodexHistory(changed, summary); err == nil {
			t.Fatalf("history mutation %d was accepted", index)
		}
	}
}

func TestDecodeCompletedCodexWorkflowResult(t *testing.T) {
	want := CodexActivityResult{
		TemporalAttempt: 2, PhysicalAttemptID: "attempt-2", ThreadID: "thread-1",
		Result: "EFFECT_COMPLETE", ProcessIdentity: "process-1",
	}
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(want)
	if err != nil {
		t.Fatalf("encode result: %v", err)
	}
	event := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: payloads},
		},
	}

	got, err := decodeCompletedCodexWorkflowResult(event)
	if err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if got != want {
		t.Fatalf("decoded result = %+v, want %+v", got, want)
	}
}

func TestDecodeCompletedCodexWorkflowResultRejectsOtherCloseEvents(t *testing.T) {
	event := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED}
	if _, err := decodeCompletedCodexWorkflowResult(event); err == nil || !strings.Contains(err.Error(), "WorkflowExecutionCanceled") {
		t.Fatalf("decode error = %v", err)
	}
}

func TestDecodeCompletedCodexWorkflowResultRejectsMissingResult(t *testing.T) {
	event := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
		},
	}
	if _, err := decodeCompletedCodexWorkflowResult(event); err == nil || !strings.Contains(err.Error(), "missing result") {
		t.Fatalf("decode error = %v", err)
	}
}

func TestValidateCanceledCodexWorkflowClose(t *testing.T) {
	if err := validateCanceledCodexWorkflowClose(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
	}); err != nil {
		t.Fatalf("validate canceled close: %v", err)
	}
	if err := validateCanceledCodexWorkflowClose(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}); err == nil || !strings.Contains(err.Error(), "WorkflowExecutionCompleted") {
		t.Fatalf("completed close error = %v", err)
	}
}

func TestNextCloseHistoryQueryDelayHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := waitForNextCloseHistoryQuery(ctx, time.Hour); err == nil {
		t.Fatal("wait returned nil after cancellation")
	}
	if time.Since(started) > time.Second {
		t.Fatal("canceled close-history wait did not return promptly")
	}
}

func TestCodexWorkflowCloseEventClassification(t *testing.T) {
	for _, eventType := range []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
	} {
		if !isCodexWorkflowCloseEvent(&historypb.HistoryEvent{EventType: eventType}) {
			t.Fatalf("%s was not classified as a close event", eventType)
		}
	}
	if isCodexWorkflowCloseEvent(nil) {
		t.Fatal("nil was classified as a close event")
	}
	if isCodexWorkflowCloseEvent(&historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED}) {
		t.Fatal("Activity completion was classified as Workflow close")
	}
}

func TestCodexWorkflowClosedStatusClassification(t *testing.T) {
	for _, status := range []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	} {
		if !isCodexWorkflowClosedStatus(status) {
			t.Fatalf("%s was not classified as closed", status)
		}
	}
	if isCodexWorkflowClosedStatus(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
		t.Fatal("running Workflow was classified as closed")
	}
	if isCodexWorkflowClosedStatus(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED) {
		t.Fatal("unspecified Workflow status was classified as closed")
	}
}

func TestWorkflowQueryRetryRecognizesTransientContextAndGRPC(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		status.Error(codes.DeadlineExceeded, "deadline"),
		status.Error(codes.Canceled, "stream terminated by RST_STREAM with error code: CANCEL"),
		status.Error(codes.Unavailable, "unavailable"),
		status.Error(codes.ResourceExhausted, "busy"),
	} {
		if !isWorkflowQueryRetryable(err) {
			t.Fatalf("transient Workflow query error was not retryable: %v", err)
		}
	}
	for _, err := range []error{
		context.Canceled,
		status.Error(codes.InvalidArgument, "invalid"),
		status.Error(codes.PermissionDenied, "denied"),
	} {
		if isWorkflowQueryRetryable(err) {
			t.Fatalf("terminal Workflow query error was retryable: %v", err)
		}
	}
}

func TestCompletedCodexWorkflowPayloadMustBeDecodable(t *testing.T) {
	event := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("not-json")}}},
			},
		},
	}
	if _, err := decodeCompletedCodexWorkflowResult(event); err == nil {
		t.Fatal("malformed result decoded without error")
	}
}

func TestLiveWorkflowStatusAndClosedHistoryRead(t *testing.T) {
	address := os.Getenv("CODEX_DIRECT_TEMPORAL_ADDRESS")
	workflowID := os.Getenv("CODEX_DIRECT_WORKFLOW_ID")
	runID := os.Getenv("CODEX_DIRECT_WORKFLOW_RUN_ID")
	if address == "" || workflowID == "" || runID == "" {
		t.Skip("set the Codex direct Temporal address, Workflow ID, and Run ID")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort: address, Namespace: "default", Identity: "codex-history-regression-test",
	})
	if err != nil {
		t.Fatalf("connect to Temporal: %v", err)
	}
	defer temporalClient.Close()
	description, err := temporalClient.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		t.Fatalf("describe Workflow: %v", err)
	}
	closed := isCodexWorkflowClosedStatus(description.WorkflowExecutionInfo.Status)
	if os.Getenv("CODEX_DIRECT_EXPECT_OPEN_WORKFLOW") == "1" {
		if closed {
			t.Fatalf("Workflow status = %s, want open", description.WorkflowExecutionInfo.Status)
		}
		return
	}
	if !closed {
		t.Fatalf("Workflow status = %s, want closed", description.WorkflowExecutionInfo.Status)
	}
	event, err := readWorkflowCloseHistory(ctx, temporalClient, workflowID, runID)
	if err != nil {
		t.Fatalf("read closed Workflow history: %v", err)
	}
	if event == nil {
		t.Fatal("closed Workflow history did not contain a terminal event")
	}
	if _, err := decodeCompletedCodexWorkflowResult(event); err != nil {
		t.Fatalf("decode closed Workflow result: %v", err)
	}
}
