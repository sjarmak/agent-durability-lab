package lab

import (
	"encoding/json"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

func TestValidatePreservedHistoryBindsWorkflowInputAndResult(t *testing.T) {
	input := ClaudeActivityInput{
		LogicalSessionID: "logical-session", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
	}
	result := ClaudeActivityResult{
		TemporalAttempt: 1, PhysicalAttemptID: "attempt-1",
		VendorSessionID: "11111111-2222-4333-8444-555555555555",
		Result:          "EFFECT_COMPLETE", ProcessIdentity: "pid:1:start:boot:1",
	}
	inputData, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	resultData, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	value := &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowId: "workflow-1", OriginalExecutionRunId: "run-1",
					WorkflowType: &commonpb.WorkflowType{Name: DirectClaudeWorkflowName},
					Input:        &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: inputData}}},
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{Attempt: 1},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: resultData}}},
				},
			},
		},
	}}
	summary := trialSummary{WorkflowID: "workflow-1", WorkflowRunID: "run-1", WorkflowResult: result}
	if err := validatePreservedHistory(value, input.LogicalSessionID, summary); err != nil {
		t.Fatalf("validate matching history: %v", err)
	}

	changed := summary
	changed.WorkflowID = "another-workflow"
	if err := validatePreservedHistory(value, input.LogicalSessionID, changed); err == nil {
		t.Fatal("history from another Workflow returned nil error")
	}
	changed = summary
	changed.WorkflowResult.VendorSessionID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	if err := validatePreservedHistory(value, input.LogicalSessionID, changed); err == nil {
		t.Fatal("history with another completion result returned nil error")
	}
}
