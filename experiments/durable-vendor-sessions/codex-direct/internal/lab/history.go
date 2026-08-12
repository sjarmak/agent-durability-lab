package lab

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func readWorkflowCloseHistory(ctx context.Context, temporalClient client.Client,
	workflowID, runID string,
) (*history.HistoryEvent, error) {
	iterator := temporalClient.GetWorkflowHistory(
		ctx, workflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	var last *history.HistoryEvent
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, fmt.Errorf("read Temporal close history: %w", err)
		}
		last = event
	}
	if isCodexWorkflowCloseEvent(last) {
		return last, nil
	}
	return nil, nil
}

func isCodexWorkflowCloseEvent(event *history.HistoryEvent) bool {
	if event == nil {
		return false
	}
	switch event.GetEventType() {
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return true
	default:
		return false
	}
}

func isCodexWorkflowClosedStatus(status enums.WorkflowExecutionStatus) bool {
	switch status {
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enums.WORKFLOW_EXECUTION_STATUS_FAILED,
		enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enums.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enums.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return true
	default:
		return false
	}
}

func decodeCompletedCodexWorkflowResult(event *history.HistoryEvent) (CodexActivityResult, error) {
	if event.GetEventType() != enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
		return CodexActivityResult{}, fmt.Errorf("workflow closed with %s, want completed", event.GetEventType())
	}
	attributes := event.GetWorkflowExecutionCompletedEventAttributes()
	if attributes == nil || attributes.Result == nil {
		return CodexActivityResult{}, errors.New("completed Temporal Workflow history is missing result payloads")
	}
	var result CodexActivityResult
	if err := converter.GetDefaultDataConverter().FromPayloads(attributes.Result, &result); err != nil {
		return CodexActivityResult{}, fmt.Errorf("decode completed Temporal Workflow result: %w", err)
	}
	return result, nil
}

func validateCanceledCodexWorkflowClose(event *history.HistoryEvent) error {
	if event.GetEventType() != enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
		return fmt.Errorf("workflow closed with %s, want canceled", event.GetEventType())
	}
	return nil
}

func exportWorkflowHistory(ctx context.Context, temporalClient client.Client,
	workflowID, runID string,
) ([]byte, error) {
	iterator := temporalClient.GetWorkflowHistory(
		ctx, workflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	value := &history.History{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, fmt.Errorf("read Temporal history: %w", err)
		}
		value.Events = append(value.Events, event)
	}
	encoded, err := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode Temporal history: %w", err)
	}
	return append(encoded, '\n'), nil
}

func replayWorkflowHistory(encoded []byte) error {
	value := &history.History{}
	if err := protojson.Unmarshal(encoded, value); err != nil {
		return fmt.Errorf("decode Temporal history for replay: %w", err)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(CodexWorkflow, workflow.RegisterOptions{Name: CodexWorkflowName})
	if err := replayer.ReplayWorkflowHistory(nil, value); err != nil {
		return fmt.Errorf("replay Codex Workflow history: %w", err)
	}
	return nil
}

func validatePreservedCodexHistory(value *history.History, summary trialSummary) error {
	wantInput := CodexActivityInput{
		LogicalSessionID: summary.LogicalSessionID, LogicalTurnID: summary.LogicalTurnID,
		LogicalEffectID: summary.LogicalEffectID, RecoveryMode: summary.Mode,
	}
	var workflowStarts, activitySchedules, activityStarts, completed, canceled int
	var scheduledEventID int64
	var lastRecordedAttempt int32
	for _, event := range value.GetEvents() {
		if attributes := event.GetWorkflowExecutionStartedEventAttributes(); attributes != nil {
			workflowStarts++
			if attributes.GetWorkflowId() != summary.WorkflowID ||
				attributes.GetOriginalExecutionRunId() != summary.WorkflowRunID ||
				attributes.GetWorkflowType().GetName() != CodexWorkflowName {
				return errors.New("temporal history start identity differs from the trial summary")
			}
			var input CodexActivityInput
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &input); err != nil {
				return fmt.Errorf("decode Temporal Workflow input: %w", err)
			}
			if input != wantInput {
				return errors.New("temporal history Workflow input differs from the admitted logical identity")
			}
		}
		if attributes := event.GetActivityTaskScheduledEventAttributes(); attributes != nil {
			activitySchedules++
			scheduledEventID = event.GetEventId()
			if attributes.GetActivityId() != codexActivityID ||
				attributes.GetActivityType().GetName() != RunCodexActivityName {
				return errors.New("temporal history scheduled the wrong Activity identity")
			}
			var input CodexActivityInput
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &input); err != nil {
				return fmt.Errorf("decode Temporal Activity input: %w", err)
			}
			if input != wantInput {
				return errors.New("temporal history Activity input differs from the admitted logical identity")
			}
		}
		if attributes := event.GetActivityTaskStartedEventAttributes(); attributes != nil {
			activityStarts++
			attempt := attributes.GetAttempt()
			if attributes.GetScheduledEventId() != scheduledEventID || attempt < 1 || attempt <= lastRecordedAttempt ||
				attempt > 1 && !validCompactedRetryFailure(summary.FaultBoundary, attributes.GetLastFailure()) {
				return errors.New("temporal history Activity attempt identity is invalid")
			}
			lastRecordedAttempt = attempt
		}
		if attributes := event.GetWorkflowExecutionCompletedEventAttributes(); attributes != nil {
			completed++
			var result CodexActivityResult
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetResult(), &result); err != nil {
				return fmt.Errorf("decode Temporal Workflow result: %w", err)
			}
			if result != summary.WorkflowResult {
				return errors.New("temporal history completion differs from the trial summary")
			}
		}
		if event.GetWorkflowExecutionCanceledEventAttributes() != nil {
			canceled++
		}
	}
	if workflowStarts != 1 || activitySchedules != 1 || activityStarts == 0 {
		return errors.New("temporal history lacks one start, one Activity schedule, or unique Activity attempts")
	}
	if summary.FaultBoundary == FaultCancellationWhileExecuting {
		if completed != 0 || canceled != 1 || summary.WorkflowResult != (CodexActivityResult{}) {
			return errors.New("temporal cancellation history differs from the canceled trial summary")
		}
		return nil
	}
	if completed != 1 || canceled != 0 || summary.WorkflowResult.TemporalAttempt < 1 ||
		lastRecordedAttempt != summary.WorkflowResult.TemporalAttempt {
		return errors.New("temporal history lacks the accepted Workflow result or Activity attempt")
	}
	return nil
}

func validCompactedRetryFailure(boundary FaultBoundary, failure *failurepb.Failure) bool {
	if failure == nil {
		return false
	}
	if boundary == FaultProcessFailureReplacement {
		return failure.GetApplicationFailureInfo() != nil && failure.GetMessage() == "supervisor request failed"
	}
	return failure.GetTimeoutFailureInfo().GetTimeoutType() == enums.TIMEOUT_TYPE_HEARTBEAT
}
