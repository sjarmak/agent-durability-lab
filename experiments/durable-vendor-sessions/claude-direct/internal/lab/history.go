package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func exportWorkflowHistory(
	ctx context.Context,
	temporalClient client.Client,
	workflowID string,
	runID string,
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
	return replayDecodedWorkflowHistory(value)
}

func replayDecodedWorkflowHistory(value *history.History) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		DirectClaudeWorkflow,
		workflow.RegisterOptions{Name: DirectClaudeWorkflowName},
	)
	if err := replayer.ReplayWorkflowHistory(nil, value); err != nil {
		return fmt.Errorf("replay direct Claude Workflow history: %w", err)
	}
	return nil
}

func validatePreservedHistory(value *history.History, manifestRunID string, summary trialSummary) error {
	var started, completed int
	attempts := make(map[int32]bool)
	for _, event := range value.Events {
		if attributes := event.GetWorkflowExecutionStartedEventAttributes(); attributes != nil {
			started++
			payloads := attributes.GetInput().GetPayloads()
			if attributes.GetWorkflowId() != summary.WorkflowID ||
				attributes.GetOriginalExecutionRunId() != summary.WorkflowRunID ||
				attributes.GetWorkflowType().GetName() != DirectClaudeWorkflowName || len(payloads) != 1 {
				return errors.New("temporal history start identity differs from the trial summary")
			}
			var input ClaudeActivityInput
			if err := json.Unmarshal(payloads[0].GetData(), &input); err != nil {
				return fmt.Errorf("decode Temporal history Workflow input: %w", err)
			}
			if input.LogicalSessionID != manifestRunID || input.LogicalTurnID != "turn-1" ||
				input.LogicalEffectID != "effect-1" || input.RecoveryMode.normalized() != summary.RecoveryMode.normalized() ||
				input.SelectedVendorSessionID != summary.SelectedVendorSessionID {
				return errors.New("temporal history Workflow input differs from the admitted logical identity")
			}
		}
		if attributes := event.GetActivityTaskStartedEventAttributes(); attributes != nil {
			attempts[attributes.GetAttempt()] = true
		}
		if attributes := event.GetWorkflowExecutionCompletedEventAttributes(); attributes != nil {
			completed++
			payloads := attributes.GetResult().GetPayloads()
			if len(payloads) != 1 {
				return errors.New("temporal history completion must contain one result payload")
			}
			var result ClaudeActivityResult
			if err := json.Unmarshal(payloads[0].GetData(), &result); err != nil {
				return fmt.Errorf("decode Temporal history Workflow result: %w", err)
			}
			if !reflect.DeepEqual(result, summary.WorkflowResult) {
				return errors.New("temporal history completion differs from the trial summary")
			}
		}
	}
	if started != 1 || completed != 1 || summary.WorkflowResult.TemporalAttempt < 1 {
		return errors.New("temporal history lacks one start and one completion")
	}
	if !attempts[summary.WorkflowResult.TemporalAttempt] {
		return fmt.Errorf("temporal history lacks accepted Activity attempt %d", summary.WorkflowResult.TemporalAttempt)
	}
	for attempt := range attempts {
		if attempt < 1 || attempt > summary.WorkflowResult.TemporalAttempt {
			return errors.New("temporal history contains an unexpected Activity attempt ordinal")
		}
	}
	return nil
}
