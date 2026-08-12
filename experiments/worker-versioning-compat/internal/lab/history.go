package lab

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
)

type scheduledActivity struct {
	ID    string
	Input ActivityInput
}

type startedActivity struct {
	EventID     int64
	WorkerBuild string
	Attempt     int32
}

func inspectActivityHistory(historyValue *history.History, scenario Scenario, workflowID, runID, registryPath string, expectedFailure bool) ([]ActivityReceipt, error) {
	scheduled := make(map[int64]scheduledActivity)
	started := make(map[int64]startedActivity)
	completed := make(map[int64]ActivityReceipt)
	failed := make(map[int64]string)
	dataConverter := converter.GetDefaultDataConverter()
	for _, event := range historyValue.Events {
		if attributes := event.GetActivityTaskScheduledEventAttributes(); attributes != nil {
			if attributes.GetActivityType().GetName() != ActivityName {
				return nil, fmt.Errorf("unexpected Activity type %q", attributes.GetActivityType().GetName())
			}
			var input ActivityInput
			if err := dataConverter.FromPayloads(attributes.GetInput(), &input); err != nil {
				return nil, fmt.Errorf("decode Activity input: %w", err)
			}
			if attributes.GetActivityId() != fmt.Sprintf("attach-agent-phase-%d", input.Phase) || input.SessionID != "session-1" || input.Phase < 1 || input.Phase > 2 || input.RegistryPath != registryPath {
				return nil, fmt.Errorf("invalid Activity schedule %q input %+v", attributes.GetActivityId(), input)
			}
			scheduled[event.GetEventId()] = scheduledActivity{ID: attributes.GetActivityId(), Input: input}
		}
		if attributes := event.GetActivityTaskStartedEventAttributes(); attributes != nil {
			started[attributes.GetScheduledEventId()] = startedActivity{EventID: event.GetEventId(), WorkerBuild: attributes.GetIdentity(), Attempt: attributes.GetAttempt()}
		}
		if attributes := event.GetActivityTaskCompletedEventAttributes(); attributes != nil {
			var receipt ActivityReceipt
			if err := dataConverter.FromPayloads(attributes.GetResult(), &receipt); err != nil {
				return nil, fmt.Errorf("decode Activity result: %w", err)
			}
			if start, ok := started[attributes.GetScheduledEventId()]; !ok || attributes.GetStartedEventId() != start.EventID {
				return nil, errors.New("activity completion does not target its exact started event")
			}
			completed[attributes.GetScheduledEventId()] = receipt
		}
		if attributes := event.GetActivityTaskFailedEventAttributes(); attributes != nil {
			failureType := ""
			if attributes.GetFailure() != nil && attributes.GetFailure().GetApplicationFailureInfo() != nil {
				failureType = attributes.GetFailure().GetApplicationFailureInfo().GetType()
			}
			if start, ok := started[attributes.GetScheduledEventId()]; !ok || attributes.GetStartedEventId() != start.EventID {
				return nil, errors.New("activity failure does not target its exact started event")
			}
			failed[attributes.GetScheduledEventId()] = failureType
		}
	}
	if len(scheduled) != 2 || len(started) != 2 {
		return nil, fmt.Errorf("activity population scheduled=%d started=%d, want 2/2", len(scheduled), len(started))
	}
	if expectedFailure {
		if len(completed) != 1 || len(failed) != 1 {
			return nil, fmt.Errorf("incompatible activity terminals completed=%d failed=%d, want 1/1", len(completed), len(failed))
		}
	} else if len(completed) != 2 || len(failed) != 0 {
		return nil, fmt.Errorf("compatible activity terminals completed=%d failed=%d, want 2/0", len(completed), len(failed))
	}

	ids := make([]int64, 0, len(completed))
	for scheduledID := range completed {
		ids = append(ids, scheduledID)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	receipts := make([]ActivityReceipt, 0, len(ids))
	for _, scheduledID := range ids {
		schedule, scheduleOK := scheduled[scheduledID]
		start, startOK := started[scheduledID]
		receipt := completed[scheduledID]
		if !scheduleOK || !startOK || receipt.SessionID != schedule.Input.SessionID || receipt.Phase != schedule.Input.Phase || receipt.WorkflowID != workflowID || receipt.RunID != runID || receipt.WorkerBuild != start.WorkerBuild || receipt.TemporalAttempt != start.Attempt || start.WorkerBuild != expectedActivityWorker(scenario, schedule.Input.Phase) {
			return nil, fmt.Errorf("activity receipt is not bound to schedule/start/execution: %+v", receipt)
		}
		if receipt.AgentBuild != "agent-v1" || (receipt.Action != ActionStarted && receipt.Action != ActionAttached) {
			return nil, fmt.Errorf("activity receipt has invalid agent identity/action: %+v", receipt)
		}
		receipts = append(receipts, receipt)
	}
	for scheduledID, failureType := range failed {
		schedule, scheduleOK := scheduled[scheduledID]
		start, startOK := started[scheduledID]
		if !scheduleOK || !startOK || schedule.Input.Phase != 2 || start.WorkerBuild != "worker-v3" || start.Attempt != 1 || failureType != "incompatible-agent-build" {
			return nil, fmt.Errorf("incompatible failure is not bound to phase-two worker-v3: schedule=%+v start=%+v type=%q", schedule, start, failureType)
		}
	}
	if len(receipts) > 0 && receipts[0].Action != ActionStarted {
		return nil, errors.New("phase-one activity did not start the detached agent")
	}
	if len(receipts) == 2 && receipts[1].Action != ActionAttached {
		return nil, errors.New("phase-two compatible activity did not attach")
	}
	return receipts, nil
}

func inspectWorkflowStart(historyValue *history.History, scenario Scenario, trial int, runLabel, workflowID, runID string) (string, error) {
	if len(historyValue.Events) == 0 {
		return "", errors.New("history is empty")
	}
	attributes := historyValue.Events[0].GetWorkflowExecutionStartedEventAttributes()
	if attributes == nil || attributes.GetOriginalExecutionRunId() != runID || attributes.GetFirstExecutionRunId() != runID || attributes.GetWorkflowId() != workflowID {
		return "", errors.New("history start does not bind the stored run ID")
	}
	expectedWorkflowID := fmt.Sprintf("%s-%s-trial-%d", runLabel, scenario, trial)
	if workflowID != expectedWorkflowID {
		return "", fmt.Errorf("workflow ID = %q, want %q", workflowID, expectedWorkflowID)
	}
	var input WorkflowInput
	if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &input); err != nil {
		return "", fmt.Errorf("decode workflow input: %w", err)
	}
	if input.SessionID != "session-1" || input.Phases != 2 || unsafeRegistryPath(input.RegistryPath) || filepath.Base(input.RegistryPath) != evidencePrefix(scenario, trial)+"-registry.db" {
		return "", fmt.Errorf("invalid workflow input: %+v", input)
	}
	return input.RegistryPath, nil
}

func unsafeRegistryPath(path string) bool {
	clean := filepath.ToSlash(filepath.Clean(path))
	return path == "" || filepath.IsAbs(path) || strings.Contains(path, `\`) || clean == ".." || strings.HasPrefix(clean, "../")
}

func inspectWorkflowTerminal(historyValue *history.History, expectedFailure bool) (WorkflowResult, error) {
	var result WorkflowResult
	completed, failed := 0, 0
	for _, event := range historyValue.Events {
		if attributes := event.GetWorkflowExecutionCompletedEventAttributes(); attributes != nil {
			completed++
			if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetResult(), &result); err != nil {
				return WorkflowResult{}, fmt.Errorf("decode workflow result: %w", err)
			}
		}
		if event.GetWorkflowExecutionFailedEventAttributes() != nil {
			failed++
		}
	}
	if expectedFailure {
		if completed != 0 || failed != 1 {
			return WorkflowResult{}, fmt.Errorf("incompatible workflow terminals completed=%d failed=%d, want 0/1", completed, failed)
		}
		return WorkflowResult{}, nil
	}
	if completed != 1 || failed != 0 {
		return WorkflowResult{}, fmt.Errorf("compatible workflow terminals completed=%d failed=%d, want 1/0", completed, failed)
	}
	return result, nil
}

func expectedActivityWorker(scenario Scenario, phase int) string {
	if phase == 1 || scenario == ScenarioPinnedCompatible {
		return "worker-v1"
	}
	if scenario == ScenarioAutoCompatible {
		return "worker-v2"
	}
	return "worker-v3"
}
