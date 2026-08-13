package lab

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const externalReferenceMessageType = "temporal.api.sdk.v1.ExternalStorageReference"

func readHistory(ctx context.Context, temporalClient client.Client, workflowID, runID string) (*historypb.History, error) {
	iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, 0)
	history := &historypb.History{Events: make([]*historypb.HistoryEvent, 0)}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, fmt.Errorf("read Temporal history: %w", err)
		}
		history.Events = append(history.Events, event)
	}
	return history, nil
}

func summarizeHistory(history *historypb.History) HistoryObservation {
	observation := HistoryObservation{}
	scheduled := make(map[int64]string)
	producerCompletedEventID := int64(0)
	consumerStartedEventID := int64(0)
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			attributes := event.GetActivityTaskScheduledEventAttributes()
			scheduled[event.GetEventId()] = attributes.GetActivityId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attributes := event.GetActivityTaskStartedEventAttributes()
			activityID := scheduled[attributes.GetScheduledEventId()]
			observation.Attempts = append(observation.Attempts, ActivityAttemptObservation{
				ActivityID: activityID, Attempt: attributes.GetAttempt(),
				WorkerIdentity: attributes.GetIdentity(), EventID: event.GetEventId(),
				PreviousFailure: attributes.GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType().String(),
			})
			if activityID == acknowledgeActivityID && consumerStartedEventID == 0 {
				consumerStartedEventID = event.GetEventId()
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attributes := event.GetActivityTaskCompletedEventAttributes()
			activityID := scheduled[attributes.GetScheduledEventId()]
			observation.CompletedActivityIDs = append(observation.CompletedActivityIDs, activityID)
			if activityID == produceActivityID {
				producerCompletedEventID = event.GetEventId()
			}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			observation.WorkflowCompleted = true
		}
		for _, payload := range eventPayloads(event) {
			if len(payload.GetData()) > observation.MaximumPayloadDataBytes {
				observation.MaximumPayloadDataBytes = len(payload.GetData())
			}
			if len(payload.GetData()) >= ExternalStorageThreshold {
				observation.ArtifactBytesInline = true
			}
			if string(payload.GetMetadata()[converter.MetadataMessageType]) == externalReferenceMessageType {
				observation.ExternalReferencePayloads++
			}
		}
	}
	observation.ProducerCompletedBeforeConsumerStarted = producerCompletedEventID > 0 &&
		consumerStartedEventID > producerCompletedEventID
	return observation
}

func eventPayloads(event *historypb.HistoryEvent) []*commonpb.Payload {
	var payloadGroups []*commonpb.Payloads
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		payloadGroups = append(payloadGroups, event.GetWorkflowExecutionStartedEventAttributes().GetInput())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		payloadGroups = append(payloadGroups, event.GetActivityTaskScheduledEventAttributes().GetInput())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		payloadGroups = append(payloadGroups, event.GetActivityTaskCompletedEventAttributes().GetResult())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		payloadGroups = append(payloadGroups, event.GetWorkflowExecutionCompletedEventAttributes().GetResult())
	}
	var payloads []*commonpb.Payload
	for _, group := range payloadGroups {
		payloads = append(payloads, group.GetPayloads()...)
	}
	return payloads
}

func writeHistory(path string, history *historypb.History) error {
	data, err := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}.Marshal(history)
	if err != nil {
		return fmt.Errorf("encode Temporal history: %w", err)
	}
	return writeFileAtomically(path, append(data, '\n'))
}

func replayHistory(history *historypb.History, externalRoot string, mode Mode) error {
	if externalRoot == "" {
		return replayHistoryWithExternalStorage(history, converter.ExternalStorage{})
	}
	driver, err := NewFileStorageDriver(externalRoot, mode, nil)
	if err != nil {
		return err
	}
	return replayHistoryWithExternalStorage(history, converter.ExternalStorage{
		Drivers: []converter.StorageDriver{driver}, PayloadSizeThreshold: ExternalStorageThreshold,
	})
}

func replayHistoryWithExternalStorage(history *historypb.History, storage converter.ExternalStorage) error {
	var replayer worker.WorkflowReplayer
	if len(storage.Drivers) == 0 {
		replayer = worker.NewWorkflowReplayer()
	} else {
		configured, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{ExternalStorage: storage})
		if err != nil {
			return fmt.Errorf("configure history replay: %w", err)
		}
		replayer = configured
	}
	replayer.RegisterWorkflowWithOptions(artifactWorkflow, workflow.RegisterOptions{Name: workflowName})
	replayer.RegisterWorkflowWithOptions(externalStorageWorkflow, workflow.RegisterOptions{Name: externalWorkflowName})
	replayCopy := proto.Clone(history).(*historypb.History)
	if err := replayer.ReplayWorkflowHistory(discardReplayLogger{}, replayCopy); err != nil {
		return fmt.Errorf("replay Temporal history: %w", err)
	}
	return nil
}

type discardReplayLogger struct{}

func (discardReplayLogger) Debug(_ string, keyvals ...interface{}) { _ = len(keyvals) }
func (discardReplayLogger) Info(_ string, keyvals ...interface{})  { _ = len(keyvals) }
func (discardReplayLogger) Warn(_ string, keyvals ...interface{})  { _ = len(keyvals) }
func (discardReplayLogger) Error(_ string, keyvals ...interface{}) { _ = len(keyvals) }

var _ log.Logger = discardReplayLogger{}

func writeFileAtomically(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".evidence-*")
	if err != nil {
		return fmt.Errorf("create evidence file: %w", err)
	}
	temporary := file.Name()
	defer func() { _ = os.Remove(temporary) }()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(temporary, path); err != nil {
		return fmt.Errorf("publish append-only evidence file: %w", err)
	}
	if err := os.Remove(temporary); err != nil {
		return fmt.Errorf("remove temporary evidence file: %w", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	return nil
}
