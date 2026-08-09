package lab

import (
	"context"
	"fmt"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
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
