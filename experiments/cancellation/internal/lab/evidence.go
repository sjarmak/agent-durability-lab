package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"

	"github.com/sjarmak/temporal_projects/internal/workstore"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
)

func preserveEvidence(
	ctx context.Context,
	runDirectory string,
	store *workstore.Store,
	sessionID string,
	snapshot workstore.Snapshot,
	boundary BoundaryEvidence,
	verdict Verdict,
	manifest Manifest,
	history *historypb.History,
) error {
	if err := store.ExportJSONL(ctx, sessionID, filepath.Join(runDirectory, "events.jsonl")); err != nil {
		return err
	}
	for name, value := range map[string]any{
		"application-state.json": snapshot,
		"boundary-state.json":    boundary,
		"verdict.json":           verdict,
		"manifest.json":          manifest,
	} {
		if err := writeJSON(filepath.Join(runDirectory, name), value); err != nil {
			return err
		}
	}
	return writeHistory(filepath.Join(runDirectory, "temporal-history.json"), history)
}

func readHistory(ctx context.Context, temporalClient client.Client, workflowID, runID string) (*historypb.History, error) {
	iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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

func writeHistory(path string, history *historypb.History) error {
	data, err := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}.Marshal(history)
	if err != nil {
		return fmt.Errorf("encode Temporal history: %w", err)
	}
	return writeFileAtomically(path, append(data, '\n'))
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	return writeFileAtomically(path, append(data, '\n'))
}

func writeFileAtomically(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".evidence-*")
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	temporary := file.Name()
	published := false
	defer func() {
		if !published {
			_ = os.Remove(temporary)
		}
	}()
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
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	published = true
	return nil
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

func temporalServerVersion(ctx context.Context, temporalClient client.Client) string {
	response, err := temporalClient.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err != nil {
		return "unknown: " + err.Error()
	}
	return response.GetServerVersion()
}
