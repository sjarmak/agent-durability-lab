package lab

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestValidateOptionsRejectsUnsafeRunIDs(t *testing.T) {
	t.Parallel()
	valid := Options{
		Scenario: ScenarioHealthySafe, TemporalPath: "temporal", WorkerBinary: "worker",
		AgentBinary: "agent", OutputRoot: "evidence", RunID: "run-01_safe.v1",
	}
	if err := validateOptions(valid); err != nil {
		t.Fatalf("valid options: %v", err)
	}

	for _, runID := range []string{"", ".", "..", "../outside", "a/b", `a\b`, "has space", strings.Repeat("a", 129)} {
		runID := runID
		t.Run(runID, func(t *testing.T) {
			options := valid
			options.RunID = runID
			if err := validateOptions(options); err == nil {
				t.Fatalf("validateOptions accepted unsafe run ID %q", runID)
			}
		})
	}
}

func TestControlTargetRequiresCompleteExactProcessEvidence(t *testing.T) {
	valid := workstore.Snapshot{
		SessionID: "session-1",
		Executors: []workstore.Executor{{
			Generation: 1, OwnerTokenHash: "owner-hash", PID: 101,
			ProcessStart: "boot:101", ProcessGroupID: 101,
		}},
		Events: []workstore.Event{{
			Sequence: 2, Kind: "tool_child_registered", Generation: 1,
			OwnerTokenHash: "owner-hash", PID: 102,
			Details: map[string]string{"process_start": "boot:102", "process_group_id": "101"},
		}},
	}
	target, err := controlTarget(valid)
	if err != nil {
		t.Fatalf("valid target: %v", err)
	}
	if len(target.Members) != 2 || target.Leader.PID != 101 || target.Members[1].PID != 102 {
		t.Fatalf("target = %+v; want exact leader and child", target)
	}

	invalid := []workstore.Snapshot{
		{},
		{Executors: []workstore.Executor{{}, {}}},
		{Executors: []workstore.Executor{{Generation: 1, OwnerTokenHash: "hash"}}},
	}
	badChild := valid
	badChild.Events = []workstore.Event{{
		Sequence: 2, Kind: "tool_child_registered", Generation: 1,
		OwnerTokenHash: "owner-hash", PID: 102,
		Details: map[string]string{"process_start": "boot:102", "process_group_id": "not-a-number"},
	}}
	invalid = append(invalid, badChild)
	for index, snapshot := range invalid {
		if _, err := controlTarget(snapshot); err == nil {
			t.Errorf("invalid snapshot %d was accepted", index)
		}
	}
}

func TestWaitForSnapshotAndProcessTreeHonorCancellation(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waitForSnapshot(ctx, store, "missing", func(workstore.Snapshot) bool { return false }); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForSnapshot error = %v; want context cancellation", err)
	}

	deadline, stop := context.WithTimeout(context.Background(), time.Nanosecond)
	defer stop()
	target := agentprocess.ControlTarget{Members: []agentprocess.ProcessIdentity{{
		PID: 1, StartIdentity: "definitely-not-pid-one", ProcessGroupID: 1,
	}}}
	if err := waitForProcessTreeGone(deadline, target); err == nil {
		t.Fatal("waitForProcessTreeGone accepted a live or mismatched identity")
	}
}

func TestFailureBoundaryAndTemporalVersionFallbacks(t *testing.T) {
	if got := failureBoundary("unknown"); got != "unknown" {
		t.Fatalf("unknown failure boundary = %q", got)
	}
	if got := temporalCLIVersion(context.Background(), filepath.Join(t.TempDir(), "missing-temporal")); !strings.HasPrefix(got, "unknown: ") {
		t.Fatalf("missing CLI version = %q", got)
	}
}
