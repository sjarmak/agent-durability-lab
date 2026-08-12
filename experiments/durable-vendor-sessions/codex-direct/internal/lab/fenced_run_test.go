package lab

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestFencedRunValidatesLogicalStartAndClassifiesLostAuthority(t *testing.T) {
	root := t.TempDir()
	config := fencedCodexRunConfig{
		Command: testActivities(root).Command, LauncherBinary: "/opt/launcher", FaultBoundary: FaultNone,
		EffectBinary: "/opt/effect", EffectPayload: "controlled-edit",
		WorkspacePath: filepath.Join(root, "effects.jsonl"), AuthorityStorePath: filepath.Join(root, "authority.db"),
		BarrierURL:   "http://127.0.0.1:8080",
		BarrierPoint: committedEffectBarrier, RunRoot: filepath.Join(root, "attempts"),
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		SupervisorURL: func() string { return "http://127.0.0.1:8081" },
	}
	if err := config.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := config.validateStart(supervisorStartRequest{
		SessionID: "session-1", LogicalTurnID: "wrong", LogicalEffectID: "effect-1",
	}); err == nil {
		t.Fatal("mismatched logical start unexpectedly succeeded")
	}
	store := openCodexSupervisorStore(t)
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := store.CancelSession(context.Background(), workstore.CancelRequest{SessionID: "session-1", RequestID: "cancel-1"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	runErr := errors.New("process ended")
	if err := config.authorityError(canceled, store, decision.Lease, runErr); !errors.Is(err, workstore.ErrSessionCanceled) || !errors.Is(err, runErr) {
		t.Fatalf("authority error = %v", err)
	}
}

func TestFencedRunClassifiesCurrentMissingAndStaleAuthority(t *testing.T) {
	config := fencedCodexRunConfig{}
	runErr := errors.New("process ended")
	if err := config.authorityError(context.Background(), nil, workstore.Lease{}, runErr); !errors.Is(err, runErr) {
		t.Fatalf("current authority error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store := openCodexSupervisorStore(t)
	if err := config.authorityError(canceled, store, workstore.Lease{SessionID: "missing"}, runErr); !errors.Is(err, workstore.ErrSessionNotFound) || !errors.Is(err, runErr) {
		t.Fatalf("missing authority error = %v", err)
	}
	first, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-stale", Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-stale", Mode: workstore.ModeFenced, CandidateOwner: "owner-2",
		WorkerID: "worker-2", Attempt: 2, ReplaceOwner: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := config.authorityError(canceled, store, first.Lease, runErr); !errors.Is(err, workstore.ErrStaleOwner) || !errors.Is(err, runErr) {
		t.Fatalf("stale authority error = %v", err)
	}
}
