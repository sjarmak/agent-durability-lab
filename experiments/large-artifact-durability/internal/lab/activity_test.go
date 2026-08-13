package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"go.temporal.io/sdk/testsuite"
)

func TestProduceActivityArrivesAfterDurableBoundary(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "large-output.bin")
	content := []byte("large artifact bytes stay outside Workflow history")
	if err := os.WriteFile(source, content, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	arrivals := make(chan failureinject.Arrival, 1)
	activities := Activities{
		WorkerID: "worker-1",
		Arrive: func(_ context.Context, arrival failureinject.Arrival) error {
			arrivals <- arrival
			return errInjectedCrash
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.Produce)
	_, err := environment.ExecuteActivity(activities.Produce, WorkflowInput{
		StoreRoot:       filepath.Join(root, "store"),
		SourcePath:      source,
		LogicalID:       "artifact-1",
		ConsumerID:      "consumer-1",
		Mode:            ModeProtected,
		FailureBoundary: BoundaryReferencePublished,
	})
	if err == nil || !strings.Contains(err.Error(), errInjectedCrash.Error()) {
		t.Fatalf("Produce error = %v, want injected crash", err)
	}
	arrival := <-arrivals
	if arrival.Point != string(BoundaryReferencePublished) || arrival.SessionID != "artifact-1" ||
		arrival.Generation != 1 || arrival.ActorID != "worker-1" || arrival.PID != os.Getpid() {
		t.Fatalf("arrival = %+v", arrival)
	}
	store, err := NewArtifactStore(filepath.Join(root, "store"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	snapshot := snapshotStore(t, store)
	if len(snapshot.References) != 1 || len(snapshot.PendingReferences) != 0 {
		t.Fatalf("boundary snapshot = %+v", snapshot)
	}
}

func TestActivitiesRejectInvalidIdentitySourceAndBarrierClient(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	valid := WorkflowInput{
		StoreRoot: filepath.Join(root, "store"), SourcePath: filepath.Join(root, "source"),
		LogicalID: "artifact-1", ConsumerID: "consumer-1", Mode: ModeProtected,
		FailureBoundary: BoundaryReferencePublished,
	}
	if err := os.WriteFile(valid.SourcePath, []byte("artifact"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if _, err := (Activities{}).Produce(context.Background(), valid); err == nil {
		t.Fatal("Produce without Worker identity accepted")
	}
	activities := Activities{WorkerID: "worker-1"}
	alias := filepath.Join(root, "source-link")
	if err := os.Symlink(valid.SourcePath, alias); err != nil {
		t.Fatalf("create source symlink: %v", err)
	}
	invalidSource := valid
	invalidSource.SourcePath = alias
	if _, err := activities.Produce(context.Background(), invalidSource); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("symlinked Produce error = %v, want ErrInvalidArtifact", err)
	}
	if err := os.WriteFile(valid.SourcePath, nil, 0o600); err != nil {
		t.Fatalf("empty source: %v", err)
	}
	if _, err := activities.Produce(context.Background(), valid); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("empty Produce error = %v, want ErrInvalidArtifact", err)
	}
	if err := activities.arrive(context.Background(), "artifact-1", BoundaryReferencePublished, 1); err == nil {
		t.Fatal("arrival without authenticated client accepted")
	}
	if _, err := activities.Acknowledge(context.Background(), ConsumeInput{}); err == nil {
		t.Fatal("invalid acknowledgement accepted")
	}
}

func TestActivityCompletedBoundaryPrecedesAcknowledgement(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	reference, err := store.Produce(context.Background(), testArtifactRequest(ModeProtected, 1), nil)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	arrived := false
	activities := Activities{
		WorkerID: "worker-1",
		Arrive: func(_ context.Context, arrival failureinject.Arrival) error {
			arrived = true
			if count := len(snapshotStore(t, store).Acknowledgements); count != 0 {
				t.Fatalf("acknowledgements before activity-completed barrier = %d", count)
			}
			if arrival.Point != string(BoundaryActivityCompleted) {
				t.Fatalf("arrival point = %q", arrival.Point)
			}
			return errInjectedCrash
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.Acknowledge)
	_, err = environment.ExecuteActivity(activities.Acknowledge, ConsumeInput{
		StoreRoot:       store.root,
		Reference:       reference,
		ConsumerID:      "consumer-1",
		Mode:            ModeProtected,
		FailureBoundary: BoundaryActivityCompleted,
	})
	if err == nil || !strings.Contains(err.Error(), errInjectedCrash.Error()) {
		t.Fatalf("Acknowledge error = %v, want injected crash", err)
	}
	if !arrived {
		t.Fatal("activity-completed barrier was not reached")
	}
}

func TestAcknowledgementBoundaryObservesDurableAcknowledgement(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	reference, err := store.Produce(context.Background(), testArtifactRequest(ModeProtected, 1), nil)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	activities := Activities{
		WorkerID: "worker-1",
		Arrive: func(_ context.Context, arrival failureinject.Arrival) error {
			if arrival.Point != string(BoundaryAcknowledgementPublished) {
				t.Fatalf("arrival point = %q", arrival.Point)
			}
			if count := len(snapshotStore(t, store).Acknowledgements); count != 1 {
				t.Fatalf("acknowledgements at barrier = %d, want 1", count)
			}
			return errInjectedCrash
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.Acknowledge)
	_, err = environment.ExecuteActivity(activities.Acknowledge, ConsumeInput{
		StoreRoot:       store.root,
		Reference:       reference,
		ConsumerID:      "consumer-1",
		Mode:            ModeProtected,
		FailureBoundary: BoundaryAcknowledgementPublished,
	})
	if err == nil || !strings.Contains(err.Error(), errInjectedCrash.Error()) {
		t.Fatalf("Acknowledge error = %v, want injected crash", err)
	}
}
