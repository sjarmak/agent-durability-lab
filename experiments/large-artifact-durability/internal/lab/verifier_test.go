package lab

import (
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestVerifyDistinguishesProtectedAndUnsafeBoundaryOutcomes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		boundary      Boundary
		mode          Mode
		before        StoreSnapshot
		final         StoreSnapshot
		external      StoreSnapshot
		reconcile     ReconcileReport
		wantInvariant bool
	}{
		{
			name: "protected reference publication", boundary: BoundaryReferencePublished, mode: ModeProtected,
			before: inventory(1, 1, 0, 1), final: inventory(1, 1, 0, 1), wantInvariant: true,
		},
		{
			name: "unsafe reference publication", boundary: BoundaryReferencePublished, mode: ModeUnsafe,
			before: inventory(2, 2, 0, 1), final: inventory(2, 2, 0, 1), wantInvariant: false,
		},
		{
			name: "protected acknowledgement", boundary: BoundaryAcknowledgementPublished, mode: ModeProtected,
			before: inventory(1, 1, 0, 1), final: inventory(1, 1, 0, 1), wantInvariant: true,
		},
		{
			name: "unsafe acknowledgement", boundary: BoundaryAcknowledgementPublished, mode: ModeUnsafe,
			before: inventory(1, 1, 0, 2), final: inventory(1, 1, 0, 2), wantInvariant: false,
		},
		{
			name: "protected external storage", boundary: BoundaryExternalStorageStored, mode: ModeProtected,
			external: inventory(1, 0, 0, 0), wantInvariant: true,
		},
		{
			name: "unsafe external storage", boundary: BoundaryExternalStorageStored, mode: ModeUnsafe,
			external: inventory(2, 0, 0, 0), wantInvariant: false,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			evidence := validEvidence(test.boundary, test.mode)
			evidence.BeforeReconcile = test.before
			evidence.FinalStore = test.final
			evidence.FinalExternalStore = test.external
			evidence.Reconciliation = test.reconcile
			verdict := Verify(evidence)
			if !verdict.RunValid || !verdict.ExpectedObservation || verdict.InvariantSatisfied != test.wantInvariant {
				t.Fatalf("verdict = %+v", verdict)
			}
		})
	}
}

func TestVerifyRejectsArtifactBytesInHistoryAndWrongOrdering(t *testing.T) {
	t.Parallel()

	evidence := validEvidence(BoundaryReferencePublished, ModeProtected)
	evidence.BeforeReconcile = inventory(1, 1, 0, 1)
	evidence.FinalStore = inventory(1, 1, 0, 1)
	evidence.History.ArtifactBytesInline = true
	evidence.History.ProducerCompletedBeforeConsumerStarted = false
	verdict := Verify(evidence)
	if verdict.RunValid || len(verdict.Failures) < 2 {
		t.Fatalf("verdict = %+v", verdict)
	}
}

func TestVerifyRejectsEachEvidenceIdentityAndStateMutation(t *testing.T) {
	t.Parallel()

	base := validEvidence(BoundaryReferenceCreated, ModeProtected)
	base.BeforeReconcile = inventory(1, 1, 1, 1)
	base.FinalStore = inventory(1, 1, 0, 1)
	base.Reconciliation.RemovedPendingReferences = []string{"pending.json"}
	if verdict := Verify(base); !verdict.RunValid || !verdict.ExpectedObservation {
		t.Fatalf("baseline verdict = %+v", verdict)
	}
	for name, mutate := range map[string]func(*Evidence){
		"mode":             func(value *Evidence) { value.Mode = "invalid" },
		"barrier point":    func(value *Evidence) { value.Barrier.Point = "wrong" },
		"barrier actor":    func(value *Evidence) { value.Barrier.ActorID = "worker-2" },
		"kill pid":         func(value *Evidence) { value.Kill.PID++ },
		"kill order":       func(value *Evidence) { value.Kill.KilledAt = value.Barrier.Time },
		"workflow":         func(value *Evidence) { value.History.WorkflowCompleted = false },
		"retry failure":    func(value *Evidence) { value.History.Attempts[0].PreviousFailure = "ApplicationFailure" },
		"first failure":    func(value *Evidence) { value.History.Attempts[1].PreviousFailure = "Timeout" },
		"completed":        func(value *Evidence) { value.History.CompletedActivityIDs = nil },
		"attempts":         func(value *Evidence) { value.History.Attempts[0].Attempt = 3 },
		"external refs":    func(value *Evidence) { value.History.ExternalReferencePayloads = 1 },
		"before inventory": func(value *Evidence) { value.BeforeReconcile.Blobs = nil },
		"final inventory":  func(value *Evidence) { value.FinalStore.Acknowledgements = nil },
		"reconciliation":   func(value *Evidence) { value.Reconciliation.RemovedPendingReferences = nil },
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := base
			changed.History.Attempts = append([]ActivityAttemptObservation(nil), base.History.Attempts...)
			changed.History.CompletedActivityIDs = append([]string(nil), base.History.CompletedActivityIDs...)
			mutate(&changed)
			verdict := Verify(changed)
			if verdict.RunValid && verdict.ExpectedObservation {
				t.Fatalf("mutation accepted: %+v", verdict)
			}
		})
	}
}

func validEvidence(boundary Boundary, mode Mode) Evidence {
	observed := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	started := []ActivityAttemptObservation{
		{ActivityID: produceActivityID, Attempt: 2, PreviousFailure: "StartToClose"},
		{ActivityID: acknowledgeActivityID, Attempt: 1},
	}
	if boundary == BoundaryActivityCompleted || boundary == BoundaryAcknowledgementPublished {
		started = []ActivityAttemptObservation{
			{ActivityID: produceActivityID, Attempt: 1},
			{ActivityID: acknowledgeActivityID, Attempt: 2, PreviousFailure: "StartToClose"},
		}
	}
	if boundary == BoundaryExternalStorageStored {
		started = []ActivityAttemptObservation{
			{ActivityID: externalPayloadActivityID, Attempt: 2, PreviousFailure: "StartToClose"},
		}
	}
	return Evidence{
		Boundary: boundary,
		Mode:     mode,
		Barrier: failureinject.Arrival{
			ID:    "artifact-1/" + string(boundary) + "/attempt-1",
			Point: string(boundary), SessionID: "artifact-1", Generation: 1,
			ActorID: "worker-1", PID: 123, Time: observed,
		},
		Kill: KillObservation{WorkerID: "worker-1", PID: 123, Signal: "SIGKILL", KilledAt: observed.Add(time.Millisecond)},
		History: HistoryObservation{
			Attempts:                               started,
			CompletedActivityIDs:                   expectedCompletedActivities(boundary),
			WorkflowCompleted:                      true,
			ProducerCompletedBeforeConsumerStarted: boundary != BoundaryExternalStorageStored,
			ExternalReferencePayloads:              externalReferenceCount(boundary),
		},
	}
}

func inventory(blobs, references, pending, acknowledgements int) StoreSnapshot {
	return StoreSnapshot{
		Blobs:             make([]StoredEntry, blobs),
		References:        make([]StoredEntry, references),
		PendingReferences: make([]StoredEntry, pending),
		Acknowledgements:  make([]StoredEntry, acknowledgements),
	}
}
