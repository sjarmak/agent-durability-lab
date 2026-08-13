package lab

import (
	"fmt"
	"reflect"
)

func Verify(evidence Evidence) Verdict {
	failures := validateEvidenceStructure(evidence)
	expectedFailures := validateExpectedState(evidence)
	invariantSatisfied := invariantSatisfied(evidence)
	return Verdict{
		RunValid:            len(failures) == 0,
		ExpectedObservation: len(expectedFailures) == 0,
		InvariantSatisfied:  invariantSatisfied,
		Failures:            append(failures, expectedFailures...),
	}
}

func validateEvidenceStructure(evidence Evidence) []string {
	var failures []string
	if !evidence.Mode.valid() || !evidence.Boundary.Valid() {
		failures = append(failures, "mode or failure boundary is invalid")
	}
	if evidence.Barrier.Point != string(evidence.Boundary) || evidence.Barrier.SessionID == "" ||
		evidence.Barrier.Generation != 1 || evidence.Barrier.ActorID != "worker-1" ||
		evidence.Barrier.PID <= 0 || evidence.Barrier.Time.IsZero() {
		failures = append(failures, "authenticated barrier identity is invalid")
	}
	if evidence.Kill.WorkerID != "worker-1" || evidence.Kill.PID != evidence.Barrier.PID ||
		evidence.Kill.Signal != "SIGKILL" || !evidence.Kill.KilledAt.After(evidence.Barrier.Time) {
		failures = append(failures, "barrier and Worker SIGKILL ordering is invalid")
	}
	if !evidence.History.WorkflowCompleted {
		failures = append(failures, "Workflow did not complete")
	}
	if evidence.History.ArtifactBytesInline {
		failures = append(failures, "artifact bytes entered Temporal history")
	}
	for _, attempt := range evidence.History.Attempts {
		if attempt.Attempt == 2 && attempt.PreviousFailure != "StartToClose" {
			failures = append(failures, fmt.Sprintf("retry prior failure = %q, want StartToClose", attempt.PreviousFailure))
		}
		if attempt.Attempt == 1 && attempt.PreviousFailure != "Unspecified" && attempt.PreviousFailure != "" {
			failures = append(failures, fmt.Sprintf("first Activity prior failure = %q", attempt.PreviousFailure))
		}
	}
	if evidence.Boundary != BoundaryExternalStorageStored &&
		!evidence.History.ProducerCompletedBeforeConsumerStarted {
		failures = append(failures, "producer completion was not persisted before consumer start")
	}
	return failures
}

func validateExpectedState(evidence Evidence) []string {
	var failures []string
	if !reflect.DeepEqual(evidence.History.CompletedActivityIDs, expectedCompletedActivities(evidence.Boundary)) {
		failures = append(failures, fmt.Sprintf("completed Activities = %v", evidence.History.CompletedActivityIDs))
	}
	if !reflect.DeepEqual(activityAttemptPairs(evidence.History.Attempts), expectedAttemptPairs(evidence.Boundary)) {
		failures = append(failures, fmt.Sprintf("Activity attempts = %v", activityAttemptPairs(evidence.History.Attempts)))
	}
	if evidence.History.ExternalReferencePayloads != externalReferenceCount(evidence.Boundary) {
		failures = append(failures, "external reference payload count differs")
	}
	if evidence.Boundary == BoundaryExternalStorageStored {
		wantObjects := 1
		if evidence.Mode == ModeUnsafe {
			wantObjects = 2
		}
		if len(evidence.FinalExternalStore.Blobs) != wantObjects {
			failures = append(failures, fmt.Sprintf("external objects = %d, want %d", len(evidence.FinalExternalStore.Blobs), wantObjects))
		}
		return failures
	}
	wantBefore, wantFinal, wantRemovedBlobs, wantRemovedPending := expectedApplicationInventory(evidence.Boundary, evidence.Mode)
	if !sameInventoryCounts(evidence.BeforeReconcile, wantBefore) {
		failures = append(failures, fmt.Sprintf("pre-reconcile inventory = %s, want %s", inventoryCounts(evidence.BeforeReconcile), inventoryCounts(wantBefore)))
	}
	if !sameInventoryCounts(evidence.FinalStore, wantFinal) {
		failures = append(failures, fmt.Sprintf("final inventory = %s, want %s", inventoryCounts(evidence.FinalStore), inventoryCounts(wantFinal)))
	}
	if len(evidence.Reconciliation.RemovedBlobs) != wantRemovedBlobs ||
		len(evidence.Reconciliation.RemovedPendingReferences) != wantRemovedPending {
		failures = append(failures, "reconciliation actions differ from expected orphan state")
	}
	return failures
}

func invariantSatisfied(evidence Evidence) bool {
	if evidence.Boundary == BoundaryExternalStorageStored {
		return len(evidence.FinalExternalStore.Blobs) == 1
	}
	return sameInventoryCounts(evidence.FinalStore, inventoryOf(1, 1, 0, 1))
}

func expectedApplicationInventory(boundary Boundary, mode Mode) (StoreSnapshot, StoreSnapshot, int, int) {
	before := inventoryOf(1, 1, 0, 1)
	final := before
	removedBlobs := 0
	removedPending := 0
	if boundary == BoundaryReferenceCreated {
		before = inventoryOf(1, 1, 1, 1)
		removedPending = 1
	}
	if mode == ModeUnsafe {
		switch boundary {
		case BoundaryBlobPublished:
			before = inventoryOf(2, 1, 0, 1)
			removedBlobs = 1
		case BoundaryReferenceCreated:
			before = inventoryOf(2, 1, 1, 1)
			removedBlobs, removedPending = 1, 1
		case BoundaryReferencePublished:
			before = inventoryOf(2, 2, 0, 1)
			final = before
		case BoundaryAcknowledgementPublished:
			before = inventoryOf(1, 1, 0, 2)
			final = before
		}
	}
	return before, final, removedBlobs, removedPending
}

func inventoryOf(blobs, references, pending, acknowledgements int) StoreSnapshot {
	return StoreSnapshot{
		Blobs:             make([]StoredEntry, blobs),
		References:        make([]StoredEntry, references),
		PendingReferences: make([]StoredEntry, pending),
		Acknowledgements:  make([]StoredEntry, acknowledgements),
	}
}

func expectedCompletedActivities(boundary Boundary) []string {
	if boundary == BoundaryExternalStorageStored {
		return []string{externalPayloadActivityID}
	}
	return []string{produceActivityID, acknowledgeActivityID}
}

func expectedAttemptPairs(boundary Boundary) []string {
	switch boundary {
	case BoundaryBlobPublished, BoundaryReferenceCreated, BoundaryReferencePublished:
		return []string{produceActivityID + ":2", acknowledgeActivityID + ":1"}
	case BoundaryActivityCompleted, BoundaryAcknowledgementPublished:
		return []string{produceActivityID + ":1", acknowledgeActivityID + ":2"}
	case BoundaryExternalStorageStored:
		return []string{externalPayloadActivityID + ":2"}
	default:
		return nil
	}
}

func activityAttemptPairs(attempts []ActivityAttemptObservation) []string {
	pairs := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		pairs = append(pairs, fmt.Sprintf("%s:%d", attempt.ActivityID, attempt.Attempt))
	}
	return pairs
}

func externalReferenceCount(boundary Boundary) int {
	if boundary == BoundaryExternalStorageStored {
		return 1
	}
	return 0
}

func sameInventoryCounts(left, right StoreSnapshot) bool {
	return len(left.Blobs) == len(right.Blobs) && len(left.References) == len(right.References) &&
		len(left.PendingReferences) == len(right.PendingReferences) &&
		len(left.Acknowledgements) == len(right.Acknowledgements)
}

func inventoryCounts(snapshot StoreSnapshot) string {
	return fmt.Sprintf("blobs=%d refs=%d pending=%d acks=%d", len(snapshot.Blobs), len(snapshot.References),
		len(snapshot.PendingReferences), len(snapshot.Acknowledgements))
}
