package lab

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestObservationStorePersistsAttemptAcrossProcessReopen(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "observations.db")
	started := AttemptObservation{
		Attempt: 1, WorkerID: "worker-1", PID: 42,
		StartedAt: time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	}
	if err := recordAttemptStart(path, started); err != nil {
		t.Fatalf("record start: %v", err)
	}
	requestedAt := started.StartedAt.Add(time.Second)
	respondedAt := requestedAt.Add(time.Second)
	if err := recordAttemptFinish(path, 1, requestedAt, respondedAt, EffectResult{
		Receipt: "receipt-1", Outcome: OutcomeApplied,
	}, nil); err != nil {
		t.Fatalf("record finish: %v", err)
	}
	attempts, err := readAttempts(path)
	if err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Receipt != "receipt-1" || attempts[0].PID != 42 {
		t.Fatalf("attempts = %+v", attempts)
	}
}

func TestObservationStoreRejectsDuplicateStart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "observations.db")
	observation := AttemptObservation{Attempt: 1, WorkerID: "worker-1", PID: 42, StartedAt: time.Now().UTC()}
	if err := recordAttemptStart(path, observation); err != nil {
		t.Fatalf("record first start: %v", err)
	}
	if err := recordAttemptStart(path, observation); err == nil {
		t.Fatal("duplicate start succeeded")
	}
}

func TestObservationStoreRejectsIncompleteAndDuplicateFinishes(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "observations.db")
	if err := recordAttemptStart(path, AttemptObservation{}); err == nil {
		t.Fatal("incomplete attempt start succeeded")
	}
	if err := recordAttemptFinish(path, 1, time.Now(), time.Now(), EffectResult{}, nil); err == nil {
		t.Fatal("finish without attempt store succeeded")
	}
	started := AttemptObservation{Attempt: 1, WorkerID: "worker-1", PID: 42, StartedAt: time.Now().UTC()}
	if err := recordAttemptStart(path, started); err != nil {
		t.Fatalf("record start: %v", err)
	}
	if err := recordAttemptFinish(path, 2, time.Now(), time.Now(), EffectResult{}, nil); err == nil {
		t.Fatal("finish without matching start succeeded")
	}
	wantErr := errors.New("destination unavailable")
	if err := recordAttemptFinish(path, 1, time.Now(), time.Now(), EffectResult{}, wantErr); err != nil {
		t.Fatalf("record failed finish: %v", err)
	}
	if err := recordAttemptFinish(path, 1, time.Now(), time.Now(), EffectResult{}, nil); err == nil {
		t.Fatal("duplicate finish succeeded")
	}
	attempts, err := readAttempts(path)
	if err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Error != wantErr.Error() {
		t.Fatalf("attempts = %+v, want recorded error", attempts)
	}
}

func TestObservationStoreReportsMissingSnapshot(t *testing.T) {
	t.Parallel()
	if _, err := readAttempts(filepath.Join(t.TempDir(), "missing.db")); err == nil {
		t.Fatal("missing observation snapshot succeeded")
	}
}
