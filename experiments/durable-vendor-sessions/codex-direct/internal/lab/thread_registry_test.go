package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCanonicalThreadRegistrationIsStableAndRejectsCompetingThread(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canonical-thread.json")
	record := CanonicalThread{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1",
		ThreadID:               "019ff302-7730-7f21-90ed-73c37fb4e8fa",
		FirstPhysicalAttemptID: "attempt-1", RegisteredAt: time.Now().UTC(),
	}
	if err := RegisterCanonicalThread(path, record); err != nil {
		t.Fatalf("register canonical thread: %v", err)
	}
	if err := RegisterCanonicalThread(path, record); err != nil {
		t.Fatalf("idempotent registration: %v", err)
	}
	competing := record
	competing.ThreadID = "019ff302-7730-7f21-90ed-73c37fb4e8fb"
	competing.FirstPhysicalAttemptID = "attempt-2"
	if err := RegisterCanonicalThread(path, competing); !errors.Is(err, ErrCanonicalThreadConflict) {
		t.Fatalf("competing registration = %v, want conflict", err)
	}
	got, err := ReadCanonicalThread(path)
	if err != nil {
		t.Fatalf("read canonical thread: %v", err)
	}
	if got != record {
		t.Fatalf("canonical thread = %+v, want %+v", got, record)
	}
}

func TestWaitForCanonicalThreadObservesRegistrationAndCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "threads", "canonical-thread.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	record := CanonicalThread{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1",
		ThreadID:               "019ff302-7730-7f21-90ed-73c37fb4e8fa",
		FirstPhysicalAttemptID: "attempt-1", RegisteredAt: time.Now().UTC(),
	}
	observed := make(chan CanonicalThread, 1)
	waitErr := make(chan error, 1)
	go func() {
		got, err := WaitForCanonicalThread(context.Background(), path)
		observed <- got
		waitErr <- err
	}()
	if err := RegisterCanonicalThread(path, record); err != nil {
		t.Fatalf("register waited-for canonical thread: %v", err)
	}
	if err := <-waitErr; err != nil {
		t.Fatalf("wait for canonical thread: %v", err)
	}
	if got := <-observed; got != record {
		t.Fatalf("waited-for canonical thread = %+v, want %+v", got, record)
	}

	canceledPath := filepath.Join(t.TempDir(), "missing.json")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WaitForCanonicalThread(ctx, canceledPath); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait = %v, want context.Canceled", err)
	}
}
