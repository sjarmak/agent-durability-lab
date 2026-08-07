package lab

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRejectsInvalidOptionsBeforeStartingTemporal(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "temporal")
	if err := os.WriteFile(executable, []byte("test"), 0o700); err != nil {
		t.Fatalf("write executable fixture: %v", err)
	}
	tests := []struct {
		name    string
		options Options
		want    string
	}{
		{name: "missing fields", options: Options{}, want: "valid arm"},
		{
			name: "unsafe run ID",
			options: Options{
				Arm: ArmStaleByID, TemporalPath: executable, OutputRoot: t.TempDir(), RunID: "../escape",
			},
			want: "run ID",
		},
		{
			name: "missing Temporal binary",
			options: Options{
				Arm: ArmStaleByID, TemporalPath: filepath.Join(t.TempDir(), "missing"),
				OutputRoot: t.TempDir(), RunID: "safe",
			},
			want: "inspect Temporal binary",
		},
		{
			name: "non-executable Temporal binary",
			options: Options{
				Arm: ArmStaleByID, TemporalPath: filepath.Join(t.TempDir(), "not-executable"),
				OutputRoot: t.TempDir(), RunID: "safe",
			},
			want: "not executable",
		},
	}
	if err := os.WriteFile(tests[3].options.TemporalPath, []byte("test"), 0o600); err != nil {
		t.Fatalf("write non-executable fixture: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Run(context.Background(), test.options)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run() error = %v; want substring %q", err, test.want)
			}
		})
	}
}

func TestAwaitAttemptsRejectsUnexpectedAndDuplicateAttempts(t *testing.T) {
	tests := []struct {
		name     string
		attempts []int32
		want     string
	}{
		{name: "unexpected", attempts: []int32{3}, want: "unexpected attempt"},
		{name: "duplicate", attempts: []int32{1, 1}, want: "duplicate attempt"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := make(chan capturedAttempt, len(test.attempts))
			for _, attempt := range test.attempts {
				channel <- capturedAttempt{observation: AttemptObservation{Attempt: attempt}}
			}
			_, _, err := awaitAttempts(context.Background(), channel)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("awaitAttempts() error = %v; want substring %q", err, test.want)
			}
		})
	}
}
