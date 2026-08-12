package oracle

import (
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
)

func TestRequiresPassedReplayForEveryCapturedHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		replay  evidence.ReplayDisposition
		wantErr string
	}{
		{
			name: "captured and passed",
			replay: evidence.ReplayDisposition{
				Captured: true, Status: evidence.ReplayPassed, HistoryPath: "histories/run.json", HistoryHash: strings.Repeat("a", 64),
			},
		},
		{
			name: "captured but not replayed",
			replay: evidence.ReplayDisposition{
				Captured: true, Status: evidence.ReplayNotApplicable, HistoryPath: "histories/run.json", HistoryHash: strings.Repeat("a", 64),
			},
			wantErr: "captured history did not pass replay",
		},
		{
			name:    "captured without provenance",
			replay:  evidence.ReplayDisposition{Captured: true, Status: evidence.ReplayPassed},
			wantErr: "lacks a path",
		},
		{
			name: "captured with malformed hash",
			replay: evidence.ReplayDisposition{
				Captured: true, Status: evidence.ReplayPassed, HistoryPath: "histories/run.json", HistoryHash: strings.Repeat("z", 64),
			},
			wantErr: "hash is invalid",
		},
		{
			name: "calibration not applicable",
			replay: evidence.ReplayDisposition{
				Status: evidence.ReplayNotApplicable, Explanation: evidence.CalibrationReplayExplanation,
			},
		},
		{
			name:    "not captured without explanation",
			replay:  evidence.ReplayDisposition{Status: evidence.ReplayNotApplicable},
			wantErr: "explanation",
		},
		{
			name: "not captured but names bytes",
			replay: evidence.ReplayDisposition{
				Status: evidence.ReplayNotApplicable, Explanation: "none", HistoryPath: "history.json",
			},
			wantErr: "cannot name history bytes",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateReplay(test.replay)
			if test.wantErr == "" && err != nil {
				t.Fatalf("ValidateReplay() error = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("ValidateReplay() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}
