package lab

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestHTTPBarrierExpectationsBindDeclaredCodexFaultIdentities(t *testing.T) {
	tests := []struct {
		name     string
		mode     RecoveryMode
		boundary FaultBoundary
		want     []failureinject.Expectation
	}{
		{
			name: "unsafe pre-thread deliveries", mode: RecoveryModeUnsafeFresh, boundary: FaultBeforeThreadObservation,
			want: []failureinject.Expectation{
				{Point: preThreadBarrier, SessionID: "session-1", Generation: 1, ActorID: "worker-one-attempt-1"},
				{Point: preThreadBarrier, SessionID: "session-1", Generation: 1, ActorID: "worker-two-attempt-2"},
			},
		},
		{
			name: "resume final-output deliveries", mode: RecoveryModeResumeOnly, boundary: FaultAfterFinalOutput,
			want: []failureinject.Expectation{
				{Point: finalOutputBarrier, SessionID: "session-1", Generation: 1, ActorID: "worker-one-attempt-1"},
				{Point: finalOutputBarrier, SessionID: "session-1", Generation: 1, ActorID: "worker-two-attempt-2"},
			},
		},
		{
			name: "fenced claim", mode: RecoveryModeFenced, boundary: FaultAfterClaimBeforeExec,
			want: []failureinject.Expectation{
				{Point: claimBeforeExecBarrier, SessionID: "session-1", Generation: 1, ActorID: "codex-supervisor-g1"},
			},
		},
		{
			name: "fenced thread registration", mode: RecoveryModeFenced, boundary: FaultAfterThreadBeforeRegistration,
			want: []failureinject.Expectation{
				{Point: threadRegistrationBarrier, SessionID: "session-1", Generation: 1, ActorID: "codex-supervisor-g1"},
			},
		},
		{
			name: "fenced final output", mode: RecoveryModeFenced, boundary: FaultAfterFinalOutput,
			want: []failureinject.Expectation{
				{Point: finalOutputBarrier, SessionID: "session-1", Generation: 1, ActorID: "codex-supervisor-g1"},
			},
		},
		{
			name: "fenced process replacement generations", mode: RecoveryModeFenced,
			boundary: FaultProcessFailureReplacement,
			want: []failureinject.Expectation{
				{Point: preThreadBarrier, SessionID: "session-1", Generation: 1, ActorID: "codex-supervisor-g1"},
				{Point: preThreadBarrier, SessionID: "session-1", Generation: 2, ActorID: "codex-supervisor-g2"},
			},
		},
		{
			name: "fenced cancellation thread", mode: RecoveryModeFenced,
			boundary: FaultCancellationWhileExecuting,
			want: []failureinject.Expectation{
				{Point: threadRegistrationBarrier, SessionID: "session-1", Generation: 1, ActorID: "codex-supervisor-g1"},
			},
		},
		{name: "file barrier needs no HTTP grant", mode: RecoveryModeFenced, boundary: FaultConcurrentRecovery},
		{name: "effect file barrier needs no HTTP grant", mode: RecoveryModeFenced, boundary: FaultAfterToolEffect},
		{name: "unfaulted needs no HTTP grant", mode: RecoveryModeFenced, boundary: FaultNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := httpBarrierExpectations(test.mode, test.boundary, "session-1")
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("expectations = %+v; want %+v", got, test.want)
			}
		})
	}
}

func TestClassifyPostExecutionControlRequiresCompetingExecutions(t *testing.T) {
	tests := []struct {
		name         string
		effects      int
		workspace    int
		attempts     int
		wantAdmitted bool
		wantReasons  []string
	}{
		{name: "single physical effect", effects: 1, workspace: 1, attempts: 2, wantAdmitted: true, wantReasons: []string{"competing_execution"}},
		{name: "duplicate physical effect", effects: 2, workspace: 2, attempts: 2, wantAdmitted: true, wantReasons: []string{"competing_execution", "duplicate_effect"}},
		{name: "missing recovery attempt", effects: 1, workspace: 1, attempts: 1},
		{name: "workspace disagrees", effects: 1, workspace: 0, attempts: 2},
		{name: "too many effects", effects: 3, workspace: 3, attempts: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifyPostExecutionControl(test.effects, test.workspace, test.attempts)
			if got.Admitted != test.wantAdmitted || got.SafetyPassed ||
				got.NegativeControlTriggered != test.wantAdmitted || !reflect.DeepEqual(got.ReasonCodes, test.wantReasons) {
				t.Fatalf("classifyPostExecutionControl() = %+v, want admitted=%t reasons=%v", got, test.wantAdmitted, test.wantReasons)
			}
		})
	}
}

func TestCollectTrialAttemptsAllowsPreThreadProcessReplacementEvidence(t *testing.T) {
	runRoot := filepath.Join(t.TempDir(), ".staging-codex-direct-fenced-start-or-attach-authorized-process-failure-before-thread-trial-1")
	root := filepath.Join(runRoot, "attempts")
	attemptID := "supervisor-generation-1"
	directory := filepath.Join(root, attemptID)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	process := ProcessRecord{AttemptID: attemptID, ActorID: "codex-supervisor-g1", PID: 101, Identity: "pid:101:start:fixture"}
	request := ControlledEffectInput{
		WorkspacePath:       filepath.Join(runRoot, "fixture", "effects.jsonl"),
		ThreadReceiptPath:   filepath.Join(directory, threadReceiptFile),
		CanonicalThreadPath: filepath.Join(root, "canonical-thread.json"),
		SupervisorURL:       "http://127.0.0.1:12345",
		OwnershipGeneration: 1,
		OwnerCapability:     "fixture-capability",
		Payload:             "controlled-edit",
		BarrierURL:          "http://127.0.0.1:12346",
		BarrierPoint:        committedEffectBarrier,
		LogicalSessionID:    "logical-session",
		LogicalTurnID:       "turn-1",
		LogicalEffectID:     "effect-1",
		PhysicalAttemptID:   attemptID,
		ActorID:             process.ActorID,
	}
	if err := writeJSONExclusive(filepath.Join(directory, attemptID+".process-started.json"), process); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONExclusive(filepath.Join(directory, effectRequestFile), request); err != nil {
		t.Fatal(err)
	}

	if _, err := collectTrialAttempts(root, FaultBeforeThreadObservation, "/opt/effect"); err == nil {
		t.Fatal("ordinary pre-thread evidence without a thread receipt must fail closed")
	}
	attempts, err := collectTrialAttempts(root, FaultProcessFailureReplacement, "/opt/effect")
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Process == nil || attempts[0].Thread != nil || attempts[0].Request == nil {
		t.Fatalf("unexpected replacement evidence: %+v", attempts)
	}
	if attempts[0].Request.OwnerCapability != "" || attempts[0].Request.OwnerCapabilitySHA256 == "" {
		t.Fatalf("published request retained raw capability: %+v", attempts[0].Request)
	}
	invalidStream := `{"type":"thread.started","thread_id":"` + testThreadID + `"}` + "\n" +
		`{"type":"turn.started"}` + "\n" +
		`{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"/opt/other --request ` +
		filepath.Join(directory, effectRequestFile) + `","exit_code":null,"status":"in_progress"}}` + "\n"
	if err := os.WriteFile(filepath.Join(directory, attemptID+".stdout.jsonl"), []byte(invalidStream), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := collectTrialAttempts(root, FaultProcessFailureReplacement, "/opt/effect"); err == nil {
		t.Fatal("structurally invalid interrupted stream was accepted as an ordinary incomplete attempt")
	}
}

func TestExpectedAttemptCommandSurvivesSealingAndTransportRelocation(t *testing.T) {
	runID := "codex-direct-unsafe-fresh-unfaulted-trial-1"
	request := ControlledEffectInput{
		WorkspacePath: "/original/evidence/.staging-" + runID + "/fixture/effects.jsonl",
	}
	got, err := expectedAttemptCommand(
		"/restored/evidence/"+runID+"/attempts", "attempt-1", request, "/original/bin/effect",
	)
	if err != nil {
		t.Fatalf("derive transported command: %v", err)
	}
	want := "/original/bin/effect --request /original/evidence/.staging-" + runID +
		"/attempts/attempt-1/effect-request.json"
	if got != want {
		t.Fatalf("transported command = %q, want %q", got, want)
	}
	request.WorkspacePath = "/other/.staging-unrelated/fixture/effects.jsonl"
	if _, err := expectedAttemptCommand(
		"/restored/evidence/"+runID+"/attempts", "attempt-1", request, "/original/bin/effect",
	); err == nil {
		t.Fatal("unrelated original staging identity was accepted")
	}
}
