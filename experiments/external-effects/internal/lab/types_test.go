package lab

import "testing"

func TestDestinationAndModeValidation(t *testing.T) {
	t.Parallel()
	for _, destination := range AllDestinations() {
		if !destination.Valid() {
			t.Fatalf("destination %q should be valid", destination)
		}
	}
	if Destination("unknown").Valid() {
		t.Fatal("unknown destination should be invalid")
	}
	for _, mode := range []Mode{ModeUnsafe, ModeProtected} {
		if !mode.Valid() {
			t.Fatalf("mode %q should be valid", mode)
		}
	}
	if Mode("unknown").Valid() {
		t.Fatal("unknown mode should be invalid")
	}
}

func TestProtectedOutcomeNamesTheDestinationMechanism(t *testing.T) {
	t.Parallel()
	want := map[Destination]EffectOutcome{
		DestinationIdempotentAPI:    OutcomeDeduplicated,
		DestinationNonIdempotentAPI: OutcomeReconciled,
		DestinationDatabase:         OutcomeDeduplicated,
		DestinationGit:              OutcomeReconciled,
		DestinationMessage:          OutcomeDeduplicated,
		DestinationArtifact:         OutcomeDeduplicated,
	}
	for destination, expected := range want {
		if got := protectedOutcome(destination); got != expected {
			t.Errorf("protectedOutcome(%q) = %q, want %q", destination, got, expected)
		}
	}
	if got := protectedOutcome("unknown"); got != "" {
		t.Errorf("unknown protected outcome = %q, want empty", got)
	}
}

func TestSafePathComponent(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"run-1", "run_1.json", "A1"} {
		if !safePathComponent(value) {
			t.Errorf("safePathComponent(%q) = false", value)
		}
	}
	for _, value := range []string{"", ".", "..", "a/b", "a:b"} {
		if safePathComponent(value) {
			t.Errorf("safePathComponent(%q) = true", value)
		}
	}
}
