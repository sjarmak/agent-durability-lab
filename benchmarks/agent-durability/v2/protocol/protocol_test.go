package protocol

import (
	"slices"
	"testing"
)

func TestCasesMatchFrozenV2Contract(t *testing.T) {
	t.Parallel()

	want := []CaseID{
		CaseABAReacquisition,
		CaseLayeredRetryAmplification,
		CaseOutageBacklogRecovery,
		CaseBackpressureOverload,
		CasePoisonWorkIsolation,
		CaseSilentProgress,
	}
	if !slices.Equal(Cases(), want) {
		t.Fatalf("Cases() = %v, want %v", Cases(), want)
	}
}

func TestValidateCausalEventsRejectsMissingAndForwardParents(t *testing.T) {
	t.Parallel()

	events := validEvents()
	if err := ValidateCausalEvents("run-1", events); err != nil {
		t.Fatalf("ValidateCausalEvents(valid) failed: %v", err)
	}

	tests := []struct {
		name   string
		mutate func([]CausalEvent)
	}{
		{name: "missing parent", mutate: func(values []CausalEvent) { values[2].ParentEventIDs = []string{"missing"} }},
		{name: "forward parent", mutate: func(values []CausalEvent) { values[0].ParentEventIDs = []string{"event-2"} }},
		{name: "duplicate event", mutate: func(values []CausalEvent) { values[2].EventID = values[0].EventID }},
		{name: "missing logical operation", mutate: func(values []CausalEvent) { values[2].LogicalOperationID = "" }},
		{name: "retry without layer", mutate: func(values []CausalEvent) { values[2].RetryLayer = "" }},
		{name: "attempt without causal parent", mutate: func(values []CausalEvent) { values[2].ParentAttemptID = "" }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			values := append([]CausalEvent(nil), events...)
			values[0].ParentEventIDs = append([]string(nil), events[0].ParentEventIDs...)
			values[1].ParentEventIDs = append([]string(nil), events[1].ParentEventIDs...)
			values[2].ParentEventIDs = append([]string(nil), events[2].ParentEventIDs...)
			test.mutate(values)
			if err := ValidateCausalEvents("run-1", values); err == nil {
				t.Fatal("ValidateCausalEvents() succeeded, want error")
			}
		})
	}
}

func TestValidateCausalEventsAllowsRepeatedAttemptLifecycleAndOperationRoots(t *testing.T) {
	t.Parallel()

	events := validEvents()
	events = append(events,
		CausalEvent{
			Sequence: 4, EventID: "event-4", ParentEventIDs: []string{"event-3"}, Time: "2026-08-08T00:00:02Z",
			Kind: EventAttemptFinished, RunID: "run-1", LogicalOperationID: "operation-1", WorkItemID: "item-1",
			AttemptID: "activity-attempt-2", ParentAttemptID: "activity-attempt-1", RetryLayer: RetryLayerActivity,
			RetryOrdinal: 2, RetryCause: "timeout", Decision: DecisionAccepted,
		},
		CausalEvent{
			Sequence: 5, EventID: "event-5", Time: "2026-08-08T00:00:03Z", Kind: EventOperationReady,
			RunID: "run-1", LogicalOperationID: "operation-2", WorkItemID: "item-2", Decision: DecisionObserved,
		},
	)
	if err := ValidateCausalEvents("run-1", events); err != nil {
		t.Fatalf("ValidateCausalEvents() failed: %v", err)
	}

	events[3].RetryOrdinal = 3
	if err := ValidateCausalEvents("run-1", events); err == nil {
		t.Fatal("ValidateCausalEvents() allowed attempt identity drift")
	}
}

func TestVerdictValidationEnforcesParityGate(t *testing.T) {
	t.Parallel()

	verdict := Verdict{
		ContractVersion:    ContractVersion,
		RunID:              "run-1",
		Case:               CaseOutageBacklogRecovery,
		Probe:              ProbeProtected,
		Trial:              1,
		Admission:          AdmissionValid,
		Correctness:        OutcomePass,
		Safety:             OutcomePass,
		Liveness:           OutcomePass,
		Diagnosability:     OutcomePass,
		EfficiencyEligible: true,
		Oracle:             OracleProtocol,
	}
	if err := verdict.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}
	verdict.Safety = OutcomeFail
	if err := verdict.Validate(); err == nil {
		t.Fatal("Validate() allowed efficiency comparison after safety failure")
	}
}

func validEvents() []CausalEvent {
	return []CausalEvent{
		{
			Sequence: 1, EventID: "event-1", Time: "2026-08-08T00:00:00Z", Kind: EventOperationReady,
			RunID: "run-1", LogicalOperationID: "operation-1", Decision: DecisionObserved,
		},
		{
			Sequence: 2, EventID: "event-2", ParentEventIDs: []string{"event-1"}, Time: "2026-08-08T00:00:01Z",
			Kind: EventAttemptStarted, RunID: "run-1", LogicalOperationID: "operation-1", WorkItemID: "item-1",
			AttemptID: "activity-attempt-1", RetryLayer: RetryLayerActivity,
			RetryOrdinal: 1, ActorID: "worker-a", Generation: 7, CapabilityHash: "sha256:old",
			WorkerID: "worker-1", ProcessIdentity: "pid:101:start:fixture", Decision: DecisionAccepted,
		},
		{
			Sequence: 3, EventID: "event-3", ParentEventIDs: []string{"event-2"}, Time: "2026-08-08T00:00:02Z",
			Kind: EventAttemptStarted, RunID: "run-1", LogicalOperationID: "operation-1", WorkItemID: "item-1",
			AttemptID: "activity-attempt-2", ParentAttemptID: "activity-attempt-1", RetryLayer: RetryLayerActivity,
			RetryOrdinal: 2, RetryCause: "timeout", ActorID: "worker-a", Generation: 9, CapabilityHash: "sha256:current",
			WorkerID: "worker-2", ProcessIdentity: "pid:202:start:fixture", Decision: DecisionAccepted,
		},
	}
}
