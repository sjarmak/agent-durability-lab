package protocol

import (
	"errors"
	"slices"
	"testing"
)

func TestCausalEventsRequireCompleteTopologySpecificIdentityAndLineage(t *testing.T) {
	direct := validIdentity(TopologyDirectActivity)
	events := []CausalEvent{
		{Identity: direct, Sequence: 1, EventID: "event-1", TimestampUTC: "2026-08-09T16:00:00Z", Kind: EventInputRegistered, Decision: DecisionObserved},
		{Identity: direct, Sequence: 2, EventID: "event-2", ParentEventIDs: []string{"event-1"}, TimestampUTC: "2026-08-09T16:00:00.001Z", MonotonicOffsetNS: 1_000_000, Kind: EventActivityStarted, Decision: DecisionObserved},
		{Identity: direct, Sequence: 3, EventID: "event-3", ParentEventIDs: []string{"event-2"}, TimestampUTC: "2026-08-09T16:00:00.002Z", MonotonicOffsetNS: 2_000_000, Kind: EventAcknowledged, Decision: DecisionAccepted},
	}
	if err := ValidateCausalEvents(events); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func([]CausalEvent)
	}{
		{name: "missing parent", mutate: func(value []CausalEvent) { value[1].ParentEventIDs = nil }},
		{name: "unknown parent", mutate: func(value []CausalEvent) { value[1].ParentEventIDs[0] = "future" }},
		{name: "duplicate event", mutate: func(value []CausalEvent) { value[2].EventID = value[1].EventID }},
		{name: "monotonic regression", mutate: func(value []CausalEvent) { value[2].MonotonicOffsetNS = 1 }},
		{name: "wrong run", mutate: func(value []CausalEvent) { value[2].RunID = "other" }},
		{name: "direct has child", mutate: func(value []CausalEvent) { value[1].ChildWorkflowID, value[1].ChildRunID = "child", "child-run" }},
		{name: "missing worker", mutate: func(value []CausalEvent) { value[1].WorkerID = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneEvents(events)
			test.mutate(candidate)
			if err := ValidateCausalEvents(candidate); !errors.Is(err, ErrInvalidEvidence) {
				t.Fatalf("ValidateCausalEvents() error = %v", err)
			}
		})
	}
}

func TestChildWorkflowIdentityIsRequiredOnlyForChildArm(t *testing.T) {
	child := validIdentity(TopologyChildWorkflow)
	if err := child.Validate(); err != nil {
		t.Fatal(err)
	}
	child.ChildRunID = ""
	if err := child.Validate(); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("missing child run error = %v", err)
	}
	child.ChildWorkflowID = ""
	if err := child.Validate(); err != nil {
		t.Fatalf("parent-scoped child-topology identity = %v", err)
	}

	direct := validIdentity(TopologyDirectActivity)
	if err := direct.Validate(); err != nil {
		t.Fatal(err)
	}
	direct.ChildWorkflowID = "unexpected"
	if err := direct.Validate(); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("unexpected direct child error = %v", err)
	}
}

func TestActivityAttemptIsDeliveryIdentityNotLogicalOrProcessIdentity(t *testing.T) {
	first := validIdentity(TopologyDirectActivity)
	second := first
	second.ActivityAttempt = 2
	second.ProcessIdentity = "pid:202/start:second"
	second.WorkerPID = 202
	if err := ValidateAttemptTransition(first, second); err != nil {
		t.Fatal(err)
	}
	second.Generation = 2
	second.CapabilityHash = testSHA('b')
	if err := ValidateAttemptTransition(first, second); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("retry changed fenced authority error = %v", err)
	}
	second.Generation = first.Generation
	second.CapabilityHash = first.CapabilityHash
	second.WorkItemID = "different-item"
	if err := ValidateAttemptTransition(first, second); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("changed logical identity error = %v", err)
	}
}

func TestRecoveryDynamicsCasesFreezeCompleteMetricSets(t *testing.T) {
	want := map[CaseID][]metricSpec{
		CaseCrashRecoveryBoundaries: {
			{name: "agent_process_count", unit: "count"},
			{name: "duplicate_effect_count", unit: "count"},
			{name: "duplicate_result_count", unit: "count"},
			{name: "time_to_recovery_ms", unit: "ms"},
			{name: "schedule_to_start_ms", unit: "ms"},
			{name: "activity_attempt_count", unit: "count"},
			{name: "workflow_task_count", unit: "count"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		},
		CaseLayeredRetryAmplification: {
			{name: "physical_request_count", unit: "count"},
			{name: "amplification_factor", unit: "ratio_milli"},
			{name: "retry_delay_ms", unit: "ms"},
			{name: "active_execution_ms", unit: "ms"},
			{name: "recovery_delay_ms", unit: "ms"},
			{name: "cost_units", unit: "cost_units"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		},
		CaseOutageBacklogHerdRecovery: {
			{name: "peak_qps", unit: "requests_per_second"},
			{name: "peak_retry_concurrency", unit: "count"},
			{name: "backlog_integral_ms", unit: "item_ms"},
			{name: "backlog_drain_p50_ms", unit: "ms"},
			{name: "backlog_drain_p90_ms", unit: "ms"},
			{name: "recovery_delay_ms", unit: "ms"},
			{name: "duplicate_effect_count", unit: "count"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		},
		CaseBackpressureOverload: {
			{name: "schedule_to_start_ms", unit: "ms"},
			{name: "queue_age_ms", unit: "ms"},
			{name: "end_to_end_latency_ms", unit: "ms"},
			{name: "throughput_per_second", unit: "items_per_second_milli"},
			{name: "admission_rejection_fraction", unit: "ratio_milli"},
			{name: "peak_in_flight_count", unit: "count"},
			{name: "history_events_per_item", unit: "events_per_item_milli"},
			{name: "history_bytes_per_item", unit: "bytes_per_item"},
		},
		CasePoisonWorkIsolation: {
			{name: "poison_attempt_count", unit: "count"},
			{name: "poison_cost_units", unit: "cost_units"},
			{name: "poison_capacity_ms", unit: "ms"},
			{name: "healthy_schedule_to_start_ms", unit: "ms"},
			{name: "healthy_task_latency_ms", unit: "ms"},
			{name: "healthy_completion_fraction", unit: "ratio_milli"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		},
		CaseSilentProgress: {
			{name: "failure_detection_latency_ms", unit: "ms"},
			{name: "false_positive_revocation_count", unit: "count"},
			{name: "stale_action_accept_count", unit: "count"},
			{name: "replacement_recovery_ms", unit: "ms"},
			{name: "healthy_task_latency_ms", unit: "ms"},
			{name: "end_to_end_latency_ms", unit: "ms"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		},
	}
	for benchmarkCase, specs := range want {
		if got := metricSpecs(benchmarkCase); !slices.Equal(got, specs) {
			t.Fatalf("metricSpecs(%s) = %+v, want %+v", benchmarkCase, got, specs)
		}
	}
}

func TestRecoveryDynamicsRequiresCompleteTerminalAccounting(t *testing.T) {
	manifest := Manifest{Case: CaseCrashRecoveryBoundaries}
	required := map[string]bool{"item-001": true, "item-002": true}
	state := validRecoveryDynamics(manifest.Case)
	if err := validateRecoveryDynamics(state, manifest, required); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*RecoveryDynamics)
	}{
		{name: "missing item", mutate: func(value *RecoveryDynamics) { value.Items = value.Items[:1] }},
		{name: "duplicate item", mutate: func(value *RecoveryDynamics) { value.Items[1].WorkItemID = value.Items[0].WorkItemID }},
		{name: "admitted without start", mutate: func(value *RecoveryDynamics) { value.Items[0].StartEventID = "" }},
		{name: "unadmitted succeeded", mutate: func(value *RecoveryDynamics) {
			value.Items[0].Admitted = false
			value.Items[0].StartEventID = ""
		}},
		{name: "missing bound", mutate: func(value *RecoveryDynamics) { value.Bounds = value.Bounds[:len(value.Bounds)-1] }},
		{name: "changed frozen bound", mutate: func(value *RecoveryDynamics) { value.Bounds[0].Value++ }},
		{name: "missing metric", mutate: func(value *RecoveryDynamics) { value.Metrics = value.Metrics[:len(value.Metrics)-1] }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneRecoveryDynamics(state)
			test.mutate(&candidate)
			if err := validateRecoveryDynamics(candidate, manifest, required); !errors.Is(err, ErrInvalidEvidence) {
				t.Fatalf("validateRecoveryDynamics() error = %v", err)
			}
		})
	}
}

func TestRecoveryDynamicsReferencesTypedCausalEvents(t *testing.T) {
	state := validRecoveryDynamics(CaseCrashRecoveryBoundaries)
	events := map[string]CausalEvent{
		"schedule-1": {Identity: Identity{WorkItemID: "item-001"}, Sequence: 1, EventID: "schedule-1", Kind: EventActivityScheduled},
		"start-1":    {Identity: Identity{WorkItemID: "item-001"}, Sequence: 2, EventID: "start-1", Kind: EventActivityStarted},
		"terminal-1": {Identity: Identity{WorkItemID: "item-001"}, Sequence: 3, EventID: "terminal-1", Kind: EventResultAccepted, Decision: DecisionAccepted},
		"schedule-2": {Identity: Identity{WorkItemID: "item-002"}, Sequence: 4, EventID: "schedule-2", Kind: EventActivityScheduled},
		"start-2":    {Identity: Identity{WorkItemID: "item-002"}, Sequence: 5, EventID: "start-2", Kind: EventActivityStarted},
		"terminal-2": {Identity: Identity{WorkItemID: "item-002"}, Sequence: 6, EventID: "terminal-2", Kind: EventResultAccepted, Decision: DecisionAccepted},
		"ack":        {EventID: "ack", Kind: EventAcknowledged, Decision: DecisionAccepted},
	}
	if err := validateRecoveryReferences(state, events); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*RecoveryDynamics, map[string]CausalEvent)
	}{
		{name: "unknown schedule", mutate: func(value *RecoveryDynamics, _ map[string]CausalEvent) { value.Items[0].ScheduleEventID = "missing" }},
		{name: "wrong schedule kind", mutate: func(_ *RecoveryDynamics, value map[string]CausalEvent) {
			event := value["schedule-1"]
			event.Kind = EventActivityStarted
			value["schedule-1"] = event
		}},
		{name: "wrong terminal item", mutate: func(_ *RecoveryDynamics, value map[string]CausalEvent) {
			event := value["terminal-1"]
			event.WorkItemID = "item-002"
			value["terminal-1"] = event
		}},
		{name: "wrong acknowledgement", mutate: func(_ *RecoveryDynamics, value map[string]CausalEvent) {
			event := value["ack"]
			event.Kind = EventOutcomeAccepted
			value["ack"] = event
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneRecoveryDynamics(state)
			candidateEvents := make(map[string]CausalEvent, len(events))
			for key, event := range events {
				candidateEvents[key] = event
			}
			test.mutate(&candidate, candidateEvents)
			if err := validateRecoveryReferences(candidate, candidateEvents); !errors.Is(err, ErrInvalidEvidence) {
				t.Fatalf("validateRecoveryReferences() error = %v", err)
			}
		})
	}
}

func TestDependencyRetriesRequireOrderedOwnedLineage(t *testing.T) {
	state := DependencyState{RunID: "run-1", Requests: []DependencyRequest{
		{
			RequestID: "request-1", StartedEventID: "started-1", EventID: "finished-1", WorkItemID: "item-001",
			Attempt: 1, RetryOrdinal: 1, RetryOwner: "activity", Outcome: "timeout", CostUnits: 1,
			StartedOffsetNS: 10, FinishedOffsetNS: 20, ServiceMS: 1, ConcurrentAtStart: 1,
		},
		{
			RequestID: "request-2", ParentRequestID: "request-1", StartedEventID: "started-2", EventID: "finished-2", WorkItemID: "item-001",
			Attempt: 1, RetryOrdinal: 2, RetryOwner: "activity", Outcome: "ok", CostUnits: 1,
			StartedOffsetNS: 30, FinishedOffsetNS: 40, RetryDelayMS: 1, ServiceMS: 1, ConcurrentAtStart: 1,
		},
	}}
	if err := validateDependency(state, true); err != nil {
		t.Fatal(err)
	}
	if err := validateDependency(DependencyState{RunID: "run-1", Requests: []DependencyRequest{
		{RequestID: "legacy", EventID: "finished", WorkItemID: "item-001", Attempt: 1, Outcome: "ok"},
	}}, false); err != nil {
		t.Fatalf("legacy orchestration dependency record = %v", err)
	}

	tests := []struct {
		name   string
		mutate func([]DependencyRequest)
	}{
		{name: "missing owner", mutate: func(value []DependencyRequest) { value[1].RetryOwner = "" }},
		{name: "missing started event", mutate: func(value []DependencyRequest) { value[1].StartedEventID = "" }},
		{name: "missing parent", mutate: func(value []DependencyRequest) { value[1].ParentRequestID = "" }},
		{name: "skipped ordinal", mutate: func(value []DependencyRequest) { value[1].RetryOrdinal = 3 }},
		{name: "wrong parent", mutate: func(value []DependencyRequest) { value[1].ParentRequestID = "unknown" }},
		{name: "offset regression", mutate: func(value []DependencyRequest) { value[1].StartedOffsetNS = 19 }},
		{name: "attempt regression", mutate: func(value []DependencyRequest) { value[1].Attempt = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := append([]DependencyRequest(nil), state.Requests...)
			test.mutate(candidate)
			if err := validateDependency(DependencyState{RunID: state.RunID, Requests: candidate}, true); !errors.Is(err, ErrInvalidEvidence) {
				t.Fatalf("validateDependency() error = %v", err)
			}
		})
	}
}

func TestDependencyReferencesStartedAndFinishedEvents(t *testing.T) {
	requests := []DependencyRequest{{
		RequestID: "request-1", StartedEventID: "started-1", EventID: "finished-1", WorkItemID: "item-001",
		Attempt: 2, RetryOrdinal: 1, RetryOwner: "workflow", Outcome: "ok", CostUnits: 1,
		StartedOffsetNS: 10, FinishedOffsetNS: 20, ServiceMS: 1, ConcurrentAtStart: 1,
	}}
	events := map[string]CausalEvent{
		"started-1":  {Identity: Identity{WorkItemID: "item-001", ActivityAttempt: 2}, Sequence: 2, EventID: "started-1", Kind: EventDependencyStarted},
		"finished-1": {Identity: Identity{WorkItemID: "item-001", ActivityAttempt: 2}, Sequence: 3, EventID: "finished-1", Kind: EventDependencyFinished},
	}
	if err := validateDependencyReferences(requests, events); err != nil {
		t.Fatal(err)
	}
	finished := events["finished-1"]
	finished.Sequence = 1
	events["finished-1"] = finished
	if err := validateDependencyReferences(requests, events); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("reversed dependency event lineage error = %v", err)
	}
}

func TestDependencyAttemptMayResetOnlyAcrossActivityIDs(t *testing.T) {
	requests := []DependencyRequest{
		{
			RequestID: "request-1", StartedEventID: "started-1", EventID: "finished-1", WorkItemID: "item-001",
			Attempt: 2, RetryOrdinal: 1, RetryOwner: "activity", Outcome: "outage", CostUnits: 1,
			StartedOffsetNS: 10, FinishedOffsetNS: 20, ConcurrentAtStart: 1,
		},
		{
			RequestID: "request-2", ParentRequestID: "request-1", StartedEventID: "started-2", EventID: "finished-2", WorkItemID: "item-001",
			Attempt: 1, RetryOrdinal: 2, RetryOwner: "workflow", Outcome: "ok", CostUnits: 1,
			StartedOffsetNS: 30, FinishedOffsetNS: 40, ConcurrentAtStart: 1,
		},
	}
	events := map[string]CausalEvent{
		"started-1":  {Identity: Identity{WorkItemID: "item-001", ActivityID: "work/item-001/initial", ActivityAttempt: 2}, Sequence: 1, EventID: "started-1", Kind: EventDependencyStarted},
		"finished-1": {Identity: Identity{WorkItemID: "item-001", ActivityID: "work/item-001/initial", ActivityAttempt: 2}, Sequence: 2, EventID: "finished-1", Kind: EventDependencyFinished},
		"started-2":  {Identity: Identity{WorkItemID: "item-001", ActivityID: "work/item-001/recovery-001", ActivityAttempt: 1}, Sequence: 3, EventID: "started-2", Kind: EventDependencyStarted},
		"finished-2": {Identity: Identity{WorkItemID: "item-001", ActivityID: "work/item-001/recovery-001", ActivityAttempt: 1}, Sequence: 4, EventID: "finished-2", Kind: EventDependencyFinished},
	}
	if err := validateDependencyReferences(requests, events); err != nil {
		t.Fatalf("attempt reset across Activity IDs: %v", err)
	}
	started := events["started-2"]
	started.ActivityID = "work/item-001/initial"
	events["started-2"] = started
	finished := events["finished-2"]
	finished.ActivityID = "work/item-001/initial"
	events["finished-2"] = finished
	if err := validateDependencyReferences(requests, events); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("attempt regression within one Activity ID error = %v", err)
	}
}

func TestCausalEventsRejectPerItemIdentityDrift(t *testing.T) {
	identity := validIdentity(TopologyChildWorkflow)
	events := []CausalEvent{
		{Identity: identity, Sequence: 1, EventID: "event-1", TimestampUTC: "2026-08-09T16:00:00Z", Kind: EventInputRegistered, Decision: DecisionObserved},
		{Identity: identity, Sequence: 2, EventID: "event-2", ParentEventIDs: []string{"event-1"}, TimestampUTC: "2026-08-09T16:00:00.001Z", MonotonicOffsetNS: 1, Kind: EventActivityStarted, Decision: DecisionObserved},
		{Identity: identity, Sequence: 3, EventID: "event-3", ParentEventIDs: []string{"event-2"}, TimestampUTC: "2026-08-09T16:00:00.002Z", MonotonicOffsetNS: 2, Kind: EventAcknowledged, Decision: DecisionAccepted},
	}
	events[1].ChildRunID = "different-child-run"
	if err := ValidateCausalEvents(events); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("per-item identity drift error = %v", err)
	}
}

func validIdentity(topology Topology) Identity {
	identity := Identity{
		ProtocolVersion: PublicationProtocolVersion,
		RunID:           "run-1", PairID: "pair-1", ScheduleBlockID: "block-1", TrackerBeadID: "temporal_projects-4ic.1",
		Topology: topology, Case: CaseJoinBarrier, Boundary: "designated-item-result-observed-before-activity-completion",
		Probe: ProbeProtected, Fanout: 8, LogicalOperationID: "operation-1", WorkItemID: "item-001",
		Generation: 1, CapabilityHash: testSHA('a'), ParentWorkflowID: "parent-workflow", ParentRunID: "parent-run",
		ActivityID: "activity-item-001", ActivityAttempt: 1, WorkerID: "worker-1", WorkerPID: 101,
		ProcessIdentity: "pid:101/start:first",
	}
	if topology == TopologyChildWorkflow {
		identity.ChildWorkflowID = "child-item-001"
		identity.ChildRunID = "child-run-item-001"
	}
	return identity
}

func cloneEvents(events []CausalEvent) []CausalEvent {
	clone := make([]CausalEvent, len(events))
	for index, event := range events {
		clone[index] = event
		clone[index].ParentEventIDs = append([]string(nil), event.ParentEventIDs...)
	}
	return clone
}

func validRecoveryDynamics(benchmarkCase CaseID) RecoveryDynamics {
	metrics := make([]Metric, 0, len(metricSpecs(benchmarkCase)))
	for _, spec := range metricSpecs(benchmarkCase) {
		metrics = append(metrics, Metric{Name: spec.name, Unit: spec.unit})
	}
	return RecoveryDynamics{
		Items: []RecoveryItemObservation{
			{WorkItemID: "item-001", Role: "healthy", Admitted: true, Disposition: RecoveryDispositionSucceeded, ScheduleEventID: "schedule-1", StartEventID: "start-1", TerminalEventID: "terminal-1", ActivityAttempts: 1, AgentProcesses: 1, AcceptedEffects: 1, AcceptedResults: 1, CostUnits: 1},
			{WorkItemID: "item-002", Role: "healthy", Admitted: true, Disposition: RecoveryDispositionSucceeded, ScheduleEventID: "schedule-2", StartEventID: "start-2", TerminalEventID: "terminal-2", ActivityAttempts: 1, AgentProcesses: 1, AcceptedEffects: 1, AcceptedResults: 1, CostUnits: 1},
		},
		ParentAcknowledgementEventID: "ack",
		Bounds: []Metric{
			{Name: "requests_per_item_max", Unit: "count", Value: 4},
			{Name: "retry_concurrency_max", Unit: "count", Value: 2},
			{Name: "in_flight_max", Unit: "count", Value: 8},
			{Name: "poison_attempts_max", Unit: "count", Value: 3},
			{Name: "progress_deadline_ms", Unit: "ms", Value: 5000},
		},
		Metrics: metrics,
	}
}

func cloneRecoveryDynamics(state RecoveryDynamics) RecoveryDynamics {
	state.Items = append([]RecoveryItemObservation(nil), state.Items...)
	state.Bounds = append([]Metric(nil), state.Bounds...)
	state.Metrics = append([]Metric(nil), state.Metrics...)
	return state
}

func testSHA(character byte) string {
	value := make([]byte, 64)
	for index := range value {
		value[index] = character
	}
	return string(value)
}
