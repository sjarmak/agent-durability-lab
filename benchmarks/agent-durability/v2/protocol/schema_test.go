package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestEvidenceBundleValidationAcceptsABAAndRejectsContradictions(t *testing.T) {
	t.Parallel()

	bundle := validABABundle()
	if err := bundle.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*EvidenceBundle)
	}{
		{name: "suite mismatch", mutate: func(value *EvidenceBundle) { value.Manifest.Suite = SuiteRecoveryDynamics }},
		{name: "current generation absent", mutate: func(value *EvidenceBundle) { value.Authority.CurrentGeneration = 10 }},
		{name: "two active epochs", mutate: func(value *EvidenceBundle) { value.Authority.Epochs[0].State = OwnerEpochActive }},
		{name: "rejected destination action applied", mutate: func(value *EvidenceBundle) {
			value.Destination.Attempts[0].Applied = true
		}},
		{name: "dependency attempt missing from events", mutate: func(value *EvidenceBundle) {
			value.Dependency.Requests[0].AttemptID = "unknown-attempt"
		}},
		{name: "workload count mismatch", mutate: func(value *EvidenceBundle) { value.Workload.ExpectedWorkItems = 2 }},
		{name: "fault references forward event", mutate: func(value *EvidenceBundle) { value.Fault.AfterEventID = "event-3" }},
		{name: "oracle visibility missing", mutate: func(value *EvidenceBundle) {
			value.Input.OracleVisibility = value.Input.OracleVisibility[:len(value.Input.OracleVisibility)-1]
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := bundle.Clone()
			test.mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("Validate() succeeded, want error")
			}
		})
	}
}

func validABABundle() EvidenceBundle {
	events := []CausalEvent{
		{
			Sequence: 1, EventID: "event-1", Time: "2026-08-08T00:00:00Z", Kind: EventOperationReady,
			RunID: "run-aba", LogicalOperationID: "operation-aba", WorkItemID: "item-aba", Decision: DecisionObserved,
		},
		{
			Sequence: 2, EventID: "event-2", ParentEventIDs: []string{"event-1"}, Time: "2026-08-08T00:00:01Z",
			Kind: EventAttemptStarted, RunID: "run-aba", LogicalOperationID: "operation-aba", WorkItemID: "item-aba",
			AttemptID: "attempt-g7", RetryLayer: RetryLayerActivity, RetryOrdinal: 1, ActorID: "A", Generation: 7,
			CapabilityHash: validHash("a"), WorkerID: "worker-1", ProcessIdentity: "pid:101:start:fixture", Decision: DecisionAccepted,
		},
		{
			Sequence: 3, EventID: "event-3", ParentEventIDs: []string{"event-2"}, Time: "2026-08-08T00:00:03Z",
			Kind: EventActionRejected, RunID: "run-aba", LogicalOperationID: "operation-aba", WorkItemID: "item-aba",
			AttemptID: "attempt-g7", RetryLayer: RetryLayerActivity, RetryOrdinal: 1, ActorID: "A", Generation: 7,
			CapabilityHash: validHash("a"), LogicalEffectID: "effect-aba", PhysicalAttemptID: "physical-g7",
			Decision: DecisionRejected,
		},
	}
	return EvidenceBundle{
		Manifest: Manifest{
			ContractVersion: ContractVersion, RunID: "run-aba", Suite: SuiteAuthority, Case: CaseABAReacquisition,
			Probe: ProbeProtected, Trial: 1, EpisodeID: "episode-1", Seed: 17, CohortSize: 1,
			EvidenceSHA256: evidenceHashes(), InputSHA256: validHash("input"),
		},
		Events: events,
		Authority: AuthorityState{
			LogicalOperationID: "operation-aba", CurrentOwnerID: "A", CurrentGeneration: 9,
			CurrentCapabilityHash: validHash("c"), CurrentOwnerAlive: true,
			Epochs: []OwnerEpoch{
				{OwnerID: "A", Generation: 7, CapabilityHash: validHash("a"), State: OwnerEpochObsolete, Sequence: 1},
				{OwnerID: "B", Generation: 8, CapabilityHash: validHash("b"), State: OwnerEpochCompleted, Sequence: 2},
				{OwnerID: "A", Generation: 9, CapabilityHash: validHash("c"), State: OwnerEpochActive, Sequence: 3},
			},
		},
		Destination: DestinationState{
			DestinationID: "destination-1",
			Attempts: []DestinationAttempt{{
				LogicalOperationID: "operation-aba", LogicalEffectID: "effect-aba", PhysicalAttemptID: "physical-g7",
				OwnerID: "A", Generation: 7, CapabilityHash: validHash("a"), EventID: "event-3",
				Decision: DecisionRejected, Applied: false,
			}},
		},
		Dependency: DependencyState{
			DependencyID: "dependency-1",
			Transitions:  []DependencyTransition{{Sequence: 1, Time: "2026-08-08T00:00:00Z", State: DependencyHealthy}},
			Requests: []DependencyRequest{{
				RequestID: "request-1", LogicalOperationID: "operation-aba", WorkItemID: "item-aba",
				AttemptID: "attempt-g7", RetryLayer: RetryLayerActivity, RetryOrdinal: 1,
				StartedAt: "2026-08-08T00:00:01Z", FinishedAt: "2026-08-08T00:00:03Z", Outcome: "stale_rejected",
			}},
		},
		Workload: WorkloadState{
			EpisodeID: "episode-1", ExpectedWorkItems: 1,
			Items: []WorkItem{{WorkItemID: "item-aba", LogicalOperationID: "operation-aba", State: WorkItemSucceeded}},
		},
		Fault: FaultBoundary{
			Point: "g7-delayed-until-g9-current", Triggered: true,
			AfterSequence: 2, AfterEventID: "event-2", BeforeSequence: 3, BeforeEventID: "event-3",
			TriggeredAt: "2026-08-08T00:00:02Z",
		},
		Processes: []ProcessObservation{{
			EventID: "event-2", OwnerID: "A", Generation: 7, WorkerID: "worker-1",
			ProcessIdentity: "pid:101:start:fixture", State: "running",
		}},
		Native: []NativeRecord{{Sequence: 1, Time: "2026-08-08T00:00:01Z", Kind: "attempt_started", Detail: "attempt-g7"}},
		Input: EffectiveInput{
			AdapterID: "calibration", AdapterVersion: "source-sha256:" + validHash("adapter"), SystemID: "calibration",
			AgentBinarySHA256: validHash("fixture-agent"),
			Runtime:           "go-test", AuthorityProtocol: AuthorityProtocol, DependencyProtocol: DependencyProtocol,
			FailureProtocol: FailureProtocol, OracleProtocol: OracleProtocol, DestinationID: "destination-1",
			OracleVisibility: OracleVisibility(), HostLimits: map[string]int64{"workers": 2}, Settings: map[string]string{"probe": "protected"},
		},
	}
}

func evidenceHashes() map[string]string {
	hashes := make(map[string]string, len(RawEvidenceFiles())-1)
	for _, name := range RawEvidenceFiles()[1:] {
		hashes[name] = validHash(name)
	}
	hashes[EffectiveInputFile] = validHash("input")
	return hashes
}

func validHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
