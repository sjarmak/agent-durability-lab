package publication

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestRunPairHonorsFrozenOrderExcludesReadinessAndWritesEvidence(t *testing.T) {
	root := t.TempDir()
	clock := newFakeClock()
	var calls []string
	var callsMu sync.Mutex
	executors := map[string]TimedExecutor{
		SystemTemporal:   &fakeTimedExecutor{systemID: SystemTemporal, clock: clock, calls: &calls, callsMu: &callsMu},
		SystemPostgreSQL: &fakeTimedExecutor{systemID: SystemPostgreSQL, clock: clock, calls: &calls, callsMu: &callsMu},
	}
	block := PairBlock{
		Index: 1, PairID: "pair/outage-backlog-recovery/protected/slot-01",
		Case: protocol.CaseOutageBacklogRecovery, Probe: protocol.ProbeProtected, Slot: 1,
		SystemOrder: []string{SystemPostgreSQL, SystemTemporal},
	}
	execution, err := RunPair(context.Background(), RunnerConfig{
		Root: root, Phase: PhasePilot, Clock: clock, Executors: executors,
	}, block)
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := []string{"ready:postgresql-queue", "execute:postgresql-queue", "ready:temporal", "execute:temporal"}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls = %v, want %v", calls, wantCalls)
	}
	if execution.Admission != protocol.AdmissionValid || len(execution.Systems) != 2 {
		t.Fatalf("execution = %+v", execution)
	}
	for _, system := range execution.Systems {
		if system.DurationNanoseconds <= 0 || system.DurationNanoseconds >= int64(10*time.Second) {
			t.Errorf("system duration includes readiness/setup: %+v", system)
		}
		if len(system.Timing) != 5 || system.Timing[0].ElapsedNanoseconds < 0 {
			t.Errorf("timing = %+v", system.Timing)
		}
	}
	pairDir := filepath.Join(root, PairDirectoryName(block.PairID))
	for _, name := range []string{PublicationTimingFile, PublicationExecutionFile, PublicationInventoryFile} {
		if info, err := os.Stat(filepath.Join(pairDir, name)); err != nil || info.Size() == 0 {
			t.Fatalf("%s: info=%v error=%v", name, info, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(pairDir, PublicationExecutionFile))
	if err != nil {
		t.Fatal(err)
	}
	var persisted PairExecution
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, execution) {
		t.Fatalf("persisted execution differs\n got: %+v\nwant: %+v", persisted, execution)
	}
	if _, err := RunPair(context.Background(), RunnerConfig{Root: root, Phase: PhasePilot, Clock: clock, Executors: executors}, block); !errors.Is(err, protocol.ErrEvidenceExists) {
		t.Fatalf("second write error = %v", err)
	}
}

func TestRunPairRetainsInvalidPartialFailure(t *testing.T) {
	root := t.TempDir()
	clock := newFakeClock()
	executors := map[string]TimedExecutor{
		SystemTemporal:   &fakeTimedExecutor{systemID: SystemTemporal, clock: clock, fail: errors.New("injected executor failure")},
		SystemPostgreSQL: &fakeTimedExecutor{systemID: SystemPostgreSQL, clock: clock, omitBarrier: true},
	}
	block := PairBlock{
		Index: 2, PairID: "pair/silent-progress/protected/slot-01", Case: protocol.CaseSilentProgress,
		Probe: protocol.ProbeProtected, Slot: 1, SystemOrder: []string{SystemTemporal, SystemPostgreSQL},
	}
	execution, err := RunPair(context.Background(), RunnerConfig{Root: root, Phase: PhasePilot, Clock: clock, Executors: executors}, block)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Admission != protocol.AdmissionInvalid || len(execution.ReasonCodes) != 2 {
		t.Fatalf("execution = %+v", execution)
	}
	if execution.Systems[0].Error == "" || execution.Systems[1].Error == "" {
		t.Fatalf("partial failures were not retained: %+v", execution.Systems)
	}
	pairDir := filepath.Join(root, PairDirectoryName(block.PairID))
	if _, err := os.Stat(filepath.Join(pairDir, PublicationTimingFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(pairDir, PublicationExecutionFile)); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTimedResultRejectsInvalidSystemVerdict(t *testing.T) {
	request := EpisodeRequest{
		Phase: PhasePilot, PairID: "pilot-v2-pair/outage-backlog-recovery/unsafe/slot-01", PairIndex: 1,
		Case: protocol.CaseOutageBacklogRecovery, Probe: protocol.ProbeUnsafe, Slot: 1,
	}
	verdict := passingVerdict(request)
	verdict.Admission = protocol.AdmissionInvalid
	verdict.Correctness = protocol.OutcomeNotApplicable
	verdict.Safety = protocol.OutcomeNotApplicable
	verdict.Liveness = protocol.OutcomeNotApplicable
	verdict.Diagnosability = protocol.OutcomeNotApplicable
	verdict.EfficiencyEligible = false
	verdict.ReasonCodes = []string{protocol.ReasonNegativeControlWeak}

	err := validateTimedResult(request, TimedResult{
		ExecutionID: "execution-temporal", EvidenceDir: "evidence/temporal", Verdict: verdict,
	})
	if !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("validation error = %v, want invalid evidence", err)
	}
}

func TestTimingValidationRequiresExactBarrierAndMonotonicAnchors(t *testing.T) {
	valid := []TimingEvent{
		{Sequence: 1, UTC: "2026-08-09T00:00:00Z", ElapsedNanoseconds: 0, Kind: protocol.EventOperationReady},
		{Sequence: 2, UTC: "2026-08-09T00:00:00.001Z", ElapsedNanoseconds: int64(time.Millisecond), Kind: protocol.EventBarrierReached},
		{Sequence: 3, UTC: "2026-08-09T00:00:00.002Z", ElapsedNanoseconds: int64(2 * time.Millisecond), Kind: protocol.EventFaultCommitted},
		{Sequence: 4, UTC: "2026-08-09T00:00:00.003Z", ElapsedNanoseconds: int64(3 * time.Millisecond), Kind: protocol.EventRecoveryObserved},
		{Sequence: 5, UTC: "2026-08-09T00:00:00.004Z", ElapsedNanoseconds: int64(4 * time.Millisecond), Kind: protocol.EventAcknowledged},
	}
	if err := ValidateTimingEvents(protocol.ProbeProtected, valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func([]TimingEvent)
	}{
		{name: "missing barrier", mutate: func(events []TimingEvent) { events[1].Kind = protocol.EventAttemptStarted }},
		{name: "fault before barrier", mutate: func(events []TimingEvent) { events[1].Kind, events[2].Kind = events[2].Kind, events[1].Kind }},
		{name: "elapsed backwards", mutate: func(events []TimingEvent) { events[3].ElapsedNanoseconds = 1 }},
		{name: "UTC backwards", mutate: func(events []TimingEvent) { events[3].UTC = "2026-08-08T23:59:59Z" }},
		{name: "missing acknowledgement", mutate: func(events []TimingEvent) { events[4].Kind = protocol.EventAttemptFinished }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := append([]TimingEvent(nil), valid...)
			test.mutate(candidate)
			if err := ValidateTimingEvents(protocol.ProbeProtected, candidate); !errors.Is(err, protocol.ErrInvalidEvidence) {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type fakeTimedExecutor struct {
	systemID    string
	clock       *fakeClock
	calls       *[]string
	callsMu     *sync.Mutex
	fail        error
	omitBarrier bool
}

func (e *fakeTimedExecutor) SystemID() string { return e.systemID }

func (e *fakeTimedExecutor) Ready(context.Context) error {
	e.recordCall("ready:" + e.systemID)
	e.clock.advance(10 * time.Second)
	return nil
}

func (e *fakeTimedExecutor) ExecuteTimed(_ context.Context, request EpisodeRequest, recorder *TimingRecorder) (TimedResult, error) {
	e.recordCall("execute:" + e.systemID)
	recorder.Record(protocol.EventOperationReady, "", nil)
	e.clock.advance(time.Millisecond)
	if !e.omitBarrier {
		recorder.Record(protocol.EventBarrierReached, "fault", nil)
		e.clock.advance(time.Millisecond)
	}
	recorder.Record(protocol.EventFaultCommitted, "fault", nil)
	e.clock.advance(time.Millisecond)
	recorder.Record(protocol.EventRecoveryObserved, "fault", nil)
	e.clock.advance(time.Millisecond)
	recorder.Record(protocol.EventAcknowledged, "", nil)
	if e.fail != nil {
		return TimedResult{ExecutionID: "partial-" + e.systemID, EvidenceDir: "partial/" + e.systemID}, e.fail
	}
	return TimedResult{
		ExecutionID: "execution-" + e.systemID, EvidenceDir: "evidence/" + e.systemID,
		Verdict: passingVerdict(request),
	}, nil
}

func (e *fakeTimedExecutor) recordCall(value string) {
	if e.calls == nil {
		return
	}
	e.callsMu.Lock()
	*e.calls = append(*e.calls, value)
	e.callsMu.Unlock()
}

func passingVerdict(request EpisodeRequest) protocol.Verdict {
	return protocol.Verdict{
		ContractVersion: protocol.ContractVersion, RunID: request.PairID, Case: request.Case, Probe: request.Probe,
		Trial: request.Slot, Admission: protocol.AdmissionValid, Correctness: protocol.OutcomePass, Safety: protocol.OutcomePass,
		Liveness: protocol.OutcomePass, Diagnosability: protocol.OutcomePass, EfficiencyEligible: true, Oracle: protocol.OracleProtocol,
	}
}
