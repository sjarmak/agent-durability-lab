package publication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

const (
	PublicationTimingFile    = "publication-timing.jsonl"
	PublicationExecutionFile = "publication-execution.json"
	PublicationInventoryFile = "publication-inventory.json"
)

type Phase string

const (
	PhasePilot       Phase = "pilot"
	PhasePublication Phase = "publication"
)

func (p Phase) valid() bool { return p == PhasePilot || p == PhasePublication }

type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

type EpisodeRequest struct {
	Phase     Phase           `json:"phase"`
	PairID    string          `json:"pair_id"`
	PairIndex int             `json:"pair_index"`
	Case      protocol.CaseID `json:"case"`
	Probe     protocol.Probe  `json:"probe"`
	Slot      int             `json:"slot"`
}

type TimedResult struct {
	ExecutionID string           `json:"execution_id"`
	EvidenceDir string           `json:"evidence_dir"`
	Verdict     protocol.Verdict `json:"verdict"`
}

type TimedExecutor interface {
	SystemID() string
	Ready(context.Context) error
	ExecuteTimed(context.Context, EpisodeRequest, *TimingRecorder) (TimedResult, error)
}

type RunnerConfig struct {
	Root      string
	Phase     Phase
	Clock     Clock
	Executors map[string]TimedExecutor
}

type TimingEvent struct {
	Sequence           int               `json:"sequence"`
	SystemID           string            `json:"system_id"`
	PairID             string            `json:"pair_id"`
	Case               protocol.CaseID   `json:"case"`
	Probe              protocol.Probe    `json:"probe"`
	UTC                string            `json:"utc"`
	ElapsedNanoseconds int64             `json:"elapsed_nanoseconds"`
	Kind               string            `json:"kind"`
	Barrier            string            `json:"barrier,omitempty"`
	WorkItemID         string            `json:"work_item_id,omitempty"`
	AttemptID          string            `json:"attempt_id,omitempty"`
	Details            map[string]string `json:"details,omitempty"`
}

type TimingRecorder struct {
	mu       sync.Mutex
	clock    Clock
	start    time.Time
	systemID string
	request  EpisodeRequest
	events   []TimingEvent
}

func NewTimingRecorder(clock Clock, systemID string, request EpisodeRequest) *TimingRecorder {
	if clock == nil {
		clock = wallClock{}
	}
	return &TimingRecorder{clock: clock, start: clock.Now(), systemID: systemID, request: request}
}

func (r *TimingRecorder) Record(kind, barrier string, details map[string]string) TimingEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	event := TimingEvent{
		Sequence: len(r.events) + 1, SystemID: r.systemID, PairID: r.request.PairID,
		Case: r.request.Case, Probe: r.request.Probe, UTC: now.UTC().Format(time.RFC3339Nano),
		ElapsedNanoseconds: now.Sub(r.start).Nanoseconds(), Kind: kind, Barrier: barrier, Details: maps.Clone(details),
	}
	r.events = append(r.events, event)
	return event
}

func (r *TimingRecorder) Events() []TimingEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]TimingEvent, len(r.events))
	for index, event := range r.events {
		result[index] = event
		result[index].Details = maps.Clone(event.Details)
	}
	return result
}

type SystemRun struct {
	SystemID                   string           `json:"system_id"`
	Order                      int              `json:"order"`
	MeasuredStartUTC           string           `json:"measured_start_utc,omitempty"`
	MeasuredEndUTC             string           `json:"measured_end_utc,omitempty"`
	DurationNanoseconds        int64            `json:"duration_nanoseconds"`
	HarnessReturnUTC           string           `json:"harness_return_utc,omitempty"`
	HarnessDurationNanoseconds int64            `json:"harness_duration_nanoseconds"`
	ExecutionID                string           `json:"execution_id,omitempty"`
	EvidenceDir                string           `json:"evidence_dir,omitempty"`
	Verdict                    protocol.Verdict `json:"verdict"`
	Timing                     []TimingEvent    `json:"timing"`
	Error                      string           `json:"error,omitempty"`
}

type PairExecution struct {
	ProtocolVersion string             `json:"protocol_version"`
	Phase           Phase              `json:"phase"`
	PairID          string             `json:"pair_id"`
	PairIndex       int                `json:"pair_index"`
	Case            protocol.CaseID    `json:"case"`
	Probe           protocol.Probe     `json:"probe"`
	Slot            int                `json:"slot"`
	SystemOrder     []string           `json:"system_order"`
	Admission       protocol.Admission `json:"admission"`
	ReasonCodes     []string           `json:"reason_codes"`
	Systems         []SystemRun        `json:"systems"`
}

func RunPair(ctx context.Context, config RunnerConfig, block PairBlock) (PairExecution, error) {
	if err := validateRunnerConfig(config, block); err != nil {
		return PairExecution{}, err
	}
	if config.Clock == nil {
		config.Clock = wallClock{}
	}
	pairDir, err := createPairDirectory(config.Root, block.PairID)
	if err != nil {
		return PairExecution{}, err
	}
	execution := PairExecution{
		ProtocolVersion: ProtocolVersion, Phase: config.Phase, PairID: block.PairID, PairIndex: block.Index,
		Case: block.Case, Probe: block.Probe, Slot: block.Slot, SystemOrder: slices.Clone(block.SystemOrder),
		Admission: protocol.AdmissionValid,
	}
	request := EpisodeRequest{Phase: config.Phase, PairID: block.PairID, PairIndex: block.Index, Case: block.Case, Probe: block.Probe, Slot: block.Slot}
	for order, systemID := range block.SystemOrder {
		executor := config.Executors[systemID]
		run := SystemRun{SystemID: systemID, Order: order + 1}
		if err := executor.Ready(ctx); err != nil {
			run.Error = err.Error()
			execution.ReasonCodes = append(execution.ReasonCodes, systemID+":readiness_failed")
			execution.Systems = append(execution.Systems, run)
			continue
		}
		recorder := NewTimingRecorder(config.Clock, systemID, request)
		started := config.Clock.Now()
		run.MeasuredStartUTC = started.UTC().Format(time.RFC3339Nano)
		result, executeErr := executor.ExecuteTimed(ctx, request, recorder)
		finished := config.Clock.Now()
		run.HarnessReturnUTC = finished.UTC().Format(time.RFC3339Nano)
		run.HarnessDurationNanoseconds = finished.Sub(started).Nanoseconds()
		run.ExecutionID, run.EvidenceDir, run.Verdict = result.ExecutionID, result.EvidenceDir, result.Verdict
		run.Timing = recorder.Events()
		if len(run.Timing) > 0 {
			acknowledged := run.Timing[len(run.Timing)-1]
			run.MeasuredEndUTC = acknowledged.UTC
			run.DurationNanoseconds = acknowledged.ElapsedNanoseconds
		} else {
			run.MeasuredEndUTC = run.HarnessReturnUTC
			run.DurationNanoseconds = run.HarnessDurationNanoseconds
		}
		if executeErr != nil {
			run.Error = executeErr.Error()
			execution.ReasonCodes = append(execution.ReasonCodes, systemID+":execution_failed")
		} else {
			if err := ValidateTimingEvents(block.Probe, run.Timing); err != nil {
				run.Error = err.Error()
				execution.ReasonCodes = append(execution.ReasonCodes, systemID+":timing_invalid")
			}
			if err := validateTimingIdentity(systemID, request, run.Timing); err != nil {
				if run.Error == "" {
					run.Error = err.Error()
				}
				execution.ReasonCodes = append(execution.ReasonCodes, systemID+":timing_identity_invalid")
			}
			if err := validateTimedResult(request, result); err != nil {
				if run.Error == "" {
					run.Error = err.Error()
				}
				execution.ReasonCodes = append(execution.ReasonCodes, systemID+":result_invalid")
			}
		}
		execution.Systems = append(execution.Systems, run)
	}
	if len(execution.ReasonCodes) > 0 {
		execution.Admission = protocol.AdmissionInvalid
	}
	if err := writePairEvidence(pairDir, execution); err != nil {
		return PairExecution{}, err
	}
	return execution, nil
}

func validateTimingIdentity(systemID string, request EpisodeRequest, events []TimingEvent) error {
	for _, event := range events {
		if event.SystemID != systemID || event.PairID != request.PairID || event.Case != request.Case || event.Probe != request.Probe {
			return invalid("timing event identity")
		}
		if (event.Kind == protocol.EventBarrierReached || event.Kind == protocol.EventFaultCommitted || event.Kind == protocol.EventRecoveryObserved) && event.Barrier == "" {
			return invalid("timing named barrier")
		}
	}
	return nil
}

func ValidateTimingEvents(probe protocol.Probe, events []TimingEvent) error {
	if !probe.Valid() || len(events) == 0 {
		return invalid("timing events")
	}
	var previousUTC time.Time
	var previousElapsed int64
	barrierIndex, faultIndex, recoveryIndex := -1, -1, -1
	for index, event := range events {
		if event.Sequence != index+1 || event.ElapsedNanoseconds < 0 || (index > 0 && event.ElapsedNanoseconds < previousElapsed) {
			return invalid("timing sequence or monotonic duration")
		}
		parsed, err := time.Parse(time.RFC3339Nano, event.UTC)
		if err != nil {
			return invalid("timing UTC anchor")
		}
		_, offset := parsed.Zone()
		if offset != 0 || (!previousUTC.IsZero() && parsed.Before(previousUTC)) {
			return invalid("timing UTC order")
		}
		switch event.Kind {
		case protocol.EventBarrierReached:
			if barrierIndex == -1 {
				barrierIndex = index
			}
		case protocol.EventFaultCommitted:
			if faultIndex == -1 {
				faultIndex = index
			}
		case protocol.EventRecoveryObserved:
			if recoveryIndex == -1 {
				recoveryIndex = index
			}
		}
		previousUTC, previousElapsed = parsed, event.ElapsedNanoseconds
	}
	if events[0].Kind != protocol.EventOperationReady || events[len(events)-1].Kind != protocol.EventAcknowledged {
		return invalid("timing root or acknowledgement")
	}
	if probe == protocol.ProbeUnfaulted {
		if faultIndex != -1 {
			return invalid("unfaulted timing contains a fault")
		}
		return nil
	}
	if barrierIndex == -1 || faultIndex <= barrierIndex || recoveryIndex <= faultIndex {
		return invalid("fault is not exactly bracketed by barrier and recovery")
	}
	return nil
}

func validateRunnerConfig(config RunnerConfig, block PairBlock) error {
	if config.Root == "" || !config.Phase.valid() || block.Index < 1 || block.PairID == "" || !block.Case.Valid() ||
		!block.Probe.Valid() || block.Slot < 1 || len(block.SystemOrder) != 2 || block.SystemOrder[0] == block.SystemOrder[1] {
		return invalid("runner configuration or pair block")
	}
	for _, systemID := range block.SystemOrder {
		if systemID != SystemTemporal && systemID != SystemPostgreSQL {
			return invalid("pair system")
		}
		executor := config.Executors[systemID]
		if executor == nil || executor.SystemID() != systemID {
			return invalid("executor identity")
		}
	}
	return nil
}

func validateTimedResult(request EpisodeRequest, result TimedResult) error {
	if result.ExecutionID == "" || result.EvidenceDir == "" {
		return invalid("timed result identity")
	}
	if err := result.Verdict.Validate(); err != nil {
		return err
	}
	verdict := result.Verdict
	if verdict.Admission != protocol.AdmissionValid {
		return invalid("timed result verdict is not admitted")
	}
	if verdict.Case != request.Case || verdict.Probe != request.Probe || verdict.Trial != request.Slot {
		return invalid("timed result does not match pair")
	}
	if request.Probe == protocol.ProbeUnsafe {
		if verdict.Correctness == protocol.OutcomePass && verdict.Safety == protocol.OutcomePass && verdict.Liveness == protocol.OutcomePass {
			return invalid("unsafe control did not distinguish")
		}
		return nil
	}
	if verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass ||
		verdict.Liveness != protocol.OutcomePass || verdict.Diagnosability != protocol.OutcomePass {
		return invalid("unfaulted or protected result failed parity")
	}
	return nil
}

func PairDirectoryName(pairID string) string {
	hash := sha256.Sum256([]byte(pairID))
	return "pair-" + hex.EncodeToString(hash[:16])
}
