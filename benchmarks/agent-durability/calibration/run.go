package calibration

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

type Config struct {
	Root  string
	Case  protocol.CaseID
	Probe protocol.Probe
	Trial int
}

type rawRun struct {
	manifest    protocol.Manifest
	events      []protocol.Event
	authority   protocol.AuthorityState
	destination protocol.DestinationState
	fault       protocol.FaultBoundary
	processes   []protocol.ProcessObservation
	native      []protocol.NativeRecord
	input       protocol.EffectiveInput
}

func Run(ctx context.Context, config Config) (string, error) {
	if err := validateConfig(config); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	runID := fmt.Sprintf("%s-%s-trial-%d", config.Case, config.Probe, config.Trial)
	runDir := filepath.Join(config.Root, runID)
	if err := os.MkdirAll(config.Root, 0o750); err != nil {
		return "", fmt.Errorf("create evidence root: %w", err)
	}
	if err := os.Mkdir(runDir, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return runDir, fmt.Errorf("%w: %s", protocol.ErrEvidenceExists, runDir)
		}
		return runDir, fmt.Errorf("create run evidence: %w", err)
	}

	raw := buildRun(config, runID)
	if err := writeRawEvidence(ctx, runDir, raw); err != nil {
		return runDir, err
	}
	return runDir, nil
}

func validateConfig(config Config) error {
	if config.Root == "" || !config.Case.Valid() || !config.Probe.Valid() || config.Trial < 1 {
		return fmt.Errorf("%w: calibration root, case, probe, and positive trial are required", protocol.ErrInvalidEvidence)
	}
	return nil
}

func buildRun(config Config, runID string) rawRun {
	sessionID := "session-" + runID
	builder := newRunBuilder(config.Trial, sessionID)
	if config.Probe == protocol.ProbeUnfaulted {
		builder.unfaulted()
	} else {
		switch config.Case {
		case protocol.CaseSurvivingExecutor:
			builder.survivingExecutor(config.Probe)
		case protocol.CaseAmbiguousEffect:
			builder.ambiguousEffect(config.Probe)
		case protocol.CaseStaleGeneration:
			builder.staleGeneration(config.Probe)
		case protocol.CaseCancellationUnreachable:
			builder.cancellationUnreachable(config.Probe)
		}
	}
	input := protocol.EffectiveInput{
		AdapterID: "calibration", AdapterVersion: "v1", AgentProtocol: protocol.AgentProtocol,
		AgentBinarySHA256:   hashString("deterministic-calibration-agent-v1"),
		AuthorityProtocol:   protocol.AuthorityProtocol,
		DestinationProtocol: protocol.DestinationProtocol, DestinationID: "calibration-destination",
		FailureProtocol: protocol.FailureProtocol, OracleProtocol: protocol.OracleProtocol,
		OracleVisibility: []string{protocol.AuthorityStateFile, protocol.DestinationStateFile, protocol.FaultBoundaryFile, protocol.ProcessObservationsFile},
		Runtime:          runtime.GOOS + "/" + runtime.GOARCH,
		Settings:         map[string]string{"clock": "deterministic", "retry": "case-defined", "timeout": "test-context-only"},
	}
	return rawRun{
		manifest: protocol.Manifest{ContractVersion: protocol.ContractVersion, RunID: runID, Case: config.Case, Probe: config.Probe, Trial: config.Trial, SessionID: sessionID},
		events:   builder.events, authority: builder.authority, destination: builder.destination,
		fault: builder.fault, processes: builder.processes, native: builder.native, input: input,
	}
}

type runBuilder struct {
	now         time.Time
	sessionID   string
	events      []protocol.Event
	authority   protocol.AuthorityState
	destination protocol.DestinationState
	fault       protocol.FaultBoundary
	processes   []protocol.ProcessObservation
	native      []protocol.NativeRecord
}

func newRunBuilder(trial int, sessionID string) *runBuilder {
	return &runBuilder{
		now: time.Date(2026, 8, 7, 0, trial, 0, 0, time.UTC), sessionID: sessionID,
		authority:   protocol.AuthorityState{SessionID: sessionID, ActiveGeneration: 1, ConcurrentOwnerCount: 1, CurrentOwnerAlive: true},
		destination: protocol.DestinationState{DestinationID: "calibration-destination"},
	}
}

func (b *runBuilder) add(kind, actor string, generation uint64, process, logicalEffect, attempt, decision string) uint64 {
	sequence := uint64(len(b.events) + 1)
	b.events = append(b.events, protocol.Event{
		Sequence: sequence, Time: b.now.Add(time.Duration(sequence) * time.Millisecond).Format(time.RFC3339Nano),
		Kind: kind, SessionID: b.sessionID, ActorID: actor, Generation: generation, ProcessIdentity: process,
		LogicalEffectID: logicalEffect, PhysicalAttemptID: attempt, Decision: decision,
	})
	return sequence
}

func (b *runBuilder) observe(sequence uint64, actor string, generation uint64, process, state string) {
	b.processes = append(b.processes, protocol.ProcessObservation{Sequence: sequence, ActorID: actor, Generation: generation, ProcessIdentity: process, State: state})
}

func (b *runBuilder) unfaulted() {
	process := "pid:101:start:fixture"
	registered := b.add(protocol.EventExecutorRegistered, "agent-1", 1, process, "", "", "observed")
	b.observe(registered, "agent-1", 1, process, "running")
	effect := b.add(protocol.EventEffectAccepted, "agent-1", 1, process, "effect-1", "attempt-1", "accepted")
	b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{LogicalEffectID: "effect-1", PhysicalAttemptID: "attempt-1", Generation: 1, Sequence: effect, Applied: true})
	outcome := b.add(protocol.EventOutcomeAccepted, "agent-1", 1, process, "", "", "accepted")
	b.authority.AcceptedActions = append(b.authority.AcceptedActions, protocol.AcceptedAction{Kind: "effect", Generation: 1, Sequence: effect})
	b.authority.AcceptedOutcomes = append(b.authority.AcceptedOutcomes, protocol.AcceptedAction{Kind: "outcome", Generation: 1, Sequence: outcome})
	b.native = []protocol.NativeRecord{{Sequence: 1, Kind: "start", Detail: "calibration"}, {Sequence: 2, Kind: "complete", Detail: "ok"}}
}

func (b *runBuilder) survivingExecutor(probe protocol.Probe) {
	first := "pid:101:start:fixture"
	registered := b.add(protocol.EventExecutorRegistered, "agent-1", 1, first, "", "", "observed")
	b.observe(registered, "agent-1", 1, first, "running")
	barrier := b.add(protocol.EventBarrierReached, "agent-1", 1, first, "", "", "blocked")
	if probe == protocol.ProbeUnsafe {
		second := "pid:202:start:fixture"
		competitor := b.add(protocol.EventExecutorRegistered, "agent-2", 2, second, "", "", "observed")
		b.observe(competitor, "agent-2", 2, second, "running")
		b.authority.ActiveGeneration = 2
		b.authority.ConcurrentOwnerCount = 2
		b.acceptEffect("agent-1", 1, first, "attempt-1")
		b.acceptEffect("agent-2", 2, second, "attempt-2")
		b.acceptOutcome("agent-1", 1, first)
		b.acceptOutcome("agent-2", 2, second)
	} else {
		attached := b.add(protocol.EventExecutorAttached, "agent-1", 1, first, "", "", "observed")
		b.observe(attached, "agent-1", 1, first, "reattached")
		b.acceptEffect("agent-1", 1, first, "attempt-1")
		b.acceptOutcome("agent-1", 1, first)
	}
	b.fault = b.triggeredFault("worker-died-after-agent-registration", barrier, barrier+1, "agent-1", first)
	b.native = []protocol.NativeRecord{{Sequence: 1, Kind: "executor_lost", Detail: "after registration"}, {Sequence: 2, Kind: "recovery", Detail: string(probe)}}
}

func (b *runBuilder) ambiguousEffect(probe protocol.Probe) {
	process := "pid:101:start:fixture"
	registered := b.add(protocol.EventExecutorRegistered, "agent-1", 1, process, "", "", "observed")
	b.observe(registered, "agent-1", 1, process, "running")
	first := b.acceptEffect("agent-1", 1, process, "attempt-1")
	if probe == protocol.ProbeUnsafe {
		b.acceptEffect("agent-1", 1, process, "attempt-2")
	} else {
		sequence := b.add(protocol.EventEffectRejected, "agent-1", 1, process, "effect-1", "attempt-2", "duplicate")
		b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{LogicalEffectID: "effect-1", PhysicalAttemptID: "attempt-2", Generation: 1, Sequence: sequence, Applied: false})
	}
	b.acceptOutcome("agent-1", 1, process)
	b.fault = b.triggeredFault("effect-confirmed-before-step-completion", first, first+1, "agent-1", process)
	b.native = []protocol.NativeRecord{{Sequence: 1, Kind: "step_attempt", Detail: "completion lost"}, {Sequence: 2, Kind: "step_retry", Detail: string(probe)}}
}

func (b *runBuilder) staleGeneration(probe protocol.Probe) {
	first := "pid:101:start:fixture"
	second := "pid:202:start:fixture"
	registered := b.add(protocol.EventExecutorRegistered, "agent-1", 1, first, "", "", "observed")
	b.observe(registered, "agent-1", 1, first, "running")
	replaced := b.add(protocol.EventOwnerReplaced, "agent-2", 2, second, "", "", "accepted")
	b.observe(replaced, "agent-2", 2, second, "running")
	b.authority.ActiveGeneration = 2
	if probe == protocol.ProbeUnsafe {
		b.acceptEffect("agent-1", 1, first, "stale-attempt")
		completion := b.add(protocol.EventStaleCompletion, "agent-1", 1, first, "", "", "accepted")
		stop := b.add(protocol.EventStaleStop, "agent-1", 1, first, "", "", "accepted")
		b.authority.AcceptedActions = append(b.authority.AcceptedActions,
			protocol.AcceptedAction{Kind: "stale-completion", Generation: 1, Sequence: completion},
			protocol.AcceptedAction{Kind: "stale-stop", Generation: 1, Sequence: stop},
		)
		b.authority.CurrentOwnerAlive = false
	} else {
		sequence := b.add(protocol.EventEffectRejected, "agent-1", 1, first, "effect-1", "stale-attempt", "stale_generation")
		b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{LogicalEffectID: "effect-1", PhysicalAttemptID: "stale-attempt", Generation: 1, Sequence: sequence, Applied: false})
		b.add(protocol.EventStaleCompletion, "agent-1", 1, first, "", "", "rejected")
		b.add(protocol.EventStaleStop, "agent-1", 1, first, "", "", "rejected")
	}
	b.acceptOutcome("agent-2", 2, second)
	b.fault = b.triggeredFault("replacement-committed-before-stale-actions", replaced, replaced+1, "agent-1", first)
	b.native = []protocol.NativeRecord{{Sequence: 1, Kind: "replace", Detail: "generation 2"}, {Sequence: 2, Kind: "deliver_stale", Detail: string(probe)}}
}

func (b *runBuilder) cancellationUnreachable(probe protocol.Probe) {
	process := "pid:101:start:fixture"
	registered := b.add(protocol.EventExecutorRegistered, "agent-1", 1, process, "", "", "observed")
	b.observe(registered, "agent-1", 1, process, "frozen")
	canceled := b.add(protocol.EventCancellationCommitted, "", 0, "", "", "", "accepted")
	b.authority.CancellationCommitted = true
	b.authority.CancellationSequence = canceled
	b.authority.CurrentOwnerAlive = false
	if probe == protocol.ProbeUnsafe {
		b.acceptEffect("agent-1", 1, process, "post-cancel-attempt")
		second := "pid:202:start:fixture"
		replacement := b.add(protocol.EventExecutorRegistered, "agent-2", 2, second, "", "", "accepted")
		b.observe(replacement, "agent-2", 2, second, "running")
		b.authority.ActiveGeneration = 2
		b.authority.ReplacementAfterCancellation = true
	} else {
		sequence := b.add(protocol.EventEffectRejected, "agent-1", 1, process, "effect-1", "post-cancel-attempt", "canceled")
		b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{LogicalEffectID: "effect-1", PhysicalAttemptID: "post-cancel-attempt", Generation: 1, Sequence: sequence, Applied: false})
		b.add(protocol.EventReplacementRejected, "agent-2", 2, "pid:202:start:fixture", "", "", "canceled")
	}
	b.fault = b.triggeredFault("process-frozen-before-cancellation", registered, canceled, "agent-1", process)
	b.native = []protocol.NativeRecord{{Sequence: 1, Kind: "cancel", Detail: "authority revoked"}, {Sequence: 2, Kind: "process_resume", Detail: string(probe)}}
}

func (b *runBuilder) acceptEffect(actor string, generation uint64, process, attempt string) uint64 {
	sequence := b.add(protocol.EventEffectAccepted, actor, generation, process, "effect-1", attempt, "accepted")
	b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{LogicalEffectID: "effect-1", PhysicalAttemptID: attempt, Generation: generation, Sequence: sequence, Applied: true})
	b.authority.AcceptedActions = append(b.authority.AcceptedActions, protocol.AcceptedAction{Kind: "effect", Generation: generation, Sequence: sequence})
	return sequence
}

func (b *runBuilder) acceptOutcome(actor string, generation uint64, process string) {
	sequence := b.add(protocol.EventOutcomeAccepted, actor, generation, process, "", "", "accepted")
	b.authority.AcceptedOutcomes = append(b.authority.AcceptedOutcomes, protocol.AcceptedAction{Kind: "outcome", Generation: generation, Sequence: sequence})
}

func (b *runBuilder) triggeredFault(point string, after, before uint64, actor, process string) protocol.FaultBoundary {
	return protocol.FaultBoundary{Point: point, Triggered: true, AfterSequence: after, BeforeSequence: before, ActorID: actor, ProcessIdentity: process, TriggeredAt: b.now.Add(time.Duration(after)*time.Millisecond + time.Microsecond).Format(time.RFC3339Nano)}
}

func writeRawEvidence(ctx context.Context, runDir string, raw rawRun) error {
	if err := raw.input.Validate(); err != nil {
		return err
	}
	files := map[string]any{
		protocol.AuthorityStateFile:      raw.authority,
		protocol.DestinationStateFile:    raw.destination,
		protocol.FaultBoundaryFile:       raw.fault,
		protocol.NativeJournalFile:       raw.native,
		protocol.ProcessObservationsFile: raw.processes,
		protocol.EffectiveInputFile:      raw.input,
	}
	for name, value := range files {
		if err := writeJSONExclusive(ctx, filepath.Join(runDir, name), value); err != nil {
			return err
		}
	}
	if err := writeJSONLExclusive(ctx, filepath.Join(runDir, protocol.CommonEventsFile), raw.events); err != nil {
		return err
	}
	names := make([]string, 0, len(files)+1)
	for name := range files {
		names = append(names, name)
	}
	names = append(names, protocol.CommonEventsFile)
	sort.Strings(names)
	raw.manifest.EvidenceSHA256 = make(map[string]string, len(names))
	for _, name := range names {
		hash, err := protocol.FileSHA256(filepath.Join(runDir, name))
		if err != nil {
			return fmt.Errorf("hash %s: %w", name, err)
		}
		raw.manifest.EvidenceSHA256[name] = hash
	}
	raw.manifest.InputSHA256 = raw.manifest.EvidenceSHA256[protocol.EffectiveInputFile]
	if err := raw.manifest.Validate(); err != nil {
		return err
	}
	return writeJSONExclusive(ctx, filepath.Join(runDir, protocol.ManifestFile), raw.manifest)
}

func writeJSONExclusive(ctx context.Context, path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	return writeExclusive(ctx, path, append(data, '\n'))
}

func writeJSONLExclusive(ctx context.Context, path string, events []protocol.Event) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return evidenceWriteError(path, err)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			_ = file.Close()
			return err
		}
		if err := encoder.Encode(event); err != nil {
			_ = file.Close()
			return fmt.Errorf("encode event: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return fmt.Errorf("flush events: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync events: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close events: %w", err)
	}
	return nil
}

func writeExclusive(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return evidenceWriteError(path, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}
	return nil
}

func evidenceWriteError(path string, err error) error {
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: %s", protocol.ErrEvidenceExists, path)
	}
	return fmt.Errorf("create %s: %w", filepath.Base(path), err)
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
