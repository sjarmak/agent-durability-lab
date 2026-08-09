package livecommon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/agentsim"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

// Config identifies one live common-harness trial and its simulator binary.
type Config struct {
	Root           string
	Case           protocol.CaseID
	Probe          protocol.Probe
	Trial          int
	AgentBinary    string
	AdapterID      string
	AdapterVersion string
	SystemID       string
	Native         []protocol.NativeRecord
	Settings       map[string]string
}

type harness struct {
	ctx         context.Context
	config      Config
	runID       string
	workDir     string
	store       *workstore.Store
	coordinator *failureinject.Coordinator
	server      *httptest.Server
	launcher    *agentprocess.Launcher
	recorder    *recorder
	processes   map[uint64]launchedAgent
	controls    []agentprocess.ControlResult
}

type launchedAgent struct {
	actor   string
	lease   workstore.Lease
	process agentprocess.Process
}

// Run executes one live trial and writes raw evidence without evaluating it.
func Run(ctx context.Context, config Config) (string, error) {
	if err := validateConfig(config); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	h, err := newHarness(ctx, config)
	if err != nil {
		return "", err
	}
	succeeded := false
	defer func() {
		h.close()
		if succeeded {
			_ = os.RemoveAll(h.workDir)
		}
	}()

	if err := h.execute(); err != nil {
		return "", fmt.Errorf("run live case (work preserved at %s): %w", h.workDir, err)
	}
	runDir, err := h.writeEvidence()
	if err != nil {
		return runDir, err
	}
	succeeded = true
	return runDir, nil
}

func (h *harness) execute() error {
	if h.config.Probe == protocol.ProbeUnfaulted {
		return h.runUnfaulted()
	}
	switch h.config.Case {
	case protocol.CaseSurvivingExecutor:
		return h.runSurvivingExecutor()
	case protocol.CaseAmbiguousEffect:
		return h.runAmbiguousEffect()
	case protocol.CaseStaleGeneration:
		return h.runStaleGeneration()
	case protocol.CaseCancellationUnreachable:
		return h.runCancellationUnreachable()
	default:
		return fmt.Errorf("%w: unsupported live case", protocol.ErrInvalidEvidence)
	}
}

func (h *harness) writeEvidence() (string, error) {
	snapshot, err := h.store.Snapshot(h.ctx, h.recorder.identity.SessionID)
	if err != nil {
		return "", fmt.Errorf("snapshot live authority: %w", err)
	}
	native, err := h.nativeRecords(snapshot)
	if err != nil {
		return "", err
	}
	input, err := h.effectiveInput()
	if err != nil {
		return "", err
	}
	if err := h.validateCapture(snapshot); err != nil {
		return "", err
	}
	return evidence.WriteRun(h.ctx, h.config.Root, h.recorder.bundle(native, input))
}

func validateConfig(config Config) error {
	if config.Root == "" || !config.Case.Valid() || !config.Probe.Valid() || config.Trial < 1 ||
		config.AgentBinary == "" || config.AdapterVersion == "" {
		return fmt.Errorf("%w: live root, case, probe, trial, agent binary, and adapter version are required", protocol.ErrInvalidEvidence)
	}
	info, err := os.Stat(config.AgentBinary)
	if err != nil {
		return fmt.Errorf("inspect agent binary: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%w: agent binary is not executable", protocol.ErrInvalidEvidence)
	}
	return nil
}

func newHarness(ctx context.Context, config Config) (*harness, error) {
	runID := fmt.Sprintf("%s-%s-live-trial-%d", config.Case, config.Probe, config.Trial)
	if err := os.MkdirAll(config.Root, 0o750); err != nil {
		return nil, fmt.Errorf("create live evidence root: %w", err)
	}
	workDir, err := os.MkdirTemp(config.Root, "."+runID+"-work-")
	if err != nil {
		return nil, fmt.Errorf("create live work directory: %w", err)
	}
	store, err := workstore.Open(filepath.Join(workDir, "work.db"))
	if err != nil {
		return nil, err
	}
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	identity := evidence.RunIdentity{
		RunID: runID, Case: config.Case, Probe: config.Probe,
		Trial: config.Trial, SessionID: "session-" + runID,
	}
	return &harness{
		ctx: ctx, config: config, runID: runID, workDir: workDir, store: store,
		coordinator: coordinator, server: server,
		launcher:  agentprocess.NewLauncher(config.AgentBinary, filepath.Join(workDir, "agents")),
		recorder:  newRecorder(identity),
		processes: make(map[uint64]launchedAgent),
	}, nil
}

func (h *harness) close() {
	for _, agent := range h.processes {
		identity := processIdentity(agent.process)
		disposition, err := agentprocess.Probe(identity)
		if err == nil && disposition == agentprocess.DispositionAlive {
			_, _ = agentprocess.Signal(agentprocess.ControlRequest{
				Target: controlTarget(agent), Scope: agentprocess.ScopeProcessTree,
				Signal: agentprocess.SignalKill,
			})
		}
	}
	h.server.Close()
}

func (h *harness) effectiveInput() (protocol.EffectiveInput, error) {
	hash, err := protocol.FileSHA256(h.config.AgentBinary)
	if err != nil {
		return protocol.EffectiveInput{}, fmt.Errorf("hash agent binary: %w", err)
	}
	adapterID := h.config.AdapterID
	if adapterID == "" {
		adapterID = "live-common"
	}
	settings := map[string]string{
		"fault_selection": "named-barrier", "process_identity": "pid-start-process-group",
		"probe": string(h.config.Probe), "store": "bbolt-live-common-v1",
	}
	if h.config.SystemID != "" {
		settings["system_id"] = h.config.SystemID
	}
	for name, value := range h.config.Settings {
		settings[name] = value
	}
	return protocol.EffectiveInput{
		AdapterID: adapterID, AdapterVersion: h.config.AdapterVersion, AgentProtocol: protocol.AgentProtocol,
		AgentBinarySHA256: hash, AuthorityProtocol: protocol.AuthorityProtocol,
		DestinationProtocol: protocol.DestinationProtocol, DestinationID: h.recorder.destination.DestinationID,
		FailureProtocol: protocol.FailureProtocol, OracleProtocol: protocol.OracleProtocol,
		OracleVisibility: []string{
			protocol.AuthorityStateFile, protocol.DestinationStateFile,
			protocol.FaultBoundaryFile, protocol.ProcessObservationsFile,
		},
		Runtime:  runtime.GOOS + "/" + runtime.GOARCH,
		Settings: settings,
	}, nil
}

func (h *harness) nativeRecords(snapshot workstore.Snapshot) ([]protocol.NativeRecord, error) {
	records := append([]protocol.NativeRecord(nil), h.config.Native...)
	for index := range records {
		records[index].Sequence = uint64(index + 1)
	}
	for _, event := range snapshot.Events {
		data, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("encode native event %d: %w", event.Sequence, err)
		}
		records = append(records, protocol.NativeRecord{
			Sequence: uint64(len(records) + 1), Kind: "workstore_event", Detail: string(data),
		})
	}
	for _, control := range h.controls {
		data, err := json.Marshal(control)
		if err != nil {
			return nil, fmt.Errorf("encode process control: %w", err)
		}
		records = append(records, protocol.NativeRecord{
			Sequence: uint64(len(records) + 1), Kind: "process_control", Detail: string(data),
		})
	}
	return records, nil
}

func (h *harness) validateCapture(snapshot workstore.Snapshot) error {
	if snapshot.SessionID != h.recorder.identity.SessionID || snapshot.ActiveGeneration != h.recorder.authority.ActiveGeneration {
		return fmt.Errorf("%w: native authority identity disagrees with common evidence", protocol.ErrInvalidEvidence)
	}
	if (snapshot.Cancellation != nil) != h.recorder.authority.CancellationCommitted {
		return fmt.Errorf("%w: native cancellation disagrees with common evidence", protocol.ErrInvalidEvidence)
	}
	if err := validateEffects(snapshot.Effects, h.recorder.destination.Attempts); err != nil {
		return err
	}
	if (snapshot.Outcome != nil) != (len(h.recorder.authority.AcceptedOutcomes) == 1) {
		return fmt.Errorf("%w: native outcome disagrees with authority evidence", protocol.ErrInvalidEvidence)
	}
	return nil
}

func validateEffects(effects []workstore.AcceptedEffect, attempts []protocol.DestinationAttempt) error {
	applied := make([]protocol.DestinationAttempt, 0, len(attempts))
	for _, attempt := range attempts {
		if attempt.Applied {
			applied = append(applied, attempt)
		}
	}
	if len(effects) != len(applied) {
		return fmt.Errorf("%w: native effect count disagrees with destination evidence", protocol.ErrInvalidEvidence)
	}
	for index, effect := range effects {
		if effect.ID != applied[index].LogicalEffectID || effect.Generation != applied[index].Generation {
			return fmt.Errorf("%w: native effect identity disagrees with destination evidence", protocol.ErrInvalidEvidence)
		}
	}
	return nil
}

func processIdentity(process agentprocess.Process) agentprocess.ProcessIdentity {
	return agentprocess.ProcessIdentity{
		PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
	}
}

func processIdentityString(process agentprocess.Process) string {
	return fmt.Sprintf("pid:%d:start:%s", process.PID, process.StartIdentity)
}

func controlTarget(agent launchedAgent) agentprocess.ControlTarget {
	identity := processIdentity(agent.process)
	return agentprocess.ControlTarget{
		SessionID: agent.lease.SessionID, Generation: agent.lease.Generation,
		OwnerTokenHash: workstore.HashToken(agent.lease.OwnerToken),
		Leader:         identity, Members: []agentprocess.ProcessIdentity{identity},
	}
}

func stableHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

const conditionPollInterval = 10 * time.Millisecond

func (h *harness) waitForSnapshot(predicate func(workstore.Snapshot) bool) (workstore.Snapshot, error) {
	ticker := time.NewTicker(conditionPollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := h.store.Snapshot(h.ctx, h.recorder.identity.SessionID)
		if err != nil {
			return workstore.Snapshot{}, err
		}
		if predicate(snapshot) {
			return snapshot, nil
		}
		select {
		case <-h.ctx.Done():
			return workstore.Snapshot{}, h.ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *harness) waitGone(agent launchedAgent) error {
	ticker := time.NewTicker(conditionPollInterval)
	defer ticker.Stop()
	for {
		disposition, err := agentprocess.Probe(processIdentity(agent.process))
		if err == nil && disposition == agentprocess.DispositionGone {
			return nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *harness) launch(decision workstore.Decision, actor string, bypassAuthority bool) (launchedAgent, error) {
	process, err := h.launcher.Launch(agentprocess.LaunchRequest{
		StorePath: h.store.Path(), BarrierURL: h.server.URL,
		Config: agentsim.Config{
			Lease: decision.Lease, ActorID: actor,
			Effect:                   workstore.Effect{ID: "effect-1", Value: "live mutation"},
			Outcome:                  workstore.Outcome{Value: "live outcome", ArtifactRef: "artifact://live-common"},
			BypassAuthorityForEffect: bypassAuthority,
		},
	})
	if err != nil {
		return launchedAgent{}, err
	}
	agent := launchedAgent{actor: actor, lease: decision.Lease, process: process}
	h.processes[decision.Lease.Generation] = agent
	point := fmt.Sprintf("before-effect/%d", decision.Lease.Generation)
	arrivals, err := h.coordinator.WaitForArrivals(h.ctx, point, 1)
	if err != nil {
		return launchedAgent{}, err
	}
	arrival := arrivals[0]
	if arrival.PID != process.PID || arrival.ProcessStart != process.StartIdentity || arrival.ActorID != actor {
		return launchedAgent{}, fmt.Errorf("%w: barrier arrival targeted the wrong process", protocol.ErrInvalidEvidence)
	}
	return agent, nil
}

func (h *harness) start(mode workstore.Mode, attempt int32, replace bool) (workstore.Decision, error) {
	return h.store.StartOrAttach(h.ctx, workstore.StartRequest{
		SessionID: h.recorder.identity.SessionID, Mode: mode,
		CandidateOwner: fmt.Sprintf("owner-%d-%s", attempt, stableHash(h.runID)[:8]),
		WorkerID:       fmt.Sprintf("worker-%d", attempt), Attempt: attempt, ReplaceOwner: replace,
	})
}

func (h *harness) release(point string) error {
	if err := h.coordinator.Release(point); err != nil {
		return fmt.Errorf("release %s: %w", point, err)
	}
	return nil
}

func (h *harness) signal(agent launchedAgent, signal agentprocess.ControlSignal) error {
	result, err := agentprocess.Signal(agentprocess.ControlRequest{
		Target: controlTarget(agent), Scope: agentprocess.ScopeProcessTree, Signal: signal,
	})
	if err != nil {
		return err
	}
	h.controls = append(h.controls, result)
	return nil
}
