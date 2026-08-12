package semantics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/agent"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type EpisodeSpec struct {
	RunID              string            `json:"run_id"`
	PairID             string            `json:"pair_id"`
	ScheduleBlockID    string            `json:"schedule_block_id"`
	TrackerBeadID      string            `json:"tracker_bead_id"`
	Topology           protocol.Topology `json:"topology"`
	Case               protocol.CaseID   `json:"case"`
	Boundary           string            `json:"boundary"`
	Probe              protocol.Probe    `json:"probe"`
	Fanout             int               `json:"fanout"`
	WorkTaskQueue      string            `json:"work_task_queue"`
	EffectTaskQueue    string            `json:"effect_task_queue"`
	ParentTaskQueue    string            `json:"parent_task_queue"`
	LogicalOperationID string            `json:"logical_operation_id"`
}

func (s EpisodeSpec) validate() error {
	if s.RunID == "" || s.PairID == "" || s.ScheduleBlockID == "" || s.TrackerBeadID == "" || !s.Topology.Valid() || !s.Case.Valid() ||
		s.Boundary == "" || !s.Probe.Valid() || !slices.Contains([]int{8, 32, 128}, s.Fanout) ||
		s.WorkTaskQueue == "" || s.EffectTaskQueue == "" || s.ParentTaskQueue == "" || s.LogicalOperationID == "" {
		return fmt.Errorf("%w: semantics episode specification", protocol.ErrInvalidEvidence)
	}
	if s.Probe == protocol.ProbeUnfaulted && s.Boundary != protocol.UnfaultedBoundary ||
		s.Probe != protocol.ProbeUnfaulted && s.Boundary == protocol.UnfaultedBoundary {
		return fmt.Errorf("%w: semantics episode boundary", protocol.ErrInvalidEvidence)
	}
	return nil
}

type WorkerTarget string

const (
	WorkerTargetWork   WorkerTarget = "work"
	WorkerTargetEffect WorkerTarget = "effect"
	WorkerTargetNone   WorkerTarget = "none"
)

type FaultRequest struct {
	Target         WorkerTarget
	Boundary       string
	BarrierEventID string
	Identity       protocol.Identity
	release        chan struct{}
}

type oldEffectBoundary struct {
	identity       protocol.Identity
	barrierEventID string
}

type runtimeBarrier struct {
	coordinator *failureinject.Coordinator
	server      *http.Server
	listener    net.Listener
	url         string
	serveDone   chan error
}

func newRuntimeBarrier() (*runtimeBarrier, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	coordinator := failureinject.NewCoordinator()
	server := &http.Server{
		Handler: coordinator.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second,
	}
	barrier := &runtimeBarrier{
		coordinator: coordinator, server: server, listener: listener, url: "http://" + listener.Addr().String(),
		serveDone: make(chan error, 1),
	}
	go func() {
		serveErr := server.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		barrier.serveDone <- serveErr
	}()
	return barrier, nil
}

func (b *runtimeBarrier) close() error {
	if b == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shutdownErr := b.server.Shutdown(ctx)
	var forceCloseErr error
	if shutdownErr != nil {
		forceCloseErr = b.server.Close()
		if errors.Is(forceCloseErr, http.ErrServerClosed) {
			forceCloseErr = nil
		}
		if errors.Is(shutdownErr, context.DeadlineExceeded) && forceCloseErr == nil {
			shutdownErr = nil
		}
	}
	serveErr := <-b.serveDone
	return errors.Join(shutdownErr, forceCloseErr, serveErr)
}

type pendingLaunch struct {
	decision workstore.Decision
	claimed  bool
}

func (p *pendingLaunch) claim() workstore.Decision {
	decision := p.decision
	if p.claimed {
		decision.Action = workstore.ActionAttach
	}
	p.claimed = true
	return decision
}

type controlledProcess struct {
	lease                     workstore.Lease
	process                   agentprocess.Process
	blockedBeforeRegistration bool
}

type checkpointRecord struct {
	receiptID string
	input     CheckpointInput
}

type continuationRecord struct {
	receiptID string
	input     ContinuationInput
}

type EpisodeRuntime struct {
	spec           EpisodeSpec
	manifest       protocol.Manifest
	workRoot       string
	agentBinary    string
	agentSHA256    string
	sourceSHA256   string
	workStores     map[string]*workstore.Store
	recoveryStores map[string]*workstore.Store
	launcher       *agent.Launcher
	destination    *MemoryDestination
	barriers       map[string]*runtimeBarrier
	tokens         map[uint64]string

	mu                   sync.Mutex
	started              chan struct{}
	startOnce            sync.Once
	parentWorkflow       string
	parentRun            string
	startedAt            time.Time
	lastTimestamp        time.Time
	events               []protocol.CausalEvent
	processes            []protocol.ProcessObservation
	controlledProcesses  map[int]controlledProcess
	requests             []protocol.DependencyRequest
	destinationActions   []protocol.DestinationAction
	deliveries           map[string][]Contribution
	contributions        []protocol.ContributionObservation
	checkpoints          []protocol.CheckpointObservation
	checkpointByID       map[string]checkpointRecord
	continuations        []protocol.ContinuationObservation
	continuationByID     map[string]continuationRecord
	supersession         *protocol.SupersessionObservation
	destructive          *protocol.DestructiveObservation
	pending              map[string]*pendingLaunch
	obsoletePending      map[string]*pendingLaunch
	obsoleteLease        workstore.Lease
	oldWorkStarted       bool
	oldProcessLaunched   bool
	oldActionErr         error
	replacementActionErr error
	oldEffectBoundary    oldEffectBoundary
	supersessionReceipt  *SupersedeReceipt
	fault                protocol.FaultBoundary
	faultIdentity        protocol.Identity
	faultCommitted       bool
	recoverySeen         bool
	terminalFailure      bool
	inflight             int
	inflightChanged      chan struct{}

	faultRequests          chan FaultRequest
	oldEffectBoundaryReady chan struct{}
	oldEffectBoundaryOnce  sync.Once
	oldEffectBoundaryGate  sync.Mutex
	supersedeGate          chan struct{}
	supersessionDone       chan struct{}
	supersessionOnce       sync.Once
	replacementDone        chan struct{}
	replacementOnce        sync.Once
	replacementEffectOnce  sync.Once
	oldActionDone          chan struct{}
	oldActionOnce          sync.Once
	oldEffectOnce          sync.Once
	joinRecovery           chan struct{}
	joinRecoveryOnce       sync.Once
	heldRelease            chan struct{}
	heldReleaseOnce        sync.Once
	heldResultDone         chan struct{}
	heldResultOnce         sync.Once
	recovery               *recoveryRuntimeState
}

func NewEpisodeRuntime(spec EpisodeSpec, workRoot, agentBinary string) (*EpisodeRuntime, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if workRoot == "" || agentBinary == "" {
		return nil, fmt.Errorf("%w: semantics runtime paths", protocol.ErrInvalidEvidence)
	}
	agentSHA256, err := fileSHA256(agentBinary)
	if err != nil {
		return nil, fmt.Errorf("hash agent binary: %w", err)
	}
	sourceSHA256, err := runningExecutableSHA256()
	if err != nil {
		return nil, fmt.Errorf("hash semantics executable: %w", err)
	}
	runRoot := filepath.Join(workRoot, "runs", shortDigest(spec.RunID))
	if err := os.MkdirAll(runRoot, 0o750); err != nil {
		return nil, err
	}
	created := time.Now().UTC()
	runtime := &EpisodeRuntime{
		spec: spec,
		manifest: protocol.Manifest{
			ProtocolVersion: protocol.PublicationProtocolVersion, RunID: spec.RunID, PairID: spec.PairID,
			ScheduleBlockID: spec.ScheduleBlockID, TrackerBeadID: spec.TrackerBeadID, Topology: spec.Topology,
			Case: spec.Case, Boundary: spec.Boundary, Probe: spec.Probe, Fanout: spec.Fanout,
			LogicalOperationID: spec.LogicalOperationID, CreatedAtUTC: created.Format(time.RFC3339Nano),
			RequiredEvidence: protocol.RequiredEvidenceFiles(),
		},
		workRoot: runRoot, agentBinary: agentBinary, agentSHA256: agentSHA256, sourceSHA256: sourceSHA256,
		launcher:    agent.NewLauncher(agentBinary, filepath.Join(runRoot, "processes"), runRoot),
		destination: NewMemoryDestination(), barriers: make(map[string]*runtimeBarrier, spec.Fanout),
		tokens:  map[uint64]string{1: spec.PairID + "/owner/1", 2: spec.PairID + "/owner/2"},
		started: make(chan struct{}), startedAt: created, controlledProcesses: make(map[int]controlledProcess), inflightChanged: make(chan struct{}),
		deliveries: make(map[string][]Contribution), checkpointByID: make(map[string]checkpointRecord),
		continuationByID: make(map[string]continuationRecord), pending: make(map[string]*pendingLaunch),
		obsoletePending: make(map[string]*pendingLaunch), workStores: make(map[string]*workstore.Store, spec.Fanout),
		faultRequests: make(chan FaultRequest, 1), oldEffectBoundaryReady: make(chan struct{}), supersedeGate: make(chan struct{}, 1),
		supersessionDone: make(chan struct{}), replacementDone: make(chan struct{}), oldActionDone: make(chan struct{}),
		joinRecovery: make(chan struct{}), heldRelease: make(chan struct{}), heldResultDone: make(chan struct{}),
	}
	if spec.Case.Suite() == protocol.SuiteRecoveryDynamics {
		runtime.recovery, err = newRecoveryRuntimeState(spec)
		if err != nil {
			_ = runtime.Close()
			return nil, err
		}
		runtime.recoveryStores = make(map[string]*workstore.Store, spec.Fanout)
	}
	for index := 1; index <= spec.Fanout; index++ {
		itemID := fmt.Sprintf("item-%03d", index)
		itemStore, storeErr := workstore.Open(filepath.Join(runRoot, "work-stores", itemID+".db"))
		if storeErr != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("open work store for %s: %w", itemID, storeErr)
		}
		runtime.workStores[itemID] = itemStore
		if runtime.recovery != nil {
			recoveryStore, storeErr := workstore.Open(filepath.Join(runRoot, "recovery-stores", itemID+".db"))
			if storeErr != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("open recovery store for %s: %w", itemID, storeErr)
			}
			runtime.recoveryStores[itemID] = recoveryStore
		}
		barrier, barrierErr := newRuntimeBarrier()
		if barrierErr != nil {
			_ = runtime.Close()
			return nil, barrierErr
		}
		runtime.barriers[itemID] = barrier
	}
	initial := Authority{Generation: 1, CapabilityHash: workstore.HashToken(runtime.tokens[1])}
	if err := runtime.destination.SetAuthority("item-001", initial); err != nil {
		_ = runtime.Close()
		return nil, err
	}
	return runtime, nil
}

func (r *EpisodeRuntime) workStore(itemID string) (*workstore.Store, error) {
	store := r.workStores[itemID]
	if store == nil {
		return nil, fmt.Errorf("%w: work store for %s", protocol.ErrInvalidEvidence, itemID)
	}
	return store, nil
}

func (r *EpisodeRuntime) recoveryStore(itemID string) (*workstore.Store, error) {
	store := r.recoveryStores[itemID]
	if store == nil {
		return nil, fmt.Errorf("%w: recovery store for %s", protocol.ErrInvalidEvidence, itemID)
	}
	return store, nil
}

func (r *EpisodeRuntime) recordOldEffectBoundary(boundary oldEffectBoundary) error {
	if boundary.identity.ProcessIdentity == "" || boundary.identity.WorkItemID != "item-001" ||
		boundary.identity.Generation == 0 || boundary.identity.CapabilityHash == "" {
		return fmt.Errorf("%w: supersession effect boundary", protocol.ErrInvalidEvidence)
	}
	if r.oldEffectBoundaryReady == nil {
		return fmt.Errorf("%w: supersession effect boundary signal", protocol.ErrInvalidEvidence)
	}
	r.oldEffectBoundaryGate.Lock()
	defer r.oldEffectBoundaryGate.Unlock()
	return r.recordOldEffectBoundaryLocked(boundary)
}

func (r *EpisodeRuntime) recordOldEffectBoundaryLocked(boundary oldEffectBoundary) error {
	r.mu.Lock()
	existing := r.oldEffectBoundary
	if existing.identity.ProcessIdentity != "" {
		matches := existing.identity.ProcessIdentity == boundary.identity.ProcessIdentity &&
			existing.identity.WorkItemID == boundary.identity.WorkItemID &&
			existing.identity.Generation == boundary.identity.Generation &&
			existing.identity.CapabilityHash == boundary.identity.CapabilityHash &&
			existing.barrierEventID == boundary.barrierEventID
		r.mu.Unlock()
		if !matches {
			return fmt.Errorf("%w: conflicting supersession effect boundary", protocol.ErrInvalidEvidence)
		}
		return nil
	}
	r.oldEffectBoundary = boundary
	r.mu.Unlock()
	r.oldEffectBoundaryOnce.Do(func() { close(r.oldEffectBoundaryReady) })
	return nil
}

func (r *EpisodeRuntime) waitOldEffectBoundary(ctx context.Context) (oldEffectBoundary, error) {
	if r.oldEffectBoundaryReady == nil {
		return oldEffectBoundary{}, fmt.Errorf("%w: supersession effect boundary signal", protocol.ErrInvalidEvidence)
	}
	select {
	case <-ctx.Done():
		return oldEffectBoundary{}, ctx.Err()
	case <-r.oldEffectBoundaryReady:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.oldEffectBoundary.identity.ProcessIdentity == "" {
		return oldEffectBoundary{}, fmt.Errorf("%w: missing supersession effect boundary", protocol.ErrInvalidEvidence)
	}
	return r.oldEffectBoundary, nil
}

func (r *EpisodeRuntime) Input() ParentInput {
	items := make([]Item, r.spec.Fanout)
	for index := range items {
		items[index] = Item{ID: fmt.Sprintf("item-%03d", index+1), Ordinal: index + 1}
	}
	return ParentInput{
		ProtocolVersion: protocol.PublicationProtocolVersion, PairID: r.spec.PairID,
		LogicalOperationID: r.spec.LogicalOperationID, WorkTaskQueue: r.spec.WorkTaskQueue,
		EffectTaskQueue: r.spec.EffectTaskQueue, Topology: r.spec.Topology, Case: r.spec.Case,
		Boundary: r.spec.Boundary, Probe: r.spec.Probe, Items: items,
		InitialAuthority:     Authority{Generation: 1, CapabilityHash: workstore.HashToken(r.tokens[1])},
		ReplacementAuthority: Authority{Generation: 2, CapabilityHash: workstore.HashToken(r.tokens[2])},
	}
}

func (r *EpisodeRuntime) Start(parentWorkflowID, parentRunID string) error {
	if parentWorkflowID == "" || parentRunID == "" {
		return fmt.Errorf("%w: parent execution identity", protocol.ErrInvalidEvidence)
	}
	var startErr error
	r.startOnce.Do(func() {
		r.mu.Lock()
		r.parentWorkflow, r.parentRun = parentWorkflowID, parentRunID
		if r.recovery == nil {
			for index := range r.spec.Fanout {
				itemID := fmt.Sprintf("item-%03d", index+1)
				identity := protocol.Identity{
					ProtocolVersion: protocol.PublicationProtocolVersion, RunID: r.spec.RunID, PairID: r.spec.PairID,
					ScheduleBlockID: r.spec.ScheduleBlockID, TrackerBeadID: r.manifest.TrackerBeadID, Topology: r.spec.Topology,
					Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
					LogicalOperationID: r.spec.LogicalOperationID, WorkItemID: itemID,
					Generation: 1, CapabilityHash: workstore.HashToken(r.tokens[1]),
					ParentWorkflowID: parentWorkflowID, ParentRunID: parentRunID,
					ActivityID: fmt.Sprintf("work/%s/generation-1", itemID), ActivityAttempt: 1,
					WorkerID: "topology-benchmark-caller", WorkerPID: os.Getpid(), ProcessIdentity: fmt.Sprintf("caller:pid:%d", os.Getpid()),
				}
				r.appendEventLocked(identity, protocol.EventActivityScheduled, protocol.DecisionObserved, nil)
			}
		}
		r.mu.Unlock()
		close(r.started)
	})
	select {
	case <-r.started:
		r.mu.Lock()
		if r.parentWorkflow != parentWorkflowID || r.parentRun != parentRunID {
			startErr = fmt.Errorf("%w: parent execution changed", protocol.ErrInvalidEvidence)
		}
		r.mu.Unlock()
	default:
	}
	return startErr
}

func (r *EpisodeRuntime) waitStarted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.started:
		return nil
	}
}

func (r *EpisodeRuntime) beginActivity() {
	r.mu.Lock()
	r.inflight++
	close(r.inflightChanged)
	r.inflightChanged = make(chan struct{})
	r.mu.Unlock()
}

func (r *EpisodeRuntime) endActivity() {
	r.mu.Lock()
	r.inflight--
	close(r.inflightChanged)
	r.inflightChanged = make(chan struct{})
	r.mu.Unlock()
}

func (r *EpisodeRuntime) WaitIdle(ctx context.Context) error {
	for {
		r.mu.Lock()
		if r.inflight == 0 {
			r.mu.Unlock()
			return nil
		}
		changed := r.inflightChanged
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (r *EpisodeRuntime) FaultRequests() <-chan FaultRequest { return r.faultRequests }

func (r *EpisodeRuntime) CommitFault(request FaultRequest) error {
	if request.BarrierEventID == "" || request.Boundary != r.spec.Boundary {
		return fmt.Errorf("%w: fault request", protocol.ErrInvalidEvidence)
	}
	r.mu.Lock()
	if r.faultCommitted {
		r.mu.Unlock()
		return fmt.Errorf("%w: duplicate fault commitment", protocol.ErrInvalidEvidence)
	}
	r.faultCommitted = true
	faultID := r.appendEventLocked(request.Identity, protocol.EventFaultCommitted, protocol.DecisionAccepted, nil, request.BarrierEventID)
	r.fault = protocol.FaultBoundary{
		RunID: r.spec.RunID, Injected: true, ExpectedBoundary: r.spec.Boundary,
		BarrierEventID: request.BarrierEventID, FaultEventID: faultID,
		TargetProcessIdentity: request.Identity.ProcessIdentity,
	}
	r.faultIdentity = request.Identity
	r.mu.Unlock()
	if request.release != nil {
		close(request.release)
	}
	return nil
}

func (r *EpisodeRuntime) reachFault(ctx context.Context, identity protocol.Identity, target WorkerTarget) error {
	r.mu.Lock()
	if r.fault.Injected || r.faultCommitted {
		r.mu.Unlock()
		return nil
	}
	barrierID := r.appendEventLocked(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, nil)
	r.processes = append(r.processes, protocol.ProcessObservation{
		EventID: barrierID, WorkItemID: identity.WorkItemID, WorkerID: identity.WorkerID,
		WorkerPID: identity.WorkerPID, ProcessIdentity: identity.ProcessIdentity, State: "blocked-at-" + r.spec.Boundary,
	})
	release := make(chan struct{})
	request := FaultRequest{Target: target, Boundary: r.spec.Boundary, BarrierEventID: barrierID, Identity: identity, release: release}
	r.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.faultRequests <- request:
	}
	if target == WorkerTargetNone {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (r *EpisodeRuntime) recordRecovery(identity protocol.Identity) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.faultCommitted || r.recoverySeen {
		return
	}
	r.appendEventLocked(identity, protocol.EventRecoveryObserved, protocol.DecisionObserved, nil, r.fault.FaultEventID)
	r.recoverySeen = true
	r.joinRecoveryOnce.Do(func() { close(r.joinRecovery) })
}

func (r *EpisodeRuntime) appendEvent(identity protocol.Identity, kind, decision string, details map[string]string, extraParents ...string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.appendEventLocked(identity, kind, decision, details, extraParents...)
}

func (r *EpisodeRuntime) appendEventLocked(identity protocol.Identity, kind, decision string, details map[string]string, extraParents ...string) string {
	if len(r.events) == 0 {
		r.appendRawEventLocked(identity, protocol.EventInputRegistered, protocol.DecisionObserved, nil, nil)
	}
	parents := []string{r.events[len(r.events)-1].EventID}
	for _, parent := range extraParents {
		if parent != "" && !slices.Contains(parents, parent) {
			parents = append(parents, parent)
		}
	}
	return r.appendRawEventLocked(identity, kind, decision, details, parents)
}

func (r *EpisodeRuntime) appendRawEventLocked(identity protocol.Identity, kind, decision string, details map[string]string, parents []string) string {
	now := time.Now().UTC()
	if now.Before(r.lastTimestamp) {
		now = r.lastTimestamp
	}
	r.lastTimestamp = now
	eventID := fmt.Sprintf("event-%06d", len(r.events)+1)
	event := protocol.CausalEvent{
		Identity: identity, Sequence: uint64(len(r.events) + 1), EventID: eventID,
		ParentEventIDs: slices.Clone(parents), TimestampUTC: now.Format(time.RFC3339Nano),
		MonotonicOffsetNS: time.Since(r.startedAt).Nanoseconds(), Kind: kind, Decision: decision,
		Details: cloneStringMap(details),
	}
	r.events = append(r.events, event)
	return eventID
}

func (r *EpisodeRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	processes := make([]controlledProcess, 0, len(r.controlledProcesses))
	for _, process := range r.controlledProcesses {
		processes = append(processes, process)
	}
	barriers := make([]*runtimeBarrier, 0, len(r.barriers))
	for _, barrier := range r.barriers {
		barriers = append(barriers, barrier)
	}
	r.mu.Unlock()
	var result error
	for _, process := range processes {
		if err := cleanupControlledProcess(process); err != nil {
			result = errors.Join(result, fmt.Errorf("cleanup controlled process %d: %w", process.process.PID, err))
		}
	}
	for index, barrier := range barriers {
		if err := barrier.close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close runtime barrier %d: %w", index+1, err))
		}
	}
	if r.recovery != nil && r.recovery.dependency != nil {
		if err := r.recovery.dependency.close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close recovery dependency: %w", err))
		}
	}
	return result
}

func cleanupControlledProcess(controlled controlledProcess) error {
	_, err := agentprocess.Signal(agentprocess.ControlRequest{
		Target: agentprocess.ControlTarget{
			SessionID: controlled.lease.SessionID, Generation: controlled.lease.Generation,
			OwnerTokenHash: workstore.HashToken(controlled.lease.OwnerToken),
			Leader: agentprocess.ProcessIdentity{
				PID: controlled.process.PID, StartIdentity: controlled.process.StartIdentity,
				ProcessGroupID: controlled.process.ProcessGroupID,
			},
		},
		Scope: agentprocess.ScopeLeader, Signal: agentprocess.SignalKill,
	})
	if errors.Is(err, agentprocess.ErrProcessGone) || errors.Is(err, agentprocess.ErrProcessIdentityMismatch) {
		return nil
	}
	return err
}

func shortDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:16])
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
