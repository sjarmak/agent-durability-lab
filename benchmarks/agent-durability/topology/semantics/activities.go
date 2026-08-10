package semantics

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/agent"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type ActivityHandler struct {
	Runtime  *EpisodeRuntime
	WorkerID string
}

func (h *ActivityHandler) validate(ctx context.Context) (*EpisodeRuntime, error) {
	if h == nil || h.Runtime == nil || h.WorkerID == "" {
		return nil, fmt.Errorf("%w: semantics Activity handler", protocol.ErrInvalidEvidence)
	}
	if err := h.Runtime.waitStarted(ctx); err != nil {
		return nil, err
	}
	return h.Runtime, nil
}

func (h *ActivityHandler) Work(ctx context.Context, input WorkInput) (WorkResult, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return WorkResult{}, err
	}
	runtime.beginActivity()
	defer runtime.endActivity()
	identity := runtime.activityIdentity(ctx, h.WorkerID, input.Item.ID, input.Authority)
	startedID := runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{
		"activity_type": WorkActivityName,
	})
	info := activity.GetInfo(ctx)
	if info.Attempt > 1 || input.Replacement {
		runtime.recordRecovery(identity)
		if input.Case == protocol.CaseJoinBarrier && input.Probe == protocol.ProbeProtected &&
			input.Boundary == "designated-item-result-observed-before-activity-completion" {
			runtime.releaseHeldJoinItem()
		}
	}
	if runtime.spec.Case == protocol.CaseQueuedExecutingSupersession && input.Item.ID == "item-001" && !input.Replacement {
		runtime.mu.Lock()
		runtime.oldWorkStarted = true
		runtime.mu.Unlock()
	}
	decision, err := runtime.claimWork(ctx, input, h.WorkerID, info.Attempt)
	if err != nil {
		return WorkResult{}, err
	}
	lease := decision.Lease
	identity.Generation = lease.Generation
	identity.CapabilityHash = workstore.HashToken(lease.OwnerToken)
	requestID := fmt.Sprintf("%s/%s/generation-%d/attempt-%d", runtime.spec.RunID, input.Item.ID, lease.Generation, info.Attempt)
	dependencyStarted := runtime.appendEvent(identity, protocol.EventDependencyStarted, protocol.DecisionObserved, map[string]string{
		"request_id": requestID,
	}, startedID)
	switch decision.Action {
	case workstore.ActionLaunch:
		if err := runtime.launchAndControl(ctx, input, identity, lease); err != nil {
			return WorkResult{}, err
		}
	case workstore.ActionAttach:
		if err := runtime.controlAttachedProcess(ctx, input, identity, lease); err != nil {
			return WorkResult{}, err
		}
	}
	if decision.Action != workstore.ActionComplete {
		if _, err := runtime.waitForLeaseDisposition(ctx, input.Item.ID, lease); err != nil {
			return WorkResult{}, err
		}
	}
	if input.Case == protocol.CaseQueuedExecutingSupersession && input.Item.ID == "item-001" &&
		!input.Replacement && input.Probe != protocol.ProbeUnsafe {
		return WorkResult{}, temporal.NewCanceledError("obsolete generation was fenced before result publication")
	}
	if input.Case == protocol.CaseQueuedExecutingSupersession && input.Item.ID == "item-001" &&
		input.Probe == protocol.ProbeUnsafe && !input.Replacement {
		if err := runtime.recordOldEffect(identity, lease); err != nil {
			return WorkResult{}, err
		}
	}
	finishedID := runtime.appendEvent(identity, protocol.EventDependencyFinished, protocol.DecisionAccepted, map[string]string{
		"request_id": requestID, "action": string(decision.Action),
	}, dependencyStarted)
	runtime.mu.Lock()
	runtime.requests = append(runtime.requests, protocol.DependencyRequest{
		RequestID: requestID, EventID: finishedID, WorkItemID: input.Item.ID, Attempt: int(info.Attempt), Outcome: "ok", CostUnits: 1,
	})
	runtime.mu.Unlock()
	contribution := Contribution{ItemID: input.Item.ID, Ordinal: input.Item.Ordinal, Attempt: int(info.Attempt)}
	if input.Case == protocol.CaseJoinBarrier || input.Case == protocol.CaseIncrementalPartialReduction {
		runtime.mu.Lock()
		contributionDecision := protocol.DecisionAccepted
		if input.Probe != protocol.ProbeUnsafe && len(runtime.deliveries[input.Item.ID]) > 0 {
			contributionDecision = protocol.DecisionReconciled
		}
		runtime.mu.Unlock()
		contributionEvent := runtime.appendEvent(identity, protocol.EventContributionAccepted, contributionDecision, map[string]string{
			"ordinal": fmt.Sprint(input.Item.Ordinal),
		}, finishedID)
		runtime.mu.Lock()
		runtime.deliveries[input.Item.ID] = append(runtime.deliveries[input.Item.ID], contribution)
		runtime.contributions = append(runtime.contributions, protocol.ContributionObservation{
			EventID: contributionEvent, WorkItemID: input.Item.ID, Ordinal: input.Item.Ordinal,
			ActivityAttempt: int(info.Attempt), Decision: contributionDecision,
		})
		runtime.mu.Unlock()
	} else {
		runtime.mu.Lock()
		runtime.deliveries[input.Item.ID] = append(runtime.deliveries[input.Item.ID], contribution)
		runtime.mu.Unlock()
	}
	resultEvent := runtime.appendEvent(identity, protocol.EventResultAccepted, protocol.DecisionAccepted, nil, finishedID)
	if runtime.holdJoinItem(input) {
		runtime.heldResultOnce.Do(func() { close(runtime.heldResultDone) })
	}
	if runtime.shouldCrashWork(input, int(info.Attempt)) {
		activity.RecordHeartbeat(ctx, runtime.spec.Boundary)
		if err := runtime.reachFault(ctx, identity, WorkerTargetWork); err != nil {
			return WorkResult{}, err
		}
	}
	if runtime.shouldTerminalFail(input) {
		activity.RecordHeartbeat(ctx, runtime.spec.Boundary)
		if err := runtime.reachFault(ctx, identity, WorkerTargetNone); err != nil {
			return WorkResult{}, err
		}
		runtime.recordRecovery(identity)
		runtime.mu.Lock()
		runtime.terminalFailure = true
		runtime.mu.Unlock()
		return WorkResult{}, temporal.NewNonRetryableApplicationError(
			"required item failed at the registered terminal boundary", "terminal_required_item", nil,
		)
	}
	if input.Case == protocol.CaseQueuedExecutingSupersession && input.Replacement {
		if err := runtime.recordReplacementEffect(identity, lease, resultEvent); err != nil {
			return WorkResult{}, err
		}
	}
	runtime.mu.Lock()
	deliveries := slices.Clone(runtime.deliveries[input.Item.ID])
	runtime.mu.Unlock()
	return WorkResult{ItemID: input.Item.ID, Ordinal: input.Item.Ordinal, Deliveries: deliveries}, nil
}

func (r *EpisodeRuntime) activityIdentity(ctx context.Context, workerID, itemID string, authority Authority) protocol.Identity {
	info := activity.GetInfo(ctx)
	r.mu.Lock()
	parentWorkflow, parentRun := r.parentWorkflow, r.parentRun
	r.mu.Unlock()
	identity := protocol.Identity{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: r.spec.RunID, PairID: r.spec.PairID,
		ScheduleBlockID: r.spec.ScheduleBlockID, TrackerBeadID: r.manifest.TrackerBeadID, Topology: r.spec.Topology,
		Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
		LogicalOperationID: r.spec.LogicalOperationID, WorkItemID: itemID,
		Generation: authority.Generation, CapabilityHash: authority.CapabilityHash,
		ParentWorkflowID: parentWorkflow, ParentRunID: parentRun, ActivityID: info.ActivityID,
		ActivityAttempt: int(info.Attempt), WorkerID: workerID, WorkerPID: os.Getpid(),
		ProcessIdentity: fmt.Sprintf("worker:%s/pid:%d", workerID, os.Getpid()),
	}
	if r.spec.Topology == protocol.TopologyChildWorkflow && info.WorkflowExecution.ID != parentWorkflow {
		identity.ChildWorkflowID = info.WorkflowExecution.ID
		identity.ChildRunID = info.WorkflowExecution.RunID
	}
	return identity
}

func (r *EpisodeRuntime) claimWork(ctx context.Context, input WorkInput, workerID string, attempt int32) (workstore.Decision, error) {
	store, err := r.workStore(input.Item.ID)
	if err != nil {
		return workstore.Decision{}, err
	}
	if !input.Replacement && r.spec.Case == protocol.CaseQueuedExecutingSupersession && r.spec.Probe == protocol.ProbeUnsafe {
		r.mu.Lock()
		pending := r.obsoletePending[input.Item.ID]
		if pending != nil && !pending.claimed {
			pending.claimed = true
			decision := pending.decision
			r.mu.Unlock()
			return decision, nil
		}
		r.mu.Unlock()
	}
	if input.Replacement {
		r.mu.Lock()
		pending := r.pending[input.Item.ID]
		if pending != nil && !pending.claimed {
			pending.claimed = true
			decision := pending.decision
			r.mu.Unlock()
			return decision, nil
		}
		r.mu.Unlock()
	}
	token := r.tokens[input.Authority.Generation]
	if token == "" || workstore.HashToken(token) != input.Authority.CapabilityHash {
		return workstore.Decision{}, fmt.Errorf("%w: Work authority token", protocol.ErrInvalidEvidence)
	}
	mode := workstore.ModeFenced
	if r.spec.Case == protocol.CaseQueuedExecutingSupersession && r.spec.Probe == protocol.ProbeUnsafe {
		mode = workstore.ModeUnsafe
	}
	decision, err := store.StartOrAttach(ctx, workstore.StartRequest{
		SessionID: r.spec.LogicalOperationID + "/" + input.Item.ID, Mode: mode,
		CandidateOwner: token, WorkerID: workerID, AgentBuild: "topology-hermetic-agent-v1", Attempt: attempt,
	})
	if err != nil {
		return workstore.Decision{}, err
	}
	if mode == workstore.ModeFenced &&
		(decision.Lease.Generation != input.Authority.Generation || workstore.HashToken(decision.Lease.OwnerToken) != input.Authority.CapabilityHash) {
		return workstore.Decision{}, temporal.NewCanceledError("immutable Work authority was superseded before execution")
	}
	return decision, nil
}

func (r *EpisodeRuntime) launchAndControl(ctx context.Context, input WorkInput, identity protocol.Identity, lease workstore.Lease) error {
	store, err := r.workStore(input.Item.ID)
	if err != nil {
		return err
	}
	barrier := r.barriers[input.Item.ID]
	if barrier == nil {
		return fmt.Errorf("%w: item barrier", protocol.ErrInvalidEvidence)
	}
	beforeEffect := fmt.Sprintf("before-effect/%d", lease.Generation)
	beforeCompletion := fmt.Sprintf("before-completion/%d", lease.Generation)
	effectArrival := barrier.coordinator.ArrivalCount(beforeEffect) + 1
	completionArrival := barrier.coordinator.ArrivalCount(beforeCompletion) + 1
	r.mu.Lock()
	parentWorkflow, parentRun := r.parentWorkflow, r.parentRun
	r.mu.Unlock()
	request := agent.Request{
		Manifest: r.manifest, WorkItemID: input.Item.ID, Lease: lease,
		ParentWorkflowID: parentWorkflow, ParentRunID: parentRun, ActivityID: identity.ActivityID,
		ActivityAttempt: identity.ActivityAttempt, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID,
		StorePath: store.Path(), BarrierURL: barrier.url, EffectValue: "effect/" + input.Item.ID,
		OutcomeValue: "outcome/" + input.Item.ID + fmt.Sprintf("/generation-%d", lease.Generation),
		BypassAuthorityForEffect: r.spec.Probe == protocol.ProbeUnsafe &&
			r.spec.Case == protocol.CaseQueuedExecutingSupersession && input.Item.ID == "item-001" && !input.Replacement,
	}
	if r.spec.Topology == protocol.TopologyChildWorkflow {
		request.ChildWorkflowID, request.ChildRunID = identity.ChildWorkflowID, identity.ChildRunID
	}
	launched, err := r.launcher.Launch(ctx, request)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.controlledProcesses[launched.Process.PID] = controlledProcess{lease: lease, process: launched.Process}
	if input.Item.ID == "item-001" && !input.Replacement {
		r.obsoleteLease, r.oldProcessLaunched = lease, true
	}
	r.mu.Unlock()
	processEvent := r.appendEvent(launched.Identity, protocol.EventProcessStarted, protocol.DecisionObserved, nil)
	r.mu.Lock()
	r.processes = append(r.processes, protocol.ProcessObservation{
		EventID: processEvent, WorkItemID: input.Item.ID, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID,
		ProcessIdentity: launched.Identity.ProcessIdentity, State: "running",
	})
	r.mu.Unlock()
	arrivals, err := waitForArrivalsWithHeartbeat(ctx, barrier.coordinator, beforeEffect, effectArrival)
	if err != nil {
		return err
	}
	processIdentity := launched.Identity
	if arrival := arrivals[len(arrivals)-1]; arrival.PID != launched.Process.PID || arrival.ProcessStart != launched.Process.StartIdentity {
		return fmt.Errorf("%w: exact process barrier identity", protocol.ErrInvalidEvidence)
	}
	if r.spec.Case == protocol.CaseQueuedExecutingSupersession &&
		(r.spec.Boundary == "executing-after-process-start-before-effect" || r.spec.Probe == protocol.ProbeUnfaulted) &&
		input.Item.ID == "item-001" && !input.Replacement {
		return r.ensureOldEffectBoundary(input, processIdentity)
	}
	if err := barrier.coordinator.Release(beforeEffect); err != nil {
		return err
	}
	if _, err := waitForArrivalsWithHeartbeat(ctx, barrier.coordinator, beforeCompletion, completionArrival); err != nil {
		return err
	}
	if r.spec.Case == protocol.CaseQueuedExecutingSupersession && r.spec.Probe == protocol.ProbeUnsafe &&
		input.Item.ID == "item-001" && !input.Replacement {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.replacementDone:
		}
	}
	if r.holdJoinItem(input) {
		if err := waitForSignalWithHeartbeat(ctx, r.heldRelease, "held-join-release"); err != nil {
			return err
		}
	}
	return barrier.coordinator.Release(beforeCompletion)
}

func (r *EpisodeRuntime) controlAttachedProcess(ctx context.Context, input WorkInput, identity protocol.Identity, lease workstore.Lease) error {
	store, err := r.workStore(input.Item.ID)
	if err != nil {
		return err
	}
	snapshot, err := store.Snapshot(ctx, lease.SessionID)
	if err != nil {
		return err
	}
	status := ""
	for _, executor := range snapshot.Executors {
		if executor.Generation == lease.Generation && executor.OwnerTokenHash == workstore.HashToken(lease.OwnerToken) {
			status = executor.Status
			break
		}
	}
	if status == workstore.ExecutorStatusLaunchPending {
		return r.launchAndControl(ctx, input, identity, lease)
	}
	if status != workstore.ExecutorStatusRunning {
		return nil
	}
	barrier := r.barriers[input.Item.ID]
	if barrier == nil {
		return fmt.Errorf("%w: attached item barrier", protocol.ErrInvalidEvidence)
	}
	beforeEffect := fmt.Sprintf("before-effect/%d", lease.Generation)
	if _, err := waitForArrivalsWithHeartbeat(ctx, barrier.coordinator, beforeEffect, 1); err != nil {
		return err
	}
	if r.spec.Case == protocol.CaseQueuedExecutingSupersession &&
		(r.spec.Boundary == "executing-after-process-start-before-effect" || r.spec.Probe == protocol.ProbeUnfaulted) &&
		input.Item.ID == "item-001" && !input.Replacement {
		select {
		case <-r.oldEffectBoundaryReady:
			return nil
		default:
		}
		r.mu.Lock()
		var attached agentprocess.Process
		for _, controlled := range r.controlledProcesses {
			if controlled.lease.SessionID == lease.SessionID && controlled.lease.Generation == lease.Generation &&
				controlled.lease.OwnerToken == lease.OwnerToken {
				attached = controlled.process
				break
			}
		}
		r.mu.Unlock()
		if attached.PID <= 0 || attached.StartIdentity == "" {
			return fmt.Errorf("%w: attached supersession process identity", protocol.ErrInvalidEvidence)
		}
		processIdentity := identity
		processIdentity.Generation = lease.Generation
		processIdentity.CapabilityHash = workstore.HashToken(lease.OwnerToken)
		processIdentity.ProcessIdentity = fmt.Sprintf("pid:%d/start:%s", attached.PID, attached.StartIdentity)
		return r.ensureOldEffectBoundary(input, processIdentity)
	}
	if err := barrier.coordinator.Release(beforeEffect); err != nil {
		return err
	}
	beforeCompletion := fmt.Sprintf("before-completion/%d", lease.Generation)
	if _, err := waitForArrivalsWithHeartbeat(ctx, barrier.coordinator, beforeCompletion, 1); err != nil {
		return err
	}
	if r.holdJoinItem(input) {
		if err := waitForSignalWithHeartbeat(ctx, r.heldRelease, "attached-held-join-release"); err != nil {
			return err
		}
	}
	return barrier.coordinator.Release(beforeCompletion)
}

func (r *EpisodeRuntime) ensureOldEffectBoundary(input WorkInput, identity protocol.Identity) error {
	r.oldEffectBoundaryGate.Lock()
	defer r.oldEffectBoundaryGate.Unlock()
	r.mu.Lock()
	existing := r.oldEffectBoundary
	r.mu.Unlock()
	if existing.identity.ProcessIdentity != "" {
		return r.recordOldEffectBoundaryLocked(oldEffectBoundary{
			identity:       identity,
			barrierEventID: existing.barrierEventID,
		})
	}
	barrierEvent := ""
	if r.spec.Probe != protocol.ProbeUnfaulted {
		barrierEvent = r.appendEvent(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, map[string]string{
			"point": fmt.Sprintf("before-effect/%d", identity.Generation),
		})
		r.mu.Lock()
		r.processes = append(r.processes, protocol.ProcessObservation{
			EventID: barrierEvent, WorkItemID: input.Item.ID, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID,
			ProcessIdentity: identity.ProcessIdentity, State: "blocked-before-effect",
		})
		r.mu.Unlock()
	}
	return r.recordOldEffectBoundaryLocked(oldEffectBoundary{identity: identity, barrierEventID: barrierEvent})
}

type arrivalWaitResult struct {
	arrivals []failureinject.Arrival
	err      error
}

func waitForArrivalsWithHeartbeat(
	ctx context.Context,
	coordinator *failureinject.Coordinator,
	point string,
	count int,
) ([]failureinject.Arrival, error) {
	result := make(chan arrivalWaitResult, 1)
	go func() {
		arrivals, err := coordinator.WaitForArrivals(ctx, point, count)
		result <- arrivalWaitResult{arrivals: arrivals, err: err}
	}()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case value := <-result:
			return value.arrivals, value.err
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, point, count)
		}
	}
}

func waitForSignalWithHeartbeat(ctx context.Context, signal <-chan struct{}, label string) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-signal:
			return nil
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, label)
		}
	}
}

func (r *EpisodeRuntime) holdJoinItem(input WorkInput) bool {
	return r.spec.Case == protocol.CaseJoinBarrier && r.spec.Probe != protocol.ProbeUnfaulted &&
		r.spec.Boundary == "designated-item-result-observed-before-activity-completion" &&
		input.Item.Ordinal == r.spec.Fanout
}

func (r *EpisodeRuntime) releaseHeldJoinItem() {
	r.heldReleaseOnce.Do(func() {
		close(r.heldRelease)
		if barrier := r.barriers[fmt.Sprintf("item-%03d", r.spec.Fanout)]; barrier != nil {
			_ = barrier.coordinator.Release("before-completion/1")
		}
	})
}

func (r *EpisodeRuntime) waitForLeaseDisposition(ctx context.Context, itemID string, lease workstore.Lease) (workstore.Snapshot, error) {
	store, err := r.workStore(itemID)
	if err != nil {
		return workstore.Snapshot{}, err
	}
	return waitForStoreLeaseDisposition(ctx, store, lease)
}

func waitForStoreLeaseDisposition(
	ctx context.Context,
	store *workstore.Store,
	lease workstore.Lease,
) (workstore.Snapshot, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := store.Snapshot(ctx, lease.SessionID)
		if err != nil {
			return workstore.Snapshot{}, err
		}
		for _, event := range snapshot.Events {
			if event.Generation == lease.Generation && event.OwnerTokenHash == workstore.HashToken(lease.OwnerToken) &&
				slices.Contains([]string{
					"outcome_accepted", "completion_duplicate", "completion_rejected_terminal", "completion_rejected_stale",
					"completion_rejected_not_running", "completion_rejected_canceled",
				}, event.Kind) {
				return snapshot, nil
			}
		}
		select {
		case <-ctx.Done():
			return workstore.Snapshot{}, ctx.Err()
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, lease.Generation)
		}
	}
}

func (r *EpisodeRuntime) shouldCrashWork(input WorkInput, attempt int) bool {
	if r.spec.Probe == protocol.ProbeUnfaulted || attempt != 1 || input.Item.ID != "item-001" {
		return false
	}
	return r.spec.Case == protocol.CaseJoinBarrier && r.spec.Boundary == "designated-item-result-observed-before-activity-completion" ||
		r.spec.Case == protocol.CaseIncrementalPartialReduction && r.spec.Boundary == "contribution-observed-before-work-activity-completion"
}

func (r *EpisodeRuntime) shouldTerminalFail(input WorkInput) bool {
	return r.spec.Probe != protocol.ProbeUnfaulted && r.spec.Case == protocol.CaseJoinBarrier &&
		r.spec.Boundary == "required-item-terminal-failure-before-join" && input.Item.ID == "item-001"
}

func (h *ActivityHandler) Checkpoint(ctx context.Context, input CheckpointInput) (CheckpointReceipt, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return CheckpointReceipt{}, err
	}
	runtime.beginActivity()
	defer runtime.endActivity()
	identity := runtime.activityIdentity(ctx, h.WorkerID, firstMember(input.Members), runtime.Input().InitialAuthority)
	info := activity.GetInfo(ctx)
	runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{"activity_type": CheckpointActivityName})
	if info.Attempt > 1 {
		runtime.recordRecovery(identity)
	}
	runtime.mu.Lock()
	prior, exists := runtime.checkpointByID[input.CheckpointID]
	decision, applied := protocol.DecisionAccepted, true
	receiptID := "receipt/" + input.CheckpointID
	if input.Probe == protocol.ProbeUnsafe {
		receiptID = fmt.Sprintf("%s/attempt-%d", receiptID, info.Attempt)
	} else if exists {
		decision, applied, receiptID = protocol.DecisionReconciled, false, prior.receiptID
	}
	if applied {
		runtime.checkpointByID[input.CheckpointID] = checkpointRecord{receiptID: receiptID, input: input}
	}
	runtime.mu.Unlock()
	eventID := runtime.appendEvent(identity, protocol.EventCheckpointAccepted, decision, map[string]string{
		"checkpoint_id": input.CheckpointID, "cardinality": fmt.Sprint(input.Cardinality), "value": fmt.Sprint(input.Value),
	})
	runtime.mu.Lock()
	runtime.checkpoints = append(runtime.checkpoints, protocol.CheckpointObservation{
		EventID: eventID, CheckpointID: input.CheckpointID, Cardinality: input.Cardinality, Members: slices.Clone(input.Members),
		Value: input.Value, ReceiptID: receiptID, Decision: decision, Applied: applied,
	})
	runtime.mu.Unlock()
	if runtime.spec.Probe != protocol.ProbeUnfaulted && runtime.spec.Case == protocol.CaseIncrementalPartialReduction &&
		runtime.spec.Boundary == "partial-checkpoint-accepted-before-checkpoint-activity-completion" && info.Attempt == 1 {
		activity.RecordHeartbeat(ctx, runtime.spec.Boundary)
		if err := runtime.reachFault(ctx, identity, WorkerTargetEffect); err != nil {
			return CheckpointReceipt{}, err
		}
	}
	return CheckpointReceipt{CheckpointID: input.CheckpointID, ReceiptID: receiptID}, nil
}

func (h *ActivityHandler) Continue(ctx context.Context, input ContinuationInput) (ContinuationReceipt, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return ContinuationReceipt{}, err
	}
	runtime.beginActivity()
	defer runtime.endActivity()
	authority := runtime.Input().InitialAuthority
	if runtime.spec.Case == protocol.CaseQueuedExecutingSupersession {
		authority = runtime.Input().ReplacementAuthority
	}
	identity := runtime.activityIdentity(ctx, h.WorkerID, firstMember(input.Members), authority)
	if runtime.spec.Case == protocol.CaseQueuedExecutingSupersession {
		select {
		case <-ctx.Done():
			return ContinuationReceipt{}, ctx.Err()
		case <-runtime.oldActionDone:
		}
	}
	runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{"activity_type": ContinuationActivityName})
	runtime.mu.Lock()
	prior, exists := runtime.continuationByID[input.ContinuationID]
	decision, applied := protocol.DecisionAccepted, true
	receiptID := "receipt/" + input.ContinuationID
	if exists {
		decision, applied, receiptID = protocol.DecisionReconciled, false, prior.receiptID
	} else {
		runtime.continuationByID[input.ContinuationID] = continuationRecord{receiptID: receiptID, input: input}
	}
	runtime.mu.Unlock()
	eventID := runtime.appendEvent(identity, protocol.EventContinuationAccepted, decision, map[string]string{
		"continuation_id": input.ContinuationID, "value": fmt.Sprint(input.Value),
	})
	runtime.mu.Lock()
	runtime.continuations = append(runtime.continuations, protocol.ContinuationObservation{
		EventID: eventID, ContinuationID: input.ContinuationID, Members: slices.Clone(input.Members), Value: input.Value,
		ReceiptID: receiptID, Decision: decision, Applied: applied,
	})
	runtime.mu.Unlock()
	if runtime.spec.Case == protocol.CaseJoinBarrier && runtime.spec.Probe == protocol.ProbeUnsafe &&
		runtime.spec.Boundary == "designated-item-result-observed-before-activity-completion" && len(input.Members) < runtime.spec.Fanout {
		runtime.releaseHeldJoinItem()
		select {
		case <-ctx.Done():
			return ContinuationReceipt{}, ctx.Err()
		case <-runtime.heldResultDone:
		}
	}
	return ContinuationReceipt{ContinuationID: input.ContinuationID, ReceiptID: receiptID}, nil
}

func (h *ActivityHandler) Supersede(ctx context.Context, input SupersedeInput) (SupersedeReceipt, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return SupersedeReceipt{}, err
	}
	runtime.beginActivity()
	defer runtime.endActivity()
	select {
	case runtime.supersedeGate <- struct{}{}:
		defer func() { <-runtime.supersedeGate }()
	case <-ctx.Done():
		return SupersedeReceipt{}, ctx.Err()
	}
	store, err := runtime.workStore(input.ItemID)
	if err != nil {
		return SupersedeReceipt{}, err
	}
	identity := runtime.activityIdentity(ctx, h.WorkerID, input.ItemID, input.Obsolete)
	runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{"activity_type": SupersedeActivityName})
	runtime.mu.Lock()
	cachedReceipt := runtime.supersessionReceipt
	runtime.mu.Unlock()
	if cachedReceipt != nil {
		return *cachedReceipt, nil
	}
	var boundary oldEffectBoundary
	select {
	case <-runtime.oldEffectBoundaryReady:
		boundary, err = runtime.waitOldEffectBoundary(ctx)
		if err != nil {
			return SupersedeReceipt{}, err
		}
		goto boundaryReady
	default:
	}
	if input.Boundary == "executing-after-process-start-before-effect" || input.Probe == protocol.ProbeUnfaulted {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return SupersedeReceipt{}, ctx.Err()
			case <-runtime.oldEffectBoundaryReady:
				boundary, err = runtime.waitOldEffectBoundary(ctx)
				if err != nil {
					return SupersedeReceipt{}, err
				}
				goto boundaryReady
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, input.ItemID)
			}
		}
	} else {
		runtime.mu.Lock()
		started := runtime.oldWorkStarted
		initialLease := runtime.obsoleteLease
		runtime.mu.Unlock()
		if started && initialLease.Generation == 0 {
			return SupersedeReceipt{}, fmt.Errorf("%w: queued obsolete Activity already started", protocol.ErrInvalidEvidence)
		}
		barrierID := runtime.appendEvent(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, map[string]string{
			"point": "queued-before-activity-start",
		})
		runtime.mu.Lock()
		runtime.processes = append(runtime.processes, protocol.ProcessObservation{
			EventID: barrierID, WorkItemID: input.ItemID, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID,
			ProcessIdentity: identity.ProcessIdentity, State: "old-activity-durably-queued",
		})
		runtime.mu.Unlock()
		boundary = oldEffectBoundary{identity: identity, barrierEventID: barrierID}
		if err := runtime.recordOldEffectBoundary(boundary); err != nil {
			return SupersedeReceipt{}, err
		}
	}

boundaryReady:
	if err := runtime.destination.Supersede(input.ItemID, input.Obsolete, input.Replacement); err != nil {
		return SupersedeReceipt{}, err
	}
	sessionID := runtime.spec.LogicalOperationID + "/" + input.ItemID
	mode := workstore.ModeFenced
	if input.Probe == protocol.ProbeUnsafe {
		mode = workstore.ModeUnsafe
	}
	if input.Boundary == "queued-before-activity-start" {
		runtime.mu.Lock()
		initialLease := runtime.obsoleteLease
		runtime.mu.Unlock()
		if initialLease.Generation == 0 {
			initial, startErr := store.StartOrAttach(ctx, workstore.StartRequest{
				SessionID: sessionID, Mode: mode, CandidateOwner: runtime.tokens[1],
				WorkerID: h.WorkerID, AgentBuild: "topology-hermetic-agent-v1", Attempt: 1,
			})
			if startErr != nil {
				return SupersedeReceipt{}, startErr
			}
			runtime.mu.Lock()
			runtime.obsoleteLease = initial.Lease
			if input.Probe == protocol.ProbeUnsafe {
				runtime.obsoletePending[input.ItemID] = &pendingLaunch{decision: initial}
			}
			runtime.mu.Unlock()
		}
	}
	runtime.mu.Lock()
	pending := runtime.pending[input.ItemID]
	runtime.mu.Unlock()
	if pending == nil {
		replaceOwner := input.Probe != protocol.ProbeUnsafe
		replacement, startErr := store.StartOrAttach(ctx, workstore.StartRequest{
			SessionID: sessionID, Mode: mode, CandidateOwner: runtime.tokens[2],
			WorkerID: h.WorkerID, AgentBuild: "topology-hermetic-agent-v1", Attempt: 2, ReplaceOwner: replaceOwner,
		})
		if startErr != nil {
			return SupersedeReceipt{}, startErr
		}
		if err := validateReplacementDecision(replacement, input.Replacement); err != nil {
			return SupersedeReceipt{}, err
		}
		pending = &pendingLaunch{decision: replacement}
		runtime.mu.Lock()
		runtime.pending[input.ItemID] = pending
		runtime.mu.Unlock()
	} else if err := validateReplacementDecision(pending.decision, input.Replacement); err != nil {
		return SupersedeReceipt{}, err
	}
	commitID := runtime.appendEvent(boundary.identity, protocol.EventSupersessionCommitted, protocol.DecisionAccepted, nil, boundary.barrierEventID)
	faultID := ""
	if input.Probe != protocol.ProbeUnfaulted {
		faultID = runtime.appendEvent(boundary.identity, protocol.EventFaultCommitted, protocol.DecisionAccepted, nil, boundary.barrierEventID, commitID)
	}
	runtime.mu.Lock()
	if input.Probe != protocol.ProbeUnfaulted {
		runtime.faultCommitted = true
		runtime.fault = protocol.FaultBoundary{
			RunID: runtime.spec.RunID, Injected: true, ExpectedBoundary: runtime.spec.Boundary,
			BarrierEventID: boundary.barrierEventID, FaultEventID: faultID, TargetProcessIdentity: boundary.identity.ProcessIdentity,
		}
		runtime.faultIdentity = boundary.identity
	}
	runtime.supersession = &protocol.SupersessionObservation{
		CommitEventID: commitID, ObsoleteItemID: input.ItemID,
		ObsoleteGeneration: input.Obsolete.Generation, ObsoleteCapabilityHash: input.Obsolete.CapabilityHash,
		ReplacementGeneration: input.Replacement.Generation, ReplacementCapabilityHash: input.Replacement.CapabilityHash,
	}
	receipt := SupersedeReceipt{ItemID: input.ItemID, Generation: input.Replacement.Generation}
	runtime.supersessionReceipt = &receipt
	runtime.mu.Unlock()
	return receipt, nil
}

func validateReplacementDecision(decision workstore.Decision, authority Authority) error {
	if decision.Action == workstore.ActionComplete || decision.Lease.Generation != authority.Generation ||
		workstore.HashToken(decision.Lease.OwnerToken) != authority.CapabilityHash {
		return fmt.Errorf("%w: replacement Work decision authority", protocol.ErrInvalidEvidence)
	}
	return nil
}

func (h *ActivityHandler) Cancellation(ctx context.Context, input CancellationInput) (CancellationReceipt, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return CancellationReceipt{}, err
	}
	runtime.beginActivity()
	defer runtime.endActivity()
	identity := runtime.activityIdentity(ctx, h.WorkerID, input.ItemID, input.Obsolete)
	runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{"activity_type": CancellationActivityName})
	decision := protocol.DecisionAccepted
	if !input.Requested {
		decision = protocol.DecisionFailed
	}
	cancellationID := runtime.appendEvent(identity, protocol.EventCancellationRequested, decision, map[string]string{
		"requested": fmt.Sprint(input.Requested),
	})
	runtime.mu.Lock()
	launched, lease := runtime.oldProcessLaunched, runtime.obsoleteLease
	runtime.mu.Unlock()
	disposition := "not-started"
	if launched {
		barrier := runtime.barriers[input.ItemID]
		beforeEffect := fmt.Sprintf("before-effect/%d", lease.Generation)
		beforeCompletion := fmt.Sprintf("before-completion/%d", lease.Generation)
		if err := barrier.coordinator.Release(beforeEffect); err != nil {
			return CancellationReceipt{}, err
		}
		if _, err := waitForArrivalsWithHeartbeat(ctx, barrier.coordinator, beforeCompletion, 1); err != nil {
			return CancellationReceipt{}, err
		}
		if input.Probe == protocol.ProbeUnsafe {
			disposition = "stale-effect-accepted-awaiting-replacement"
		} else {
			if err := barrier.coordinator.Release(beforeCompletion); err != nil {
				return CancellationReceipt{}, err
			}
			if _, err := runtime.waitForLeaseDisposition(ctx, input.ItemID, lease); err != nil {
				return CancellationReceipt{}, err
			}
			disposition = "stale-effect-rejected"
		}
		if err := runtime.recordOldEffect(identity, lease); err != nil {
			return CancellationReceipt{}, err
		}
	} else if input.Probe != protocol.ProbeUnsafe {
		runtime.oldActionOnce.Do(func() { close(runtime.oldActionDone) })
	}
	dispositionID := runtime.appendEvent(identity, protocol.EventProcessDisposed, protocol.DecisionObserved, map[string]string{
		"disposition": disposition,
	}, cancellationID)
	runtime.mu.Lock()
	if runtime.supersession != nil {
		runtime.supersession.CancellationEventID = cancellationID
		runtime.supersession.ProcessDispositionEventID = dispositionID
	}
	runtime.mu.Unlock()
	runtime.supersessionOnce.Do(func() { close(runtime.supersessionDone) })
	return CancellationReceipt{ItemID: input.ItemID, Disposition: disposition}, nil
}

func (r *EpisodeRuntime) recordOldEffect(identity protocol.Identity, lease workstore.Lease) error {
	r.oldEffectOnce.Do(func() {
		action, err := r.destination.ApplyEffect(EffectRequest{
			EventID: "pending-old-effect", ItemID: "item-001", LogicalEffectID: r.spec.LogicalOperationID + "/item-001/effect",
			Authority: Authority{Generation: lease.Generation, CapabilityHash: workstore.HashToken(lease.OwnerToken)}, Probe: r.spec.Probe,
		})
		if err == nil {
			kind := protocol.EventEffectAccepted
			if action.Decision == protocol.DecisionRejected {
				kind = protocol.EventEffectRejected
			}
			identity.Generation, identity.CapabilityHash = lease.Generation, workstore.HashToken(lease.OwnerToken)
			eventID := r.appendEvent(identity, kind, action.Decision, nil)
			action.EventID = eventID
			r.mu.Lock()
			r.destinationActions = append(r.destinationActions, action)
			r.mu.Unlock()
		}
		r.oldActionErr = err
		r.oldActionOnce.Do(func() { close(r.oldActionDone) })
	})
	<-r.oldActionDone
	return r.oldActionErr
}

func (r *EpisodeRuntime) recordReplacementEffect(identity protocol.Identity, lease workstore.Lease, parent string) error {
	r.replacementEffectOnce.Do(func() {
		action, err := r.destination.ApplyEffect(EffectRequest{
			EventID: "pending-replacement-effect", ItemID: "item-001", LogicalEffectID: r.spec.LogicalOperationID + "/item-001/replacement-effect",
			Authority: Authority{Generation: lease.Generation, CapabilityHash: workstore.HashToken(lease.OwnerToken)}, Probe: r.spec.Probe,
		})
		if err == nil {
			eventID := r.appendEvent(identity, protocol.EventEffectAccepted, action.Decision, nil, parent)
			action.EventID = eventID
			r.mu.Lock()
			r.destinationActions = append(r.destinationActions, action)
			r.mu.Unlock()
		}
		r.replacementActionErr = err
		r.replacementOnce.Do(func() { close(r.replacementDone) })
		barrier := r.barriers["item-001"]
		if barrier != nil {
			_ = barrier.coordinator.Release(fmt.Sprintf("before-completion/%d", r.obsoleteLease.Generation))
		}
	})
	<-r.replacementDone
	return r.replacementActionErr
}

func (h *ActivityHandler) Destructive(ctx context.Context, input DestructiveActivityInput) (DestructiveResult, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return DestructiveResult{}, err
	}
	runtime.beginActivity()
	defer runtime.endActivity()
	identity := runtime.activityIdentity(ctx, h.WorkerID, input.ItemID, input.Authority)
	info := activity.GetInfo(ctx)
	runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{"activity_type": DestructiveActivityName})
	if info.Attempt > 1 {
		runtime.recordRecovery(identity)
	}
	if runtime.spec.Probe != protocol.ProbeUnfaulted && runtime.spec.Boundary == "before-destination-acceptance" && info.Attempt == 1 {
		activity.RecordHeartbeat(ctx, runtime.spec.Boundary)
		if err := runtime.reachFault(ctx, identity, WorkerTargetEffect); err != nil {
			return DestructiveResult{}, err
		}
	}
	if runtime.spec.Probe == protocol.ProbeUnsafe && runtime.spec.Boundary == "before-destination-acceptance" && info.Attempt > 1 {
		result := DestructiveResult{
			OperationID: input.OperationID, Decision: protocol.DecisionReconciled, Applied: false,
			ReceiptID: "unsafe-assumed-receipt/" + input.OperationID, PreviousVersion: input.ExpectedVersion,
			ResultingVersion: input.ExpectedVersion,
		}
		runtime.recordDestructiveDelivery(identity, input, int(info.Attempt), result)
		return result, nil
	}
	result, err := runtime.destination.ApplyDestructive(DestructiveRequest{
		EventID: fmt.Sprintf("pending-destructive-%d", info.Attempt), ItemID: input.ItemID, OperationID: input.OperationID,
		Authority: input.Authority, ExpectedVersion: input.ExpectedVersion, Attempt: int(info.Attempt), Probe: input.Probe,
	})
	if err != nil {
		return DestructiveResult{}, err
	}
	runtime.recordDestructiveDelivery(identity, input, int(info.Attempt), result)
	if runtime.spec.Probe != protocol.ProbeUnfaulted && runtime.spec.Boundary == "destination-accepted-before-activity-completion" && info.Attempt == 1 {
		activity.RecordHeartbeat(ctx, runtime.spec.Boundary)
		if err := runtime.reachFault(ctx, identity, WorkerTargetEffect); err != nil {
			return DestructiveResult{}, err
		}
	}
	return result, nil
}

func (r *EpisodeRuntime) recordDestructiveDelivery(
	identity protocol.Identity,
	input DestructiveActivityInput,
	attempt int,
	result DestructiveResult,
) {
	kind := protocol.EventDestructiveAccepted
	if result.Decision == protocol.DecisionReconciled {
		kind = protocol.EventDestructiveReconciled
	}
	if result.Decision == protocol.DecisionRejected {
		kind = protocol.EventEffectRejected
	}
	eventID := r.appendEvent(identity, kind, result.Decision, map[string]string{
		"operation_id": input.OperationID, "receipt_id": result.ReceiptID,
	})
	action := protocol.DestinationAction{
		EventID: eventID, WorkItemID: input.ItemID, LogicalEffectID: input.OperationID, ReceiptID: result.ReceiptID,
		Generation: input.Authority.Generation, CapabilityHash: input.Authority.CapabilityHash,
		Decision: result.Decision, Applied: result.Applied,
	}
	r.mu.Lock()
	r.destinationActions = append(r.destinationActions, action)
	if r.destructive == nil {
		r.destructive = &protocol.DestructiveObservation{
			OperationID: input.OperationID, ExpectedPriorVersion: input.ExpectedVersion,
		}
	}
	r.destructive.Deliveries = append(r.destructive.Deliveries, protocol.DestructiveDelivery{
		EventID: eventID, ActivityAttempt: attempt, OperationID: input.OperationID,
		ExpectedVersion: input.ExpectedVersion, PreviousVersion: result.PreviousVersion, ResultingVersion: result.ResultingVersion,
		ReceiptID: result.ReceiptID, Decision: result.Decision, Applied: result.Applied,
	})
	r.destructive.FinalVersion = result.ResultingVersion
	r.destructive.OutcomeReceiptID = result.ReceiptID
	r.mu.Unlock()
}

func firstMember(members []string) string {
	if len(members) == 0 {
		return "item-001"
	}
	return members[0]
}
