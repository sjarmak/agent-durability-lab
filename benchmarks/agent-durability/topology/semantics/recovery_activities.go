package semantics

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/agent"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

func (h *ActivityHandler) RecoveryAdmission(ctx context.Context, input RecoveryAdmissionInput) (RecoveryAdmissionReceipt, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return RecoveryAdmissionReceipt{}, err
	}
	if input.ProtocolVersion != protocol.PublicationProtocolVersion || input.PairID != runtime.spec.PairID ||
		input.LogicalOperationID != runtime.spec.LogicalOperationID || input.EffectTaskQueue != runtime.spec.EffectTaskQueue ||
		input.Case != runtime.spec.Case || input.Probe != runtime.spec.Probe || input.BatchOrdinal < 1 {
		return RecoveryAdmissionReceipt{}, fmt.Errorf("%w: recovery admission input", protocol.ErrInvalidEvidence)
	}
	if err := input.Authority.validate(); err != nil {
		return RecoveryAdmissionReceipt{}, err
	}
	if input.Authority.Generation != 1 ||
		input.Authority.CapabilityHash != workstore.HashToken(runtime.tokens[input.Authority.Generation]) {
		return RecoveryAdmissionReceipt{}, fmt.Errorf("%w: recovery admission authority", protocol.ErrInvalidEvidence)
	}
	window := recoveryAdmissionWindow(runtime.spec.Case, runtime.spec.Probe, runtime.spec.Fanout)
	firstOrdinal := (input.BatchOrdinal-1)*window + 1
	wantCount := window
	if remaining := runtime.spec.Fanout - firstOrdinal + 1; remaining < wantCount {
		wantCount = remaining
	}
	if firstOrdinal < 1 || firstOrdinal > runtime.spec.Fanout || wantCount < 1 || len(input.Items) != wantCount {
		return RecoveryAdmissionReceipt{}, fmt.Errorf("%w: recovery admission batch shape", protocol.ErrInvalidEvidence)
	}
	itemIDs := make([]string, len(input.Items))
	for index, item := range input.Items {
		wantOrdinal := firstOrdinal + index
		if item.Ordinal != wantOrdinal || item.ID != fmt.Sprintf("item-%03d", wantOrdinal) {
			return RecoveryAdmissionReceipt{}, fmt.Errorf("%w: recovery admission membership", protocol.ErrInvalidEvidence)
		}
		itemIDs[index] = item.ID
	}

	runtime.beginActivity()
	defer runtime.endActivity()
	identity := runtime.activityIdentity(ctx, h.WorkerID, fmt.Sprintf("batch-%03d", input.BatchOrdinal), input.Authority)
	startedID := runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{
		"activity_type": RecoveryAdmissionActivityName,
	})
	runtime.mu.Lock()
	var receipt RecoveryAdmissionReceipt
	if existing, ok := runtime.recovery.admissionBatches[input.BatchOrdinal]; ok {
		if !slices.Equal(existing.items, itemIDs) {
			runtime.mu.Unlock()
			return RecoveryAdmissionReceipt{}, fmt.Errorf("%w: changed recovery admission retry", protocol.ErrInvalidEvidence)
		}
		runtime.appendEventLocked(identity, protocol.EventAdmissionDecided, protocol.DecisionReconciled, map[string]string{
			"batch_ordinal": fmt.Sprint(input.BatchOrdinal), "cardinality": fmt.Sprint(len(itemIDs)),
		}, startedID)
		receipt = existing.receipt
	} else {
		for _, item := range input.Items {
			state := runtime.recovery.items[item.ID]
			if state == nil || state.admitted || state.terminalEventID != "" {
				runtime.mu.Unlock()
				return RecoveryAdmissionReceipt{}, fmt.Errorf("%w: duplicate recovery admission", protocol.ErrInvalidEvidence)
			}
			itemIdentity := identity
			itemIdentity.WorkItemID = item.ID
			itemIdentity.ActivityID = fmt.Sprintf("work/%s/generation-%d", item.ID, input.Authority.Generation)
			itemIdentity.ActivityAttempt = 1
			itemIdentity.ChildWorkflowID, itemIdentity.ChildRunID = "", ""
			decisionID := runtime.appendEventLocked(itemIdentity, protocol.EventAdmissionDecided, protocol.DecisionAccepted, map[string]string{
				"batch_ordinal": fmt.Sprint(input.BatchOrdinal),
			}, startedID)
			scheduleID := runtime.appendEventLocked(itemIdentity, protocol.EventActivityScheduled, protocol.DecisionObserved, map[string]string{
				"batch_ordinal": fmt.Sprint(input.BatchOrdinal),
			}, decisionID)
			state.admitted = true
			state.scheduleEventID = scheduleID
			state.scheduledOffset = runtime.events[len(runtime.events)-1].MonotonicOffsetNS
			runtime.recovery.admittedOutstanding++
		}
		if runtime.recovery.admittedOutstanding > runtime.recovery.peakAdmitted {
			runtime.recovery.peakAdmitted = runtime.recovery.admittedOutstanding
		}
		receipt = RecoveryAdmissionReceipt{BatchOrdinal: input.BatchOrdinal, Admitted: len(input.Items)}
		runtime.recovery.admissionBatches[input.BatchOrdinal] = recoveryAdmissionRecord{receipt: receipt, items: slices.Clone(itemIDs)}
	}
	runtime.mu.Unlock()
	if err := runtime.prepareRecoveryAdmissionBoundary(ctx, identity, input); err != nil {
		return RecoveryAdmissionReceipt{}, err
	}
	return receipt, nil
}

func (r *EpisodeRuntime) prepareRecoveryAdmissionBoundary(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryAdmissionInput,
) error {
	if input.BatchOrdinal != 1 ||
		(input.Case != protocol.CaseLayeredRetryAmplification && input.Case != protocol.CaseOutageBacklogHerdRecovery) {
		return nil
	}
	identity.WorkItemID = "item-001"
	identity.ActivityID = "work/item-001/generation-1"
	identity.ActivityAttempt = 1
	if input.Case == protocol.CaseOutageBacklogHerdRecovery {
		return r.prepareOutageAdmission(ctx, identity)
	}
	select {
	case <-r.recovery.retryBoundaryReady:
		return nil
	default:
	}
	response, err := r.callRecoveryDependency(ctx, identity, "shared-client-budget")
	if err != nil {
		return err
	}
	if response.Outcome != "timeout" {
		return fmt.Errorf("%w: layered retry first response", protocol.ErrInvalidEvidence)
	}
	if r.spec.Probe != protocol.ProbeUnfaulted {
		if err := r.reachFault(ctx, identity, WorkerTargetNone); err != nil {
			return err
		}
	}
	r.recovery.retryBoundaryOnce.Do(func() { close(r.recovery.retryBoundaryReady) })
	return nil
}

func (r *EpisodeRuntime) prepareOutageAdmission(ctx context.Context, identity protocol.Identity) error {
	select {
	case <-r.recovery.outageCoordinatorReady:
		return nil
	default:
	}
	response, err := r.callRecoveryDependency(ctx, identity, "steady-state-probe")
	if err != nil {
		return err
	}
	if response.Outcome != "ok" {
		return fmt.Errorf("%w: steady-state dependency probe", protocol.ErrInvalidEvidence)
	}
	r.recovery.dependency.setOutage(true)
	r.appendEvent(identity, protocol.EventDependencyStateChanged, protocol.DecisionAccepted, map[string]string{"state": "outage"})
	r.mu.Lock()
	r.recovery.outageStartedOffset = time.Since(r.startedAt).Nanoseconds()
	r.mu.Unlock()
	r.recovery.outageCoordinatorOnce.Do(func() { close(r.recovery.outageCoordinatorReady) })
	return nil
}

func (h *ActivityHandler) RecoveryWork(ctx context.Context, input RecoveryWorkInput) (RecoveryWorkResult, error) {
	runtime, err := h.validate(ctx)
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	if err := validateRecoveryActivityInput(runtime, input); err != nil {
		return RecoveryWorkResult{}, err
	}
	if err := runtime.beginRecoveryActivity(ctx); err != nil {
		return RecoveryWorkResult{}, err
	}
	defer runtime.endRecoveryActivity()

	identity := runtime.activityIdentity(ctx, h.WorkerID, input.Item.ID, input.Authority)
	startedID := runtime.appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, map[string]string{
		"activity_type": RecoveryWorkActivityName, "phase": recoveryPhaseSuffix(input),
	})
	runtime.recordRecoveryActivityStart(identity, startedID)
	if identity.ActivityAttempt > 1 || input.Replacement || input.ReleaseWedged {
		runtime.recordRecovery(identity)
	}
	if err := runtime.awaitRecoveryCaseGate(ctx, identity, input); err != nil {
		return RecoveryWorkResult{}, err
	}

	switch input.Case {
	case protocol.CaseCrashRecoveryBoundaries:
		return runtime.runCrashRecovery(ctx, identity, input, h.WorkerID)
	case protocol.CaseLayeredRetryAmplification:
		return runtime.runLayeredRetry(ctx, identity, input, h.WorkerID)
	case protocol.CaseOutageBacklogHerdRecovery:
		return runtime.runOutageRecovery(ctx, identity, input, h.WorkerID)
	case protocol.CasePoisonWorkIsolation:
		return runtime.runPoisonWork(ctx, identity, input, h.WorkerID)
	case protocol.CaseSilentProgress:
		return runtime.runSilentProgress(ctx, identity, input, h.WorkerID)
	case protocol.CaseBackpressureOverload:
		return runtime.runHealthyRecoveryAgent(ctx, identity, input, h.WorkerID)
	default:
		return RecoveryWorkResult{}, fmt.Errorf("%w: recovery case", protocol.ErrInvalidEvidence)
	}
}

func (h *ActivityHandler) RecoveryCohort(ctx context.Context, input RecoveryWorkInput) error {
	runtime, err := h.validate(ctx)
	if err != nil {
		return err
	}
	if err := validateRecoveryActivityInput(runtime, input); err != nil {
		return err
	}
	if input.Case != protocol.CaseOutageBacklogHerdRecovery || input.RecoveryRound != 0 ||
		input.Replacement || input.ReleaseWedged {
		return fmt.Errorf("%w: recovery cohort input", protocol.ErrInvalidEvidence)
	}
	if err := runtime.beginRecoveryActivity(ctx); err != nil {
		return err
	}
	defer runtime.endRecoveryActivity()
	return waitForSignalWithHeartbeat(ctx, runtime.recovery.outageRestored, "outage-backlog-cohort")
}

func validateRecoveryActivityInput(runtime *EpisodeRuntime, input RecoveryWorkInput) error {
	if input.ProtocolVersion != protocol.PublicationProtocolVersion || input.PairID != runtime.spec.PairID ||
		input.LogicalOperationID != runtime.spec.LogicalOperationID || input.WorkTaskQueue != runtime.spec.WorkTaskQueue ||
		input.EffectTaskQueue != runtime.spec.EffectTaskQueue || input.Case != runtime.spec.Case ||
		input.Boundary != runtime.spec.Boundary || input.Probe != runtime.spec.Probe ||
		input.Item.ID == "" || input.Item.Ordinal < 1 || input.Item.Ordinal > runtime.spec.Fanout {
		return fmt.Errorf("%w: recovery Activity input", protocol.ErrInvalidEvidence)
	}
	if err := input.Authority.validate(); err != nil {
		return err
	}
	if err := input.ReplacementAuthority.validate(); err != nil {
		return err
	}
	if input.Replacement || input.ReleaseWedged {
		if input.Authority != input.ReplacementAuthority {
			return fmt.Errorf("%w: active recovery replacement authority", protocol.ErrInvalidEvidence)
		}
	} else if input.ReplacementAuthority.Generation <= input.Authority.Generation ||
		input.ReplacementAuthority.CapabilityHash == input.Authority.CapabilityHash {
		return fmt.Errorf("%w: recovery replacement authority", protocol.ErrInvalidEvidence)
	}
	return nil
}

func (r *EpisodeRuntime) awaitRecoveryCaseGate(ctx context.Context, identity protocol.Identity, input RecoveryWorkInput) error {
	switch input.Case {
	case protocol.CaseBackpressureOverload:
		return r.awaitBackpressureRelease(ctx, identity)
	case protocol.CasePoisonWorkIsolation:
		return r.awaitPoisonRelease(ctx, identity)
	default:
		return nil
	}
}

func (r *EpisodeRuntime) awaitBackpressureRelease(ctx context.Context, identity protocol.Identity) error {
	r.mu.Lock()
	r.recovery.backpressureReady++
	threshold := workerActivityConcurrency
	if threshold > r.spec.Fanout {
		threshold = r.spec.Fanout
	}
	commit := r.recovery.backpressureReady == threshold
	release := r.recovery.backpressureRelease
	r.mu.Unlock()
	if commit {
		if r.spec.Probe != protocol.ProbeUnfaulted {
			if err := r.reachFault(ctx, identity, WorkerTargetNone); err != nil {
				return err
			}
		}
		r.recovery.backpressureReleaseOnce.Do(func() { close(release) })
	}
	return waitForSignalWithHeartbeat(ctx, release, "backpressure-cohort-release")
}

func (r *EpisodeRuntime) awaitPoisonRelease(ctx context.Context, identity protocol.Identity) error {
	r.mu.Lock()
	if identity.ActivityAttempt == 1 {
		r.recovery.poisonInitialStarted[identity.WorkItemID] = true
	}
	ready := len(r.recovery.poisonInitialStarted) == r.spec.Fanout
	faultCommitted := r.fault.Injected || r.faultCommitted
	release := r.recovery.poisonRelease
	r.mu.Unlock()
	if identity.WorkItemID != "item-001" {
		if identity.ActivityAttempt == 1 && ready {
			r.recovery.poisonReleaseOnce.Do(func() { close(release) })
		}
		return nil
	}
	// A large child-Workflow cohort can keep the poison Activity at this
	// exact admission barrier beyond one StartToClose attempt. The retry is
	// still the same logical poison item and must not bypass an uncommitted
	// fault merely because Temporal assigned it a new delivery attempt.
	if identity.ActivityAttempt != 1 && (r.spec.Probe == protocol.ProbeUnfaulted || faultCommitted) {
		return nil
	}
	if err := waitUntil(ctx, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.recovery.poisonInitialStarted) == r.spec.Fanout
	}, "poison-cohort-admission"); err != nil {
		return err
	}
	if r.spec.Probe != protocol.ProbeUnfaulted {
		if err := r.reachFault(ctx, identity, WorkerTargetNone); err != nil {
			return err
		}
	}
	r.recovery.poisonReleaseOnce.Do(func() { close(release) })
	return nil
}

func waitUntil(ctx context.Context, ready func() bool, heartbeat string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, heartbeat)
		}
	}
}

func (r *EpisodeRuntime) runLayeredRetry(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	limit := protectedRequestsPerItemMax
	if r.spec.Probe == protocol.ProbeUnsafe {
		limit = protectedRequestsPerItemMax * 2
	}
	r.mu.Lock()
	previous := r.recovery.lastRequestByItem[identity.WorkItemID]
	firstRequest := r.recovery.requestCountByItem[identity.WorkItemID] + 1
	r.mu.Unlock()
	if previous.Outcome == "ok" && (r.spec.Probe != protocol.ProbeUnsafe || previous.RetryOrdinal == limit) {
		return r.runHealthyRecoveryAgent(ctx, identity, input, workerID)
	}
	for request := firstRequest; request <= limit; request++ {
		owner := "shared-client-budget"
		if r.spec.Probe == protocol.ProbeUnsafe {
			owners := []string{"agent", "client", "activity", "workflow"}
			owner = owners[(request-1)%len(owners)]
		}
		response, err := r.callRecoveryDependency(ctx, identity, owner)
		if err != nil {
			return RecoveryWorkResult{}, err
		}
		if response.Outcome == "ok" && (r.spec.Probe != protocol.ProbeUnsafe || request == limit) {
			return r.runHealthyRecoveryAgent(ctx, identity, input, workerID)
		}
		if err := waitRecoveryDelay(ctx, time.Duration(1+(input.Item.Ordinal+request)%4)*time.Millisecond); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	return RecoveryWorkResult{}, temporal.NewNonRetryableApplicationError("shared retry budget exhausted", "retry_budget", nil)
}

func waitRecoveryDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *EpisodeRuntime) runOutageRecovery(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	select {
	case <-ctx.Done():
		return RecoveryWorkResult{}, ctx.Err()
	case <-r.recovery.outageCoordinatorReady:
	}
	r.mu.Lock()
	restored := r.recovery.outageIsRestored
	alreadyBacklogged := r.recovery.outageInitial[identity.WorkItemID]
	r.mu.Unlock()
	if !restored {
		if alreadyBacklogged {
			return pendingRecoveryResult(input), nil
		}
		response, err := r.callRecoveryDependency(ctx, identity, "outage-shared-budget")
		if err != nil {
			return RecoveryWorkResult{}, err
		}
		if response.Outcome != "outage" {
			return RecoveryWorkResult{}, fmt.Errorf("%w: dependency did not hold outage", protocol.ErrInvalidEvidence)
		}
		r.mu.Lock()
		r.recovery.outageInitial[identity.WorkItemID] = true
		last := len(r.recovery.outageInitial) == r.spec.Fanout
		r.mu.Unlock()
		if last {
			r.restoreOutage(identity)
		}
		return pendingRecoveryResult(input), nil
	}
	var response dependencyServiceResponse
	var err error
	if r.spec.Probe != protocol.ProbeUnsafe {
		if err := waitRecoveryDelay(ctx, time.Duration((input.Item.Ordinal*7)%17)*time.Millisecond); err != nil {
			return RecoveryWorkResult{}, err
		}
		select {
		case <-ctx.Done():
			return RecoveryWorkResult{}, ctx.Err()
		case r.recovery.retryTokens <- struct{}{}:
		}
		response, err = r.callRecoveryDependency(ctx, identity, "outage-catchup")
		<-r.recovery.retryTokens
	} else {
		response, err = r.callRecoveryDependency(ctx, identity, "outage-catchup")
	}
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	if response.Outcome != "ok" {
		return RecoveryWorkResult{}, fmt.Errorf("%w: restored dependency outcome", protocol.ErrInvalidEvidence)
	}
	shouldCrash := false
	r.recovery.outageCrashOnce.Do(func() { shouldCrash = true })
	if shouldCrash && r.spec.Probe != protocol.ProbeUnfaulted {
		if err := r.reachFault(ctx, identity, WorkerTargetWork); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	return r.runHealthyRecoveryAgent(ctx, identity, input, workerID)
}

func pendingRecoveryResult(input RecoveryWorkInput) RecoveryWorkResult {
	return RecoveryWorkResult{
		ItemID: input.Item.ID, Ordinal: input.Item.Ordinal,
		Disposition: protocol.RecoveryDispositionUnresolved, NeedsRecoveryRetry: true,
	}
}

func (r *EpisodeRuntime) restoreOutage(identity protocol.Identity) {
	r.recovery.outageRestoreOnce.Do(func() {
		r.appendEvent(identity, protocol.EventDependencyStateChanged, protocol.DecisionAccepted, map[string]string{
			"state": "exact-backlog", "cardinality": fmt.Sprint(r.spec.Fanout),
		})
		r.recovery.dependency.setOutage(false)
		restoredID := r.appendEvent(identity, protocol.EventDependencyStateChanged, protocol.DecisionAccepted, map[string]string{"state": "restored"})
		r.mu.Lock()
		r.recovery.outageIsRestored = true
		for _, event := range r.events {
			if event.EventID == restoredID {
				r.recovery.outageRestoredOffset = event.MonotonicOffsetNS
				break
			}
		}
		r.mu.Unlock()
		close(r.recovery.outageRestored)
	})
}

func (r *EpisodeRuntime) runPoisonWork(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	response, err := r.callRecoveryDependency(ctx, identity, "poison-activity-budget")
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	if identity.WorkItemID != "item-001" {
		if response.Outcome != "ok" {
			return RecoveryWorkResult{}, fmt.Errorf("%w: healthy poison-cohort response", protocol.ErrInvalidEvidence)
		}
		return r.runHealthyRecoveryAgent(ctx, identity, input, workerID)
	}
	limit := protectedPoisonAttemptsMax
	if r.spec.Probe == protocol.ProbeUnsafe {
		limit = 5
	}
	if response.Outcome != "permanent_failure" {
		return RecoveryWorkResult{}, fmt.Errorf("%w: registered poison response", protocol.ErrInvalidEvidence)
	}
	if identity.ActivityAttempt < limit {
		return RecoveryWorkResult{}, temporal.NewApplicationError("registered deterministic poison", "registered_poison")
	}
	eventID := r.appendEvent(identity, protocol.EventItemQuarantined, protocol.DecisionAccepted, map[string]string{
		"attempts": fmt.Sprint(identity.ActivityAttempt),
	})
	r.setRecoveryTerminal(identity.WorkItemID, protocol.RecoveryDispositionQuarantined, eventID)
	return RecoveryWorkResult{ItemID: input.Item.ID, Ordinal: input.Item.Ordinal, Disposition: protocol.RecoveryDispositionQuarantined}, nil
}

func (r *EpisodeRuntime) runSilentProgress(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	if input.Replacement {
		return r.replaceWedgedAgent(ctx, identity, input, workerID)
	}
	if input.ReleaseWedged {
		return r.releaseWedgedUnsafe(ctx, identity, input)
	}
	if input.Item.ID == "item-001" {
		if r.spec.Probe == protocol.ProbeUnfaulted {
			return r.runHealthyRecoveryAgent(ctx, identity, input, workerID)
		}
		return r.startWedgedAgent(ctx, identity, input, workerID)
	}
	if input.Item.ID == "item-002" {
		r.appendEvent(identity, protocol.EventProgressDeadlineCreated, protocol.DecisionObserved, map[string]string{
			"declared_wait": "true", "deadline_ms": fmt.Sprint(progressDeadlineMS),
		})
		if err := waitWithHeartbeat(ctx, 5200*time.Millisecond, "declared-legitimate-wait"); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	return r.runHealthyRecoveryAgent(ctx, identity, input, workerID)
}

func waitWithHeartbeat(ctx context.Context, duration time.Duration, label string) error {
	timer := time.NewTimer(duration)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, label)
		}
	}
}

func (r *EpisodeRuntime) runCrashRecovery(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	if input.Item.ID != "item-001" || r.spec.Probe == protocol.ProbeUnfaulted {
		return r.runHealthyRecoveryAgent(ctx, identity, input, workerID)
	}
	decision, err := r.claimRecoveryWork(ctx, input, workerID, int32(identity.ActivityAttempt))
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	identity.Generation = decision.Lease.Generation
	identity.CapabilityHash = workstore.HashToken(decision.Lease.OwnerToken)
	if identity.ActivityAttempt == 1 && r.spec.Boundary == "claim-accepted-before-process-launch" {
		if err := r.reachFault(ctx, identity, WorkerTargetWork); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	if decision.Action == workstore.ActionComplete {
		if err := r.recordRecoverySnapshot(ctx, identity, decision.Lease.SessionID); err != nil {
			return RecoveryWorkResult{}, err
		}
		if r.spec.Probe == protocol.ProbeUnsafe && r.spec.Boundary == "result-observed-before-activity-completion" {
			if err := r.recordBlindRedelivery(identity); err != nil {
				return RecoveryWorkResult{}, err
			}
		}
		return successfulRecoveryResult(input), nil
	}
	if decision.Action == workstore.ActionAttach {
		if err := r.driveAttachedRecoveryProcess(ctx, identity, input, decision.Lease); err != nil {
			return RecoveryWorkResult{}, err
		}
		if err := r.recordRecoverySnapshot(ctx, identity, decision.Lease.SessionID); err != nil {
			return RecoveryWorkResult{}, err
		}
		return successfulRecoveryResult(input), nil
	}
	blockRegistration := r.spec.Boundary == "process-launched-before-durable-registration" && identity.ActivityAttempt == 1
	launched, err := r.launchRecoveryProcess(ctx, identity, input, decision.Lease, blockRegistration)
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	if blockRegistration {
		if err := r.waitRecoveryArrival(ctx, input.Item.ID, "before-registration/1", launched.Process.PID, launched.Process.StartIdentity); err != nil {
			return RecoveryWorkResult{}, err
		}
		if err := r.reachFault(ctx, launched.Identity, WorkerTargetWork); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	if r.spec.Boundary == "checkpoint-accepted-before-activity-completion" && identity.ActivityAttempt == 1 {
		if err := r.waitRecoveryArrival(ctx, input.Item.ID, "before-effect/1", launched.Process.PID, launched.Process.StartIdentity); err != nil {
			return RecoveryWorkResult{}, err
		}
		checkpointID := r.appendEvent(launched.Identity, protocol.EventCheckpointAccepted, protocol.DecisionAccepted, map[string]string{"checkpoint": "agent-progress"})
		if err := r.reachFault(ctx, launched.Identity, WorkerTargetWork); err != nil {
			return RecoveryWorkResult{}, err
		}
		_ = checkpointID
	}
	if err := r.driveRecoveryProcess(ctx, launched.Identity, decision.Lease, blockRegistration); err != nil {
		return RecoveryWorkResult{}, err
	}
	if identity.ActivityAttempt > 1 && r.spec.Probe == protocol.ProbeUnsafe {
		if err := r.resumeOlderRecoveryProcesses(ctx, input.Item.ID, decision.Lease); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	if err := r.recordRecoverySnapshot(ctx, launched.Identity, decision.Lease.SessionID); err != nil {
		return RecoveryWorkResult{}, err
	}
	if r.spec.Probe == protocol.ProbeUnsafe && identity.ActivityAttempt > 1 {
		if err := r.recordBlindRedelivery(launched.Identity); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	if r.spec.Boundary == "result-observed-before-activity-completion" && identity.ActivityAttempt == 1 {
		if err := r.reachFault(ctx, launched.Identity, WorkerTargetWork); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	return successfulRecoveryResult(input), nil
}

func (r *EpisodeRuntime) runHealthyRecoveryAgent(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	decision, err := r.claimRecoveryWork(ctx, input, workerID, int32(identity.ActivityAttempt))
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	identity.Generation = decision.Lease.Generation
	identity.CapabilityHash = workstore.HashToken(decision.Lease.OwnerToken)
	switch decision.Action {
	case workstore.ActionLaunch:
		launched, launchErr := r.launchRecoveryProcess(ctx, identity, input, decision.Lease, false)
		if launchErr != nil {
			return RecoveryWorkResult{}, launchErr
		}
		if driveErr := r.driveRecoveryProcess(ctx, launched.Identity, decision.Lease, false); driveErr != nil {
			return RecoveryWorkResult{}, driveErr
		}
	case workstore.ActionAttach:
		if driveErr := r.driveAttachedRecoveryProcess(ctx, identity, input, decision.Lease); driveErr != nil {
			return RecoveryWorkResult{}, driveErr
		}
	case workstore.ActionComplete:
	}
	if err := r.recordRecoverySnapshot(ctx, identity, decision.Lease.SessionID); err != nil {
		return RecoveryWorkResult{}, err
	}
	return successfulRecoveryResult(input), nil
}

func successfulRecoveryResult(input RecoveryWorkInput) RecoveryWorkResult {
	return RecoveryWorkResult{ItemID: input.Item.ID, Ordinal: input.Item.Ordinal, Disposition: protocol.RecoveryDispositionSucceeded}
}

func (r *EpisodeRuntime) claimRecoveryWork(
	ctx context.Context,
	input RecoveryWorkInput,
	workerID string,
	attempt int32,
) (workstore.Decision, error) {
	token := r.tokens[input.Authority.Generation]
	if token == "" || workstore.HashToken(token) != input.Authority.CapabilityHash {
		return workstore.Decision{}, fmt.Errorf("%w: recovery Work authority", protocol.ErrInvalidEvidence)
	}
	mode := workstore.ModeFenced
	if r.spec.Probe == protocol.ProbeUnsafe && r.spec.Case != protocol.CaseSilentProgress {
		mode = workstore.ModeUnsafe
	}
	store, err := r.recoveryStore(input.Item.ID)
	if err != nil {
		return workstore.Decision{}, err
	}
	return store.StartOrAttach(ctx, workstore.StartRequest{
		SessionID: r.spec.LogicalOperationID + "/" + input.Item.ID, Mode: mode, CandidateOwner: token,
		WorkerID: workerID, AgentBuild: "topology-recovery-agent-v1", Attempt: attempt,
	})
}

func (r *EpisodeRuntime) claimRecoveryReplacement(
	ctx context.Context,
	input RecoveryWorkInput,
	workerID string,
	activityAttempt int,
) (workstore.Decision, error) {
	gate := r.recovery.replacementGates[input.Item.ID]
	if gate == nil {
		return workstore.Decision{}, fmt.Errorf("%w: recovery replacement gate for %s", protocol.ErrInvalidEvidence, input.Item.ID)
	}
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return workstore.Decision{}, ctx.Err()
	}
	token := r.tokens[input.Authority.Generation]
	if token == "" || workstore.HashToken(token) != input.Authority.CapabilityHash {
		return workstore.Decision{}, fmt.Errorf("%w: recovery replacement authority", protocol.ErrInvalidEvidence)
	}
	store, err := r.recoveryStore(input.Item.ID)
	if err != nil {
		return workstore.Decision{}, err
	}
	sessionID := r.spec.LogicalOperationID + "/" + input.Item.ID
	snapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return workstore.Decision{}, err
	}
	request := workstore.StartRequest{
		SessionID: sessionID, Mode: workstore.ModeFenced, CandidateOwner: token,
		WorkerID: workerID, AgentBuild: "topology-recovery-agent-v1", Attempt: int32(activityAttempt + 1),
	}
	switch {
	case snapshot.ActiveGeneration == input.Authority.Generation && snapshot.ActiveOwnerTokenHash == input.Authority.CapabilityHash:
	case snapshot.ActiveGeneration+1 == input.Authority.Generation &&
		snapshot.ActiveOwnerTokenHash == workstore.HashToken(r.tokens[snapshot.ActiveGeneration]):
		request.ReplaceOwner = true
	default:
		return workstore.Decision{}, fmt.Errorf("%w: recovery replacement active authority", protocol.ErrInvalidEvidence)
	}
	decision, err := store.StartOrAttach(ctx, request)
	if err != nil {
		return workstore.Decision{}, err
	}
	if decision.Lease.Generation != input.Authority.Generation ||
		workstore.HashToken(decision.Lease.OwnerToken) != input.Authority.CapabilityHash {
		return workstore.Decision{}, fmt.Errorf("%w: recovery replacement decision authority", protocol.ErrInvalidEvidence)
	}
	return decision, nil
}

func (r *EpisodeRuntime) recordSilentProgressRevocation(wedge recoveryWedge, replacement workstore.Lease) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recovery.detectionEventID != "" {
		return r.recovery.detectionEventID
	}
	revokeID := r.appendEventLocked(wedge.identity, protocol.EventAuthorityRevoked, protocol.DecisionAccepted, map[string]string{
		"reason":                      "progress-deadline-expired",
		"replacement_generation":      fmt.Sprint(replacement.Generation),
		"replacement_capability_hash": workstore.HashToken(replacement.OwnerToken),
	}, r.recovery.deadlineEventID)
	r.recovery.detectionEventID = revokeID
	return revokeID
}

func (r *EpisodeRuntime) launchRecoveryProcess(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	lease workstore.Lease,
	blockRegistration bool,
) (agent.Launched, error) {
	barrier := r.barriers[input.Item.ID]
	if barrier == nil {
		return agent.Launched{}, fmt.Errorf("%w: recovery item barrier", protocol.ErrInvalidEvidence)
	}
	r.mu.Lock()
	parentWorkflow, parentRun := r.parentWorkflow, r.parentRun
	r.mu.Unlock()
	store, err := r.recoveryStore(input.Item.ID)
	if err != nil {
		return agent.Launched{}, err
	}
	request := agent.Request{
		Manifest: r.manifest, WorkItemID: input.Item.ID, Lease: lease,
		ParentWorkflowID: parentWorkflow, ParentRunID: parentRun, ActivityID: identity.ActivityID,
		ActivityAttempt: identity.ActivityAttempt, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID,
		StorePath: store.Path(), BarrierURL: barrier.url, EffectValue: "effect/" + input.Item.ID,
		OutcomeValue: "outcome/" + input.Item.ID, BlockBeforeRegistration: blockRegistration,
	}
	if r.spec.Topology == protocol.TopologyChildWorkflow {
		request.ChildWorkflowID, request.ChildRunID = identity.ChildWorkflowID, identity.ChildRunID
	}
	launched, err := r.launcher.Launch(ctx, request)
	if err != nil {
		return agent.Launched{}, err
	}
	r.mu.Lock()
	r.controlledProcesses[launched.Process.PID] = controlledProcess{
		lease: lease, process: launched.Process, blockedBeforeRegistration: blockRegistration,
	}
	r.recovery.items[input.Item.ID].processes[launched.Identity.ProcessIdentity] = true
	r.mu.Unlock()
	eventID := r.appendEvent(launched.Identity, protocol.EventProcessStarted, protocol.DecisionObserved, map[string]string{
		"pid": fmt.Sprint(launched.Process.PID), "process_start": launched.Process.StartIdentity,
	})
	r.mu.Lock()
	r.processes = append(r.processes, protocol.ProcessObservation{
		EventID: eventID, WorkItemID: input.Item.ID, WorkerID: launched.Identity.WorkerID,
		WorkerPID: launched.Identity.WorkerPID, ProcessIdentity: launched.Identity.ProcessIdentity, State: "running",
	})
	r.mu.Unlock()
	return launched, nil
}

func (r *EpisodeRuntime) driveRecoveryProcess(
	ctx context.Context,
	identity protocol.Identity,
	lease workstore.Lease,
	blockedBeforeRegistration bool,
) error {
	barrier := r.barriers[identity.WorkItemID]
	if blockedBeforeRegistration {
		if err := barrier.coordinator.Release(fmt.Sprintf("before-registration/%d", lease.Generation)); err != nil {
			return err
		}
	}
	if _, err := waitForArrivalsWithHeartbeat(ctx, barrier.coordinator, fmt.Sprintf("before-effect/%d", lease.Generation), 1); err != nil {
		return err
	}
	if err := barrier.coordinator.Release(fmt.Sprintf("before-effect/%d", lease.Generation)); err != nil {
		return err
	}
	if _, err := waitForArrivalsWithHeartbeat(ctx, barrier.coordinator, fmt.Sprintf("before-completion/%d", lease.Generation), 1); err != nil {
		return err
	}
	if err := barrier.coordinator.Release(fmt.Sprintf("before-completion/%d", lease.Generation)); err != nil {
		return err
	}
	_, err := r.waitForRecoveryLeaseDisposition(ctx, identity.WorkItemID, lease)
	return err
}

func (r *EpisodeRuntime) waitForRecoveryLeaseDisposition(
	ctx context.Context,
	itemID string,
	lease workstore.Lease,
) (workstore.Snapshot, error) {
	store, err := r.recoveryStore(itemID)
	if err != nil {
		return workstore.Snapshot{}, err
	}
	return waitForStoreLeaseDisposition(ctx, store, lease)
}

func (r *EpisodeRuntime) driveAttachedRecoveryProcess(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	lease workstore.Lease,
) error {
	r.mu.Lock()
	controlled, found := r.controlledProcessForLeaseLocked(lease)
	r.mu.Unlock()
	if !found {
		store, storeErr := r.recoveryStore(input.Item.ID)
		if storeErr != nil {
			return storeErr
		}
		snapshot, err := store.Snapshot(ctx, lease.SessionID)
		if err != nil {
			return err
		}
		if snapshot.Outcome != nil {
			return nil
		}
		for _, executor := range snapshot.Executors {
			if executor.Generation == lease.Generation && executor.OwnerTokenHash == workstore.HashToken(lease.OwnerToken) &&
				executor.Status == workstore.ExecutorStatusLaunchPending {
				launched, launchErr := r.launchRecoveryProcess(ctx, identity, input, lease, false)
				if launchErr != nil {
					return launchErr
				}
				return r.driveRecoveryProcess(ctx, launched.Identity, lease, false)
			}
		}
		return fmt.Errorf("%w: attached recovery process identity", protocol.ErrInvalidEvidence)
	}
	identity.ProcessIdentity = fmt.Sprintf("pid:%d/start:%s", controlled.process.PID, controlled.process.StartIdentity)
	r.mu.Lock()
	blockedBeforeRegistration := r.claimRegistrationReleaseLocked(lease)
	r.mu.Unlock()
	return r.driveRecoveryProcess(ctx, identity, lease, blockedBeforeRegistration)
}

func (r *EpisodeRuntime) controlledProcessForLeaseLocked(lease workstore.Lease) (controlledProcess, bool) {
	for _, controlled := range r.controlledProcesses {
		if controlled.lease == lease {
			return controlled, true
		}
	}
	return controlledProcess{}, false
}

func (r *EpisodeRuntime) claimRegistrationReleaseLocked(lease workstore.Lease) bool {
	for pid, controlled := range r.controlledProcesses {
		if controlled.lease == lease && controlled.blockedBeforeRegistration {
			controlled.blockedBeforeRegistration = false
			r.controlledProcesses[pid] = controlled
			return true
		}
	}
	return false
}

func (r *EpisodeRuntime) resumeOlderRecoveryProcesses(ctx context.Context, itemID string, current workstore.Lease) error {
	r.mu.Lock()
	older := make([]controlledProcess, 0)
	for _, controlled := range r.controlledProcesses {
		if controlled.lease.SessionID == current.SessionID && controlled.lease != current {
			older = append(older, controlled)
		}
	}
	r.mu.Unlock()
	for _, controlled := range older {
		identity := r.identityForControlled(itemID, controlled)
		r.mu.Lock()
		blockedBeforeRegistration := r.claimRegistrationReleaseLocked(controlled.lease)
		r.mu.Unlock()
		if err := r.driveRecoveryProcess(ctx, identity, controlled.lease, blockedBeforeRegistration); err != nil &&
			!errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

func (r *EpisodeRuntime) identityForControlled(itemID string, controlled controlledProcess) protocol.Identity {
	r.mu.Lock()
	parentWorkflow, parentRun := r.parentWorkflow, r.parentRun
	r.mu.Unlock()
	return protocol.Identity{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: r.spec.RunID, PairID: r.spec.PairID,
		ScheduleBlockID: r.spec.ScheduleBlockID, TrackerBeadID: r.manifest.TrackerBeadID, Topology: r.spec.Topology,
		Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
		LogicalOperationID: r.spec.LogicalOperationID, WorkItemID: itemID, Generation: controlled.lease.Generation,
		CapabilityHash: workstore.HashToken(controlled.lease.OwnerToken), ParentWorkflowID: parentWorkflow, ParentRunID: parentRun,
		ActivityID: "recovered-process/" + itemID, ActivityAttempt: 1, WorkerID: "recovery-controller", WorkerPID: os.Getpid(),
		ProcessIdentity: fmt.Sprintf("pid:%d/start:%s", controlled.process.PID, controlled.process.StartIdentity),
	}
}

func (r *EpisodeRuntime) waitRecoveryArrival(
	ctx context.Context,
	itemID, point string,
	pid int,
	processStart string,
) error {
	arrivals, err := waitForArrivalsWithHeartbeat(ctx, r.barriers[itemID].coordinator, point, 1)
	if err != nil {
		return err
	}
	for _, arrival := range arrivals {
		if arrival.PID == pid && arrival.ProcessStart == processStart {
			return nil
		}
	}
	return fmt.Errorf("%w: exact recovery process arrival", protocol.ErrInvalidEvidence)
}

func (r *EpisodeRuntime) recordRecoverySnapshot(ctx context.Context, identity protocol.Identity, sessionID string) error {
	store, err := r.recoveryStore(identity.WorkItemID)
	if err != nil {
		return err
	}
	snapshot, err := store.Snapshot(ctx, sessionID)
	if err != nil {
		return err
	}
	resultCount := 0
	for _, event := range snapshot.Events {
		if event.Kind == "outcome_accepted" || event.Kind == "completion_duplicate" {
			resultCount++
		}
	}
	r.mu.Lock()
	prior := r.recovery.recordedBySession[sessionID]
	r.mu.Unlock()
	for index := prior.effects; index < len(snapshot.Effects); index++ {
		effect := snapshot.Effects[index]
		effectIdentity := identity
		effectIdentity.Generation, effectIdentity.CapabilityHash = effect.Generation, effect.OwnerTokenHash
		eventID := r.appendEvent(effectIdentity, protocol.EventEffectAccepted, protocol.DecisionAccepted, map[string]string{
			"effect_id": effect.ID, "source": "workstore",
		})
		r.mu.Lock()
		r.destinationActions = append(r.destinationActions, protocol.DestinationAction{
			EventID: eventID, WorkItemID: identity.WorkItemID, LogicalEffectID: effect.ID,
			ReceiptID:  fmt.Sprintf("workstore/%s/effect-%03d", identity.WorkItemID, index+1),
			Generation: effect.Generation, CapabilityHash: effect.OwnerTokenHash, Decision: protocol.DecisionAccepted, Applied: true,
		})
		r.recovery.items[identity.WorkItemID].acceptedEffects++
		r.mu.Unlock()
	}
	for index := prior.results; index < resultCount; index++ {
		eventID := r.appendEvent(identity, protocol.EventResultAccepted, protocol.DecisionAccepted, map[string]string{
			"physical_result_ordinal": fmt.Sprint(index + 1), "source": "workstore",
		})
		r.mu.Lock()
		item := r.recovery.items[identity.WorkItemID]
		item.acceptedResults++
		if item.terminalEventID == "" {
			r.markRecoveryTerminalLocked(identity.WorkItemID, protocol.RecoveryDispositionSucceeded, eventID)
		}
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.recovery.recordedBySession[sessionID] = recoverySnapshotCount{effects: len(snapshot.Effects), results: resultCount}
	r.mu.Unlock()
	return nil
}

func (r *EpisodeRuntime) setRecoveryTerminal(itemID, disposition, eventID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markRecoveryTerminalLocked(itemID, disposition, eventID)
}

func (r *EpisodeRuntime) markRecoveryTerminalLocked(itemID, disposition, eventID string) {
	item := r.recovery.items[itemID]
	item.disposition, item.terminalEventID = disposition, eventID
	item.terminalOffset = r.events[len(r.events)-1].MonotonicOffsetNS
	if item.admitted && !r.recovery.terminalAccounted[itemID] {
		r.recovery.terminalAccounted[itemID] = true
		r.recovery.admittedOutstanding--
	}
}

func (r *EpisodeRuntime) recordBlindRedelivery(identity protocol.Identity) error {
	r.mu.Lock()
	item := r.recovery.items[identity.WorkItemID]
	if item.acceptedResults > 1 {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	effectID := r.spec.LogicalOperationID + "/" + identity.WorkItemID + "/effect"
	effectEvent := r.appendEvent(identity, protocol.EventEffectAccepted, protocol.DecisionAccepted, map[string]string{"source": "blind-redelivery-control"})
	resultEvent := r.appendEvent(identity, protocol.EventResultAccepted, protocol.DecisionAccepted, map[string]string{"source": "blind-redelivery-control"}, effectEvent)
	r.mu.Lock()
	r.destinationActions = append(r.destinationActions, protocol.DestinationAction{
		EventID: effectEvent, WorkItemID: identity.WorkItemID, LogicalEffectID: effectID,
		ReceiptID: "unsafe-redelivery/" + identity.WorkItemID, Generation: identity.Generation,
		CapabilityHash: identity.CapabilityHash, Decision: protocol.DecisionAccepted, Applied: true,
	})
	item = r.recovery.items[identity.WorkItemID]
	item.acceptedEffects++
	item.acceptedResults++
	item.processes["blind-redelivery-without-stable-session"] = true
	if item.terminalEventID == "" {
		r.markRecoveryTerminalLocked(identity.WorkItemID, protocol.RecoveryDispositionSucceeded, resultEvent)
	}
	r.mu.Unlock()
	return nil
}

func (r *EpisodeRuntime) startWedgedAgent(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	decision, err := r.claimRecoveryWork(ctx, input, workerID, int32(identity.ActivityAttempt))
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	launched, err := r.launchRecoveryProcess(ctx, identity, input, decision.Lease, false)
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	point := fmt.Sprintf("before-effect/%d", decision.Lease.Generation)
	if err := r.waitRecoveryArrival(ctx, input.Item.ID, point, launched.Process.PID, launched.Process.StartIdentity); err != nil {
		return RecoveryWorkResult{}, err
	}
	progressID := r.appendEvent(launched.Identity, protocol.EventProgressAccepted, protocol.DecisionAccepted, map[string]string{"phase": "registered-progress"})
	deadlineID := r.appendEvent(launched.Identity, protocol.EventProgressDeadlineCreated, protocol.DecisionObserved, map[string]string{
		"deadline_ms": fmt.Sprint(progressDeadlineMS), "declared_wait": "false",
	}, progressID)
	r.mu.Lock()
	r.recovery.wedgeLeaseByItem[input.Item.ID] = recoveryWedge{
		identity: launched.Identity, leaseGeneration: decision.Lease.Generation, beforeEffectPoint: point,
	}
	r.recovery.progressEventID, r.recovery.deadlineEventID = progressID, deadlineID
	r.mu.Unlock()
	if r.spec.Probe != protocol.ProbeUnfaulted {
		if err := r.reachFault(ctx, launched.Identity, WorkerTargetNone); err != nil {
			return RecoveryWorkResult{}, err
		}
	}
	return RecoveryWorkResult{
		ItemID: input.Item.ID, Ordinal: input.Item.Ordinal,
		Disposition: protocol.RecoveryDispositionUnresolved, NeedsReplacement: true,
	}, nil
}

func (r *EpisodeRuntime) replaceWedgedAgent(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
	workerID string,
) (RecoveryWorkResult, error) {
	r.mu.Lock()
	wedge, exists := r.recovery.wedgeLeaseByItem[input.Item.ID]
	r.mu.Unlock()
	if !exists {
		return RecoveryWorkResult{}, fmt.Errorf("%w: missing wedged process", protocol.ErrInvalidEvidence)
	}
	decision, err := r.claimRecoveryReplacement(ctx, input, workerID, identity.ActivityAttempt)
	if err != nil {
		return RecoveryWorkResult{}, err
	}
	identity.Generation = decision.Lease.Generation
	identity.CapabilityHash = workstore.HashToken(decision.Lease.OwnerToken)
	revokeID := r.recordSilentProgressRevocation(wedge, decision.Lease)
	switch decision.Action {
	case workstore.ActionLaunch:
		launched, launchErr := r.launchRecoveryProcess(ctx, identity, input, decision.Lease, false)
		if launchErr != nil {
			return RecoveryWorkResult{}, launchErr
		}
		identity = launched.Identity
		if driveErr := r.driveRecoveryProcess(ctx, identity, decision.Lease, false); driveErr != nil {
			return RecoveryWorkResult{}, driveErr
		}
	case workstore.ActionAttach:
		if driveErr := r.driveAttachedRecoveryProcess(ctx, identity, input, decision.Lease); driveErr != nil {
			return RecoveryWorkResult{}, driveErr
		}
	case workstore.ActionComplete:
	default:
		return RecoveryWorkResult{}, fmt.Errorf("%w: recovery replacement action", protocol.ErrInvalidEvidence)
	}
	if err := r.recordRecoverySnapshot(ctx, identity, decision.Lease.SessionID); err != nil {
		return RecoveryWorkResult{}, err
	}
	r.mu.Lock()
	r.recovery.replacementEventID = r.recovery.items[input.Item.ID].terminalEventID
	oldControlled, found := r.controlledProcessForLeaseLocked(workstore.Lease{
		SessionID: decision.Lease.SessionID, Generation: wedge.leaseGeneration, OwnerToken: r.tokens[1],
	})
	r.mu.Unlock()
	if !found {
		return RecoveryWorkResult{}, fmt.Errorf("%w: wedged process control identity", protocol.ErrInvalidEvidence)
	}
	oldIdentity := r.identityForControlled(input.Item.ID, oldControlled)
	if err := r.driveRecoveryProcess(ctx, oldIdentity, oldControlled.lease, false); err != nil && !errors.Is(err, workstore.ErrStaleOwner) {
		return RecoveryWorkResult{}, err
	}
	rejectedID := r.appendEvent(oldIdentity, protocol.EventEffectRejected, protocol.DecisionRejected, map[string]string{"reason": "stale_generation"}, revokeID)
	r.mu.Lock()
	r.destinationActions = append(r.destinationActions, protocol.DestinationAction{
		EventID: rejectedID, WorkItemID: input.Item.ID,
		LogicalEffectID: r.spec.LogicalOperationID + "/" + input.Item.ID + "/effect",
		Generation:      oldControlled.lease.Generation, CapabilityHash: workstore.HashToken(oldControlled.lease.OwnerToken),
		Decision: protocol.DecisionRejected,
	})
	r.mu.Unlock()
	return successfulRecoveryResult(input), nil
}

func (r *EpisodeRuntime) releaseWedgedUnsafe(
	ctx context.Context,
	identity protocol.Identity,
	input RecoveryWorkInput,
) (RecoveryWorkResult, error) {
	r.mu.Lock()
	wedge, exists := r.recovery.wedgeLeaseByItem[input.Item.ID]
	controlled, found := r.controlledProcessForLeaseLocked(workstore.Lease{
		SessionID:  r.spec.LogicalOperationID + "/" + input.Item.ID,
		Generation: wedge.leaseGeneration, OwnerToken: r.tokens[1],
	})
	r.mu.Unlock()
	if !exists || !found {
		return RecoveryWorkResult{}, fmt.Errorf("%w: unsafe wedge release", protocol.ErrInvalidEvidence)
	}
	detectionID := r.appendEvent(wedge.identity, protocol.EventRecoveryObserved, protocol.DecisionFailed, map[string]string{"reason": "deadline-missed"})
	r.mu.Lock()
	r.recovery.detectionEventID = detectionID
	r.mu.Unlock()
	oldIdentity := r.identityForControlled(input.Item.ID, controlled)
	if err := r.driveRecoveryProcess(ctx, oldIdentity, controlled.lease, false); err != nil {
		return RecoveryWorkResult{}, err
	}
	if err := r.recordRecoverySnapshot(ctx, oldIdentity, controlled.lease.SessionID); err != nil {
		return RecoveryWorkResult{}, err
	}
	return successfulRecoveryResult(input), nil
}
