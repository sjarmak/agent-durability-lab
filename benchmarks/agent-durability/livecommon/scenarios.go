package livecommon

import (
	"errors"
	"fmt"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func (h *harness) runUnfaulted() error {
	decision, err := h.start(workstore.ModeFenced, 1, false)
	if err != nil {
		return err
	}
	agent, err := h.launch(decision, "agent-1", false)
	if err != nil {
		return err
	}
	h.recordRegistered(agent, "running")
	if err := h.releaseEffect(agent); err != nil {
		return err
	}
	h.recorder.effect(protocol.EventEffectAccepted, agent.actor, agent.lease.Generation,
		processIdentityString(agent.process), "attempt-1", "accepted", true)
	if err := h.releaseCompletion(agent, true); err != nil {
		return err
	}
	h.recorder.outcome(agent.actor, agent.lease.Generation, processIdentityString(agent.process))
	return nil
}

func (h *harness) runSurvivingExecutor() error {
	mode := workstore.ModeUnsafe
	if h.config.Probe == protocol.ProbeProtected {
		mode = workstore.ModeReattach
	}
	firstDecision, err := h.start(mode, 1, false)
	if err != nil {
		return err
	}
	first, err := h.launch(firstDecision, "agent-1", false)
	if err != nil {
		return err
	}
	h.recordRegistered(first, "running")
	barrier := h.recorder.add(protocol.EventBarrierReached, first.actor, first.lease.Generation,
		processIdentityString(first.process), "", "", "blocked")
	h.recorder.markFault("worker-died-after-agent-registration", first.actor, processIdentityString(first.process), barrier)
	second, hasSecond, err := h.survivingRetry(mode, first)
	if err != nil {
		return err
	}
	if err := h.recorder.requireNextFaultEvent(); err != nil {
		return err
	}
	return h.finishSurviving(first, second, hasSecond)
}

func (h *harness) survivingRetry(mode workstore.Mode, first launchedAgent) (launchedAgent, bool, error) {
	retry, err := h.start(mode, 2, false)
	if err != nil {
		return launchedAgent{}, false, err
	}
	if h.config.Probe == protocol.ProbeProtected {
		if retry.Action != workstore.ActionAttach || retry.Lease != first.lease {
			return launchedAgent{}, false, fmt.Errorf("protected retry = %+v, want attach to generation 1", retry)
		}
		h.recorder.add(protocol.EventExecutorAttached, first.actor, first.lease.Generation,
			processIdentityString(first.process), "", "", "observed")
		return launchedAgent{}, false, nil
	}
	second, err := h.launch(retry, "agent-2", false)
	if err != nil {
		return launchedAgent{}, false, err
	}
	h.recordRegistered(second, "running")
	h.recorder.authority.ConcurrentOwnerCount = 2
	return second, true, nil
}

func (h *harness) finishSurviving(first, second launchedAgent, hasSecond bool) error {
	if err := h.releaseEffect(first); err != nil {
		return err
	}
	h.recorder.effect(protocol.EventEffectAccepted, first.actor, first.lease.Generation,
		processIdentityString(first.process), "attempt-1", "accepted", true)
	if hasSecond {
		if err := h.releaseEffect(second); err != nil {
			return err
		}
		h.recorder.effect(protocol.EventEffectAccepted, second.actor, second.lease.Generation,
			processIdentityString(second.process), "attempt-2", "accepted", true)
	}
	if err := h.releaseCompletion(first, true); err != nil {
		return err
	}
	h.recorder.outcome(first.actor, first.lease.Generation, processIdentityString(first.process))
	if hasSecond {
		if err := h.releaseCompletion(second, false); err != nil {
			return err
		}
	}
	return nil
}

func (h *harness) runAmbiguousEffect() error {
	mode := workstore.ModeUnsafe
	if h.config.Probe == protocol.ProbeProtected {
		mode = workstore.ModeFenced
	}
	firstDecision, err := h.start(mode, 1, false)
	if err != nil {
		return err
	}
	first, err := h.launch(firstDecision, "agent-1", false)
	if err != nil {
		return err
	}
	h.recordRegistered(first, "running")
	if err := h.releaseEffect(first); err != nil {
		return err
	}
	effect := h.recorder.effect(protocol.EventEffectAccepted, first.actor, first.lease.Generation,
		processIdentityString(first.process), "attempt-1", "accepted", true)
	h.recorder.markFault("effect-confirmed-before-step-completion", first.actor, processIdentityString(first.process), effect)
	if err := h.killAtBarrier(first, "before-completion"); err != nil {
		return err
	}

	retry, err := h.start(mode, 2, h.config.Probe == protocol.ProbeProtected)
	if err != nil {
		return err
	}
	second, err := h.launch(retry, "agent-2", false)
	if err != nil {
		return err
	}
	h.recorder.authority.ActiveGeneration = second.lease.Generation
	return h.finishAmbiguous(second)
}

func (h *harness) finishAmbiguous(second launchedAgent) error {
	if h.config.Probe == protocol.ProbeUnsafe {
		if err := h.releaseEffect(second); err != nil {
			return err
		}
		h.recorder.effect(protocol.EventEffectAccepted, second.actor, second.lease.Generation,
			processIdentityString(second.process), "attempt-2", "accepted", true)
		if err := h.recorder.requireNextFaultEvent(); err != nil {
			return err
		}
		if err := h.releaseCompletion(second, true); err != nil {
			return err
		}
		h.recorder.outcome(second.actor, second.lease.Generation, processIdentityString(second.process))
		return nil
	}

	h.recorder.effect(protocol.EventEffectRejected, second.actor, second.lease.Generation,
		processIdentityString(second.process), "attempt-2", "duplicate", false)
	if err := h.recorder.requireNextFaultEvent(); err != nil {
		return err
	}
	if err := h.store.Complete(h.ctx, second.lease, workstore.Outcome{
		Value: "live outcome", ArtifactRef: "artifact://live-common",
	}); err != nil {
		return fmt.Errorf("complete reconciled effect: %w", err)
	}
	h.recorder.outcome(second.actor, second.lease.Generation, processIdentityString(second.process))
	return h.killAtBarrier(second, "before-effect")
}

func (h *harness) runStaleGeneration() error {
	mode := workstore.ModeUnsafe
	if h.config.Probe == protocol.ProbeProtected {
		mode = workstore.ModeFenced
	}
	firstDecision, err := h.start(mode, 1, false)
	if err != nil {
		return err
	}
	first, err := h.launch(firstDecision, "agent-1", false)
	if err != nil {
		return err
	}
	h.recordRegistered(first, "running")

	secondDecision, err := h.start(mode, 2, h.config.Probe == protocol.ProbeProtected)
	if err != nil {
		return err
	}
	second, err := h.launch(secondDecision, "agent-2", false)
	if err != nil {
		return err
	}
	secondProcess, replaced := h.recordReplacement(second)
	h.recorder.markFault("replacement-committed-before-stale-actions", first.actor,
		processIdentityString(first.process), replaced)

	if err := h.releaseEffect(first); err != nil {
		return err
	}
	if h.config.Probe == protocol.ProbeUnsafe {
		h.recorder.effect(protocol.EventEffectAccepted, first.actor, first.lease.Generation,
			processIdentityString(first.process), "stale-attempt", "accepted", true)
	} else {
		h.recorder.effect(protocol.EventEffectRejected, first.actor, first.lease.Generation,
			processIdentityString(first.process), "stale-attempt", "stale_generation", false)
	}
	if err := h.recorder.requireNextFaultEvent(); err != nil {
		return err
	}
	if err := h.killAtBarrier(first, "before-completion"); err != nil {
		return err
	}
	return h.finishStaleGeneration(first, second, secondProcess)
}

func (h *harness) recordReplacement(second launchedAgent) (string, uint64) {
	process := processIdentityString(second.process)
	sequence := h.recorder.add(protocol.EventOwnerReplaced, second.actor, second.lease.Generation,
		process, "", "", "accepted")
	h.recorder.processes = append(h.recorder.processes, protocol.ProcessObservation{
		Sequence: sequence, ActorID: second.actor, Generation: second.lease.Generation,
		ProcessIdentity: process, State: "running",
	})
	h.recorder.authority.ActiveGeneration = second.lease.Generation
	return process, sequence
}

func (h *harness) finishStaleGeneration(first, second launchedAgent, secondProcess string) error {
	if h.config.Probe == protocol.ProbeUnsafe {
		h.recorder.acceptedStaleAction(protocol.EventStaleStop, "stale-stop", first.actor,
			first.lease.Generation, processIdentityString(first.process))
		if err := h.killAtBarrier(second, "before-effect"); err != nil {
			return err
		}
		h.recorder.authority.CurrentOwnerAlive = false
		return nil
	}

	h.recorder.add(protocol.EventStaleCompletion, first.actor, first.lease.Generation,
		processIdentityString(first.process), "", "", "rejected")
	h.recorder.add(protocol.EventStaleStop, first.actor, first.lease.Generation,
		processIdentityString(first.process), "", "", "rejected")
	if err := h.releaseEffect(second); err != nil {
		return err
	}
	h.recorder.effect(protocol.EventEffectAccepted, second.actor, second.lease.Generation,
		secondProcess, "attempt-2", "accepted", true)
	if err := h.releaseCompletion(second, true); err != nil {
		return err
	}
	h.recorder.outcome(second.actor, second.lease.Generation, secondProcess)
	return nil
}

func (h *harness) runCancellationUnreachable() error {
	decision, err := h.start(workstore.ModeFenced, 1, false)
	if err != nil {
		return err
	}
	agent, err := h.launch(decision, "agent-1", h.config.Probe == protocol.ProbeUnsafe)
	if err != nil {
		return err
	}
	if err := h.signal(agent, agentprocess.SignalStop); err != nil {
		return fmt.Errorf("freeze exact process: %w", err)
	}
	registered := h.recordRegistered(agent, "frozen")
	h.recorder.markFault("process-frozen-before-cancellation", agent.actor,
		processIdentityString(agent.process), registered)
	cancelDecision, err := h.store.CancelSession(h.ctx, workstore.CancelRequest{
		SessionID: h.recorder.identity.SessionID, RequestID: "cancel-1",
	})
	if err != nil {
		return err
	}
	if cancelDecision.Action != workstore.CancelActionCommitted {
		return fmt.Errorf("cancellation action = %q, want committed", cancelDecision.Action)
	}
	canceled := h.recorder.add(protocol.EventCancellationCommitted, "", 0, "", "", "", "accepted")
	h.recorder.authority.CancellationCommitted = true
	h.recorder.authority.CancellationSequence = canceled
	h.recorder.authority.CurrentOwnerAlive = false
	if err := h.recorder.requireNextFaultEvent(); err != nil {
		return err
	}
	if err := h.signal(agent, agentprocess.SignalContinue); err != nil {
		return fmt.Errorf("resume exact process: %w", err)
	}
	if err := h.releaseEffect(agent); err != nil {
		return err
	}
	h.recordCancellationEffect(agent)
	if err := h.releaseCompletion(agent, false); err != nil {
		return err
	}
	if _, err := h.start(workstore.ModeFenced, 2, true); !errors.Is(err, workstore.ErrSessionCanceled) {
		return fmt.Errorf("post-cancel replacement = %v, want ErrSessionCanceled", err)
	}
	h.recorder.add(protocol.EventReplacementRejected, "agent-2", 2,
		"pid:unlaunched:start:none", "", "", "canceled")
	return nil
}

func (h *harness) recordCancellationEffect(agent launchedAgent) {
	if h.config.Probe == protocol.ProbeUnsafe {
		h.recorder.effect(protocol.EventEffectAccepted, agent.actor, agent.lease.Generation,
			processIdentityString(agent.process), "post-cancel-attempt", "accepted", true)
	} else {
		h.recorder.effect(protocol.EventEffectRejected, agent.actor, agent.lease.Generation,
			processIdentityString(agent.process), "post-cancel-attempt", "canceled", false)
	}
}

func (h *harness) recordRegistered(agent launchedAgent, state string) uint64 {
	return h.recorder.register(agent.actor, agent.lease.Generation,
		processIdentityString(agent.process), "observed", state)
}

func (h *harness) releaseEffect(agent launchedAgent) error {
	point := fmt.Sprintf("before-effect/%d", agent.lease.Generation)
	if err := h.release(point); err != nil {
		return err
	}
	next := fmt.Sprintf("before-completion/%d", agent.lease.Generation)
	if _, err := h.coordinator.WaitForArrivals(h.ctx, next, 1); err != nil {
		return fmt.Errorf("wait for %s: %w", next, err)
	}
	return nil
}

func (h *harness) releaseCompletion(agent launchedAgent, expectOutcome bool) error {
	point := fmt.Sprintf("before-completion/%d", agent.lease.Generation)
	if err := h.release(point); err != nil {
		return err
	}
	if expectOutcome {
		if _, err := h.waitForSnapshot(func(snapshot workstore.Snapshot) bool {
			return snapshot.Outcome != nil
		}); err != nil {
			return fmt.Errorf("wait for outcome: %w", err)
		}
	}
	return h.waitGone(agent)
}

func (h *harness) killAtBarrier(agent launchedAgent, phase string) error {
	if err := h.signal(agent, agentprocess.SignalKill); err != nil {
		return fmt.Errorf("kill exact process at %s: %w", phase, err)
	}
	if err := h.waitGone(agent); err != nil {
		return err
	}
	point := fmt.Sprintf("%s/%d", phase, agent.lease.Generation)
	if err := h.release(point); err != nil {
		return err
	}
	return nil
}
