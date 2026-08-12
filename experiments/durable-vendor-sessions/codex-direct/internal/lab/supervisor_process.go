package lab

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

const supervisedTerminationGrace = 5 * time.Second

var (
	errSupervisedProcessControl        = errors.New("supervised process control failed")
	errSupervisedExecutionDidNotExit   = errors.New("supervised execution did not exit after process termination")
	errSupervisedTerminationUnverified = errors.New("supervised process termination was not verified")
)

type supervisedInvocationOutcome struct {
	result InvocationResult
	err    error
}

func RunSupervisedInvocation(ctx context.Context, store *workstore.Store, lease workstore.Lease,
	invocation Invocation, input RunInvocationInput, hooks StreamHooks,
) (InvocationResult, error) {
	if ctx == nil || store == nil || lease.SessionID == "" || lease.Generation == 0 || lease.OwnerToken == "" {
		return InvocationResult{}, fmt.Errorf("%w: context, store, and complete lease are required", workstore.ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return InvocationResult{}, err
	}
	processReady := make(chan ProcessRecord, 1)
	originalProcessHook := hooks.ProcessStarted
	hooks.ProcessStarted = func(process ProcessRecord) error {
		processReady <- process
		operationContext := context.WithoutCancel(ctx)
		if err := store.RegisterProcess(operationContext, lease, workstore.Process{
			PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
		}); err != nil {
			return err
		}
		if originalProcessHook != nil {
			return originalProcessHook(process)
		}
		return nil
	}
	completed := make(chan supervisedInvocationOutcome, 1)
	go func() {
		result, err := RunInvocation(context.WithoutCancel(ctx), invocation, input, hooks)
		completed <- supervisedInvocationOutcome{result: result, err: err}
	}()
	select {
	case outcome := <-completed:
		return outcome.result, outcome.err
	case <-ctx.Done():
	}
	var process ProcessRecord
	select {
	case outcome := <-completed:
		return outcome.result, errors.Join(ctx.Err(), outcome.err)
	case process = <-processReady:
	}
	controlErr := signalSupervisedProcess(process, lease, agentprocess.SignalTerminate)
	timer := time.NewTimer(supervisedTerminationGrace)
	defer timer.Stop()
	select {
	case outcome := <-completed:
		groupContext, cancel := context.WithTimeout(context.Background(), time.Second)
		groupErr := ensureSupervisedProcessGroupGone(groupContext, process)
		cancel()
		return outcome.result, errors.Join(ctx.Err(), wrapSupervisedProcessControl(controlErr), groupErr, outcome.err)
	case <-timer.C:
		controlErr = errors.Join(controlErr, signalSupervisedProcessGroup(process, agentprocess.SignalKill))
		goneContext, cancel := context.WithTimeout(context.Background(), time.Second)
		goneErr := wrapSupervisedTerminationVerification(waitForSupervisedProcessGroupGone(goneContext, process))
		cancel()
		select {
		case outcome := <-completed:
			return outcome.result, errors.Join(ctx.Err(), wrapSupervisedProcessControl(controlErr), goneErr, outcome.err)
		case <-time.After(time.Second):
			return InvocationResult{}, errors.Join(ctx.Err(), wrapSupervisedProcessControl(controlErr), goneErr,
				errSupervisedExecutionDidNotExit)
		}
	}
}

func ensureSupervisedProcessGroupGone(ctx context.Context, process ProcessRecord) error {
	disposition, err := probeSupervisedProcessGroup(process)
	if supervisedProcessGroupTerminationVerified(disposition, err) {
		return nil
	}
	if err != nil {
		return wrapSupervisedTerminationVerification(err)
	}
	controlErr := signalSupervisedProcessGroup(process, agentprocess.SignalKill)
	return errors.Join(wrapSupervisedProcessControl(controlErr),
		wrapSupervisedTerminationVerification(waitForSupervisedProcessGroupGone(ctx, process)))
}

func probeSupervisedProcessGroup(process ProcessRecord) (agentprocess.Disposition, error) {
	return agentprocess.ProbeProcessGroup(supervisedProcessIdentity(process))
}

func signalSupervisedProcessGroup(process ProcessRecord, signal agentprocess.ControlSignal) error {
	err := agentprocess.SignalProcessGroup(supervisedProcessIdentity(process), signal)
	if errors.Is(err, agentprocess.ErrProcessGone) {
		return nil
	}
	return err
}

func supervisedProcessIdentity(process ProcessRecord) agentprocess.ProcessIdentity {
	return agentprocess.ProcessIdentity{
		PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
	}
}

func waitForSupervisedProcessGroupGone(ctx context.Context, process ProcessRecord) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		disposition, err := probeSupervisedProcessGroup(process)
		if supervisedProcessGroupTerminationVerified(disposition, err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("verify supervised process termination: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("verify supervised process termination: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func supervisedProcessGroupTerminationVerified(disposition agentprocess.Disposition, err error) bool {
	return disposition == agentprocess.DispositionGone && err == nil
}

func wrapSupervisedProcessControl(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errSupervisedProcessControl, err)
}

func wrapSupervisedTerminationVerification(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errSupervisedTerminationUnverified, err)
}

func signalSupervisedProcess(process ProcessRecord, lease workstore.Lease,
	signal agentprocess.ControlSignal,
) error {
	identity := supervisedProcessIdentity(process)
	_, err := agentprocess.Signal(agentprocess.ControlRequest{
		Target: agentprocess.ControlTarget{
			SessionID: lease.SessionID, Generation: lease.Generation,
			OwnerTokenHash: workstore.HashToken(lease.OwnerToken), Leader: identity,
			Members: []agentprocess.ProcessIdentity{identity},
		},
		Scope: agentprocess.ScopeProcessTree, Signal: signal,
	})
	if errors.Is(err, agentprocess.ErrProcessGone) {
		return nil
	}
	return err
}
