package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

const supervisedProcessTerminationGrace = 5 * time.Second

const supervisedRegistrationGateFDEnv = "CLAUDE_DIRECT_REGISTRATION_GATE_FD"

type supervisedInvocationHooks struct {
	AfterExecBeforeRegistration func(context.Context, ProcessRecord) error
	AfterFinalOutput            func(context.Context, InvocationResult) error
	RegistrationGate            bool
}

func RunSupervisedInvocation(ctx context.Context, store *workstore.Store, lease workstore.Lease,
	invocation Invocation, input RunInvocationInput, hooks supervisedInvocationHooks,
) (InvocationResult, error) {
	if ctx == nil || store == nil || lease.SessionID == "" || lease.Generation == 0 || lease.OwnerToken == "" {
		return InvocationResult{}, fmt.Errorf("%w: context, store, and complete lease are required", workstore.ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return InvocationResult{}, err
	}
	if err := validateRunInvocation(invocation, input); err != nil {
		return InvocationResult{}, err
	}
	if err := os.MkdirAll(input.Directory, 0o750); err != nil {
		return InvocationResult{}, fmt.Errorf("create supervised invocation directory: %w", err)
	}
	var gateReader, gateWriter *os.File
	if hooks.RegistrationGate {
		var err error
		gateReader, gateWriter, err = os.Pipe()
		if err != nil {
			return InvocationResult{}, fmt.Errorf("create durable registration gate: %w", err)
		}
		invocation.Env = append(append([]string(nil), invocation.Env...), supervisedRegistrationGateFDEnv+"=3")
	}
	configure := func(command *exec.Cmd) error {
		if err := configureSupervisedProcess(command); err != nil {
			return err
		}
		if gateReader != nil {
			command.ExtraFiles = append(command.ExtraFiles, gateReader)
		}
		return nil
	}
	running, err := startInvocationConfigured(invocation, input, configure)
	if err != nil {
		if gateReader != nil {
			_ = gateReader.Close()
			_ = gateWriter.Close()
		}
		return InvocationResult{}, err
	}
	if gateReader != nil {
		_ = gateReader.Close()
	}
	process := running.process
	if hooks.AfterExecBeforeRegistration != nil {
		if err := hooks.AfterExecBeforeRegistration(ctx, process); err != nil {
			if gateWriter != nil {
				_ = gateWriter.Close()
			}
			return InvocationResult{}, errors.Join(err, stopSupervisedInvocation(running, lease))
		}
	}
	registrationContext := context.WithoutCancel(ctx)
	if err := store.RegisterProcess(registrationContext, lease, workstore.Process{
		PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
	}); err != nil {
		if gateWriter != nil {
			_ = gateWriter.Close()
		}
		return InvocationResult{}, errors.Join(err, stopSupervisedInvocation(running, lease))
	}
	if gateWriter != nil {
		if _, err := gateWriter.Write([]byte{1}); err != nil {
			_ = gateWriter.Close()
			return InvocationResult{}, errors.Join(
				fmt.Errorf("release durable registration gate: %w", err),
				stopSupervisedInvocation(running, lease),
			)
		}
		if err := gateWriter.Close(); err != nil {
			return InvocationResult{}, errors.Join(err, stopSupervisedInvocation(running, lease))
		}
	}
	result, err := awaitSupervisedInvocation(ctx, running, lease)
	if err != nil {
		return result, err
	}
	if hooks.AfterFinalOutput != nil {
		if err := hooks.AfterFinalOutput(ctx, result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func awaitSupervisedInvocation(ctx context.Context, running runningInvocation,
	lease workstore.Lease,
) (InvocationResult, error) {
	waited := make(chan error, 1)
	go func() {
		waited <- running.command.Wait()
	}()
	select {
	case waitErr := <-waited:
		return running.finish(waitErr)
	case <-ctx.Done():
	}

	controlErr := signalSupervisedInvocation(running.process, lease, agentprocess.SignalTerminate)
	timer := time.NewTimer(supervisedProcessTerminationGrace)
	defer timer.Stop()
	var waitErr error
	select {
	case waitErr = <-waited:
	case <-timer.C:
		killErr := signalSupervisedInvocation(running.process, lease, agentprocess.SignalKill)
		controlErr = errors.Join(controlErr, killErr)
		waitErr = <-waited
	}
	result, finishErr := running.finish(waitErr)
	return result, errors.Join(ctx.Err(), controlErr, finishErr)
}

func stopSupervisedInvocation(running runningInvocation, lease workstore.Lease) error {
	signalErr := signalSupervisedInvocation(running.process, lease, agentprocess.SignalKill)
	waitErr := running.command.Wait()
	_, finishErr := running.finish(waitErr)
	return errors.Join(signalErr, finishErr)
}

func signalSupervisedInvocation(process ProcessRecord, lease workstore.Lease,
	signal agentprocess.ControlSignal,
) error {
	identity := agentprocess.ProcessIdentity{
		PID: process.PID, StartIdentity: process.StartIdentity, ProcessGroupID: process.ProcessGroupID,
	}
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

func configureSupervisedProcess(command *exec.Cmd) error {
	return configureSupervisedProcessPlatform(command)
}
