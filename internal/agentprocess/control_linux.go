//go:build linux

package agentprocess

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func CaptureIdentity(pid int) (ProcessIdentity, error) {
	if pid <= 0 {
		return ProcessIdentity{}, fmt.Errorf("%w: positive PID is required", ErrInvalidControlRequest)
	}
	startIdentity, err := ProcessStartIdentity(pid)
	if err != nil {
		return ProcessIdentity{}, err
	}
	processGroupID, err := ProcessGroupID(pid)
	if err != nil {
		return ProcessIdentity{}, err
	}
	return ProcessIdentity{PID: pid, StartIdentity: startIdentity, ProcessGroupID: processGroupID}, nil
}

func Probe(identity ProcessIdentity) (Disposition, error) {
	if err := validateProcessIdentity(identity); err != nil {
		return "", err
	}
	startIdentity, err := ProcessStartIdentity(identity.PID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return DispositionGone, nil
		}
		return "", err
	}
	if startIdentity != identity.StartIdentity {
		return DispositionReused, fmt.Errorf(
			"%w: PID %d start is %q, target is %q",
			ErrProcessIdentityMismatch, identity.PID, startIdentity, identity.StartIdentity,
		)
	}
	processGroupID, err := syscall.Getpgid(identity.PID)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return DispositionGone, nil
		}
		return "", fmt.Errorf("read process %d group: %w", identity.PID, err)
	}
	if processGroupID != identity.ProcessGroupID {
		return DispositionReused, fmt.Errorf(
			"%w: PID %d group is %d, target is %d",
			ErrProcessIdentityMismatch, identity.PID, processGroupID, identity.ProcessGroupID,
		)
	}
	state, err := processState(identity.PID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return DispositionGone, nil
		}
		return "", err
	}
	if state == 'Z' || state == 'X' {
		return DispositionGone, nil
	}
	return DispositionAlive, nil
}

func Signal(request ControlRequest) (ControlResult, error) {
	if err := validateControlRequest(request); err != nil {
		return ControlResult{}, err
	}
	disposition, err := Probe(request.Target.Leader)
	if err != nil {
		return ControlResult{}, err
	}
	if disposition != DispositionAlive {
		return ControlResult{}, fmt.Errorf("%w: leader PID %d", ErrProcessGone, request.Target.Leader.PID)
	}

	pidfd, err := unix.PidfdOpen(request.Target.Leader.PID, 0)
	if err != nil {
		return ControlResult{}, fmt.Errorf("open pidfd for leader %d: %w", request.Target.Leader.PID, err)
	}
	defer func() { _ = unix.Close(pidfd) }()
	if _, err := Probe(request.Target.Leader); err != nil {
		return ControlResult{}, err
	}

	signal := linuxSignal(request.Signal)
	method := MethodPIDFDLeader
	if request.Scope == ScopeProcessTree {
		if err := validateMembers(request.Target); err != nil {
			return ControlResult{}, err
		}
	}
	if err := unix.PidfdSendSignal(pidfd, signal, nil, 0); err != nil {
		return ControlResult{}, fmt.Errorf("signal exact leader %d: %w", request.Target.Leader.PID, err)
	}
	if request.Scope == ScopeProcessTree {
		if err := syscall.Kill(-request.Target.Leader.ProcessGroupID, syscall.Signal(signal)); err != nil && !errors.Is(err, syscall.ESRCH) {
			return ControlResult{}, fmt.Errorf("signal process group %d: %w", request.Target.Leader.ProcessGroupID, err)
		}
		method = MethodProcessGroupAndPIDFD
	}
	return ControlResult{
		Target: request.Target, Scope: request.Scope, Signal: request.Signal,
		Method: method, RequestedAt: time.Now().UTC(),
	}, nil
}

func validateControlRequest(request ControlRequest) error {
	if request.Target.SessionID == "" || request.Target.Generation == 0 || request.Target.OwnerTokenHash == "" {
		return fmt.Errorf("%w: logical session, generation, and owner hash are required", ErrInvalidControlRequest)
	}
	if err := validateProcessIdentity(request.Target.Leader); err != nil {
		return err
	}
	if request.Scope != ScopeLeader && request.Scope != ScopeProcessTree {
		return fmt.Errorf("%w: unsupported scope %q", ErrInvalidControlRequest, request.Scope)
	}
	if request.Signal != SignalTerminate && request.Signal != SignalKill &&
		request.Signal != SignalStop && request.Signal != SignalContinue {
		return fmt.Errorf("%w: unsupported signal %q", ErrInvalidControlRequest, request.Signal)
	}
	return nil
}

func validateProcessIdentity(identity ProcessIdentity) error {
	if identity.PID <= 0 || identity.StartIdentity == "" || identity.ProcessGroupID <= 0 {
		return fmt.Errorf("%w: PID, start identity, and process group are required", ErrInvalidControlRequest)
	}
	return nil
}

func validateMembers(target ControlTarget) error {
	if target.Leader.PID != target.Leader.ProcessGroupID {
		return fmt.Errorf("%w: process-tree leader must own its process group", ErrInvalidControlRequest)
	}
	leaderFound := false
	for _, member := range target.Members {
		if err := validateProcessIdentity(member); err != nil {
			return err
		}
		if member.ProcessGroupID != target.Leader.ProcessGroupID {
			return fmt.Errorf("%w: member PID %d belongs to group %d, target group is %d",
				ErrInvalidControlRequest, member.PID, member.ProcessGroupID, target.Leader.ProcessGroupID)
		}
		if _, err := Probe(member); err != nil {
			return err
		}
		if member == target.Leader {
			leaderFound = true
		}
	}
	if !leaderFound {
		return fmt.Errorf("%w: process-tree members omit the leader", ErrInvalidControlRequest)
	}
	return nil
}

func linuxSignal(signal ControlSignal) unix.Signal {
	switch signal {
	case SignalTerminate:
		return unix.SIGTERM
	case SignalKill:
		return unix.SIGKILL
	case SignalStop:
		return unix.SIGSTOP
	case SignalContinue:
		return unix.SIGCONT
	default:
		panic("validated process signal has no Linux mapping")
	}
}

func processState(pid int) (byte, error) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, fmt.Errorf("read process %d state: %w", pid, err)
	}
	closing := strings.LastIndexByte(string(stat), ')')
	if closing < 0 || closing+2 >= len(stat) {
		return 0, fmt.Errorf("parse process %d state", pid)
	}
	return stat[closing+2], nil
}
