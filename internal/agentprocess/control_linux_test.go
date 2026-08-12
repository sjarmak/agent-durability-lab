//go:build linux

package agentprocess

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

const controlHelperRole = "AGENT_DURABILITY_CONTROL_HELPER"

func TestSignalLeaderOnlyLeavesDescendantAlive(t *testing.T) {
	leader, child, command := startControlProcessTree(t)
	defer killControlProcessGroup(leader.ProcessGroupID)

	result, err := Signal(ControlRequest{Target: controlTarget(leader, child), Scope: ScopeLeader, Signal: SignalTerminate})
	if err != nil {
		t.Fatalf("signal leader: %v", err)
	}
	if result.Method != MethodPIDFDLeader {
		t.Fatalf("signal method = %q; want %q", result.Method, MethodPIDFDLeader)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}
	assertEventuallyDisposition(t, leader, DispositionGone)
	if got, err := Probe(child); err != nil || got != DispositionAlive {
		t.Fatalf("child disposition = %q, %v; want alive", got, err)
	}
}

func TestProcessGroupControlSurvivesLeaderExit(t *testing.T) {
	leader, child, command := startControlProcessTree(t)
	defer killControlProcessGroup(leader.ProcessGroupID)
	if _, err := Signal(ControlRequest{
		Target: controlTarget(leader, child), Scope: ScopeLeader, Signal: SignalTerminate,
	}); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if disposition, err := ProbeProcessGroup(leader); err != nil || disposition != DispositionAlive {
		t.Fatalf("group after leader exit = %q, %v", disposition, err)
	}
	if err := SignalProcessGroup(leader, SignalKill); err != nil {
		t.Fatalf("kill leaderless group: %v", err)
	}
	assertEventuallyDisposition(t, child, DispositionGone)
	if disposition, err := ProbeProcessGroup(leader); err != nil || disposition != DispositionGone {
		t.Fatalf("group after kill = %q, %v", disposition, err)
	}
}

func TestProcessGroupDispositionDoesNotHideLiveMembersAfterLeaderReuse(t *testing.T) {
	mismatch := fmt.Errorf("%w: reused leader", ErrProcessIdentityMismatch)
	tests := []struct {
		name              string
		leaderDisposition Disposition
		leaderErr         error
		liveMembers       bool
		want              Disposition
		wantMismatch      bool
	}{
		{name: "leader gone group empty", leaderDisposition: DispositionGone, want: DispositionGone},
		{name: "leader gone descendants live", leaderDisposition: DispositionGone, liveMembers: true, want: DispositionAlive},
		{name: "leader reused group empty", leaderDisposition: DispositionReused, leaderErr: mismatch, want: DispositionGone},
		{name: "leader reused group occupied", leaderDisposition: DispositionReused, leaderErr: mismatch, liveMembers: true, want: DispositionReused, wantMismatch: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveProcessGroupDisposition(test.leaderDisposition, test.leaderErr, test.liveMembers)
			if got != test.want || errors.Is(err, ErrProcessIdentityMismatch) != test.wantMismatch {
				t.Fatalf("group disposition = %q, %v; want %q mismatch=%t", got, err, test.want, test.wantMismatch)
			}
		})
	}
}

func TestSignalProcessTreeReachesLeaderAndDescendant(t *testing.T) {
	leader, child, command := startControlProcessTree(t)
	defer killControlProcessGroup(leader.ProcessGroupID)

	result, err := Signal(ControlRequest{Target: controlTarget(leader, child), Scope: ScopeProcessTree, Signal: SignalTerminate})
	if err != nil {
		t.Fatalf("signal process tree: %v", err)
	}
	if result.Method != MethodProcessGroupAndPIDFD {
		t.Fatalf("signal method = %q; want %q", result.Method, MethodProcessGroupAndPIDFD)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}
	assertEventuallyDisposition(t, leader, DispositionGone)
	assertEventuallyDisposition(t, child, DispositionGone)
}

func TestSignalProcessTreeFreezesAndResumesLeaderAndDescendant(t *testing.T) {
	leader, child, command := startControlProcessTree(t)
	defer func() {
		killControlProcessGroup(leader.ProcessGroupID)
		_ = command.Wait()
	}()
	target := controlTarget(leader, child)

	if _, err := Signal(ControlRequest{Target: target, Scope: ScopeProcessTree, Signal: SignalStop}); err != nil {
		t.Fatalf("freeze process tree: %v", err)
	}
	assertEventuallyProcessState(t, leader.PID, 'T')
	assertEventuallyProcessState(t, child.PID, 'T')
	if _, err := Signal(ControlRequest{Target: target, Scope: ScopeProcessTree, Signal: SignalContinue}); err != nil {
		t.Fatalf("resume process tree: %v", err)
	}
	assertEventuallyProcessState(t, leader.PID, 'S')
	assertEventuallyProcessState(t, child.PID, 'S')
}

func TestSignalRejectsReusedOrMismatchedIdentity(t *testing.T) {
	leader, child, command := startControlProcessTree(t)
	defer func() {
		killControlProcessGroup(leader.ProcessGroupID)
		_ = command.Wait()
	}()

	target := controlTarget(leader, child)
	target.Leader.StartIdentity = "different-boot:1"
	if _, err := Signal(ControlRequest{Target: target, Scope: ScopeProcessTree, Signal: SignalTerminate}); !errors.Is(err, ErrProcessIdentityMismatch) {
		t.Fatalf("signal mismatched identity = %v; want ErrProcessIdentityMismatch", err)
	}
	if got, err := Probe(leader); err != nil || got != DispositionAlive {
		t.Fatalf("leader disposition = %q, %v; want alive", got, err)
	}
	if got, err := Probe(child); err != nil || got != DispositionAlive {
		t.Fatalf("child disposition = %q, %v; want alive", got, err)
	}
}

func TestDelayedStaleStopCannotSignalReplacementProcessTree(t *testing.T) {
	oldLeader, oldChild, oldCommand := startControlProcessTree(t)
	defer killControlProcessGroup(oldLeader.ProcessGroupID)
	newLeader, newChild, newCommand := startControlProcessTree(t)
	defer func() {
		killControlProcessGroup(newLeader.ProcessGroupID)
		_ = newCommand.Wait()
	}()

	staleTarget := controlTarget(oldLeader, oldChild)
	staleTarget.Generation = 1
	replacementTarget := controlTarget(newLeader, newChild)
	replacementTarget.Generation = 2
	if _, err := Signal(ControlRequest{
		Target: staleTarget, Scope: ScopeProcessTree, Signal: SignalTerminate,
	}); err != nil {
		t.Fatalf("deliver delayed generation-1 stop: %v", err)
	}
	if err := oldCommand.Wait(); err != nil {
		t.Fatalf("wait for old leader: %v", err)
	}
	assertEventuallyDisposition(t, oldLeader, DispositionGone)
	assertEventuallyDisposition(t, oldChild, DispositionGone)
	for _, identity := range replacementTarget.Members {
		if got, err := Probe(identity); err != nil || got != DispositionAlive {
			t.Fatalf("replacement member %+v disposition = %q, %v; want alive", identity, got, err)
		}
	}
}

func TestSignalRejectsInvalidControlRequest(t *testing.T) {
	invalid := []ControlRequest{
		{},
		{Target: ControlTarget{SessionID: "session", Generation: 1, OwnerTokenHash: "hash"}, Scope: ScopeLeader, Signal: SignalTerminate},
		{Target: ControlTarget{SessionID: "session", Generation: 1, OwnerTokenHash: "hash", Leader: ProcessIdentity{PID: 1, StartIdentity: "start", ProcessGroupID: 1}}, Scope: "bad", Signal: SignalTerminate},
		{Target: ControlTarget{SessionID: "session", Generation: 1, OwnerTokenHash: "hash", Leader: ProcessIdentity{PID: 1, StartIdentity: "start", ProcessGroupID: 1}}, Scope: ScopeLeader, Signal: "bad"},
	}
	for _, request := range invalid {
		if _, err := Signal(request); !errors.Is(err, ErrInvalidControlRequest) {
			t.Errorf("Signal(%+v) = %v; want ErrInvalidControlRequest", request, err)
		}
	}
}

func TestProcessGoneErrorIncludesProcfsESRCH(t *testing.T) {
	for _, err := range []error{
		os.ErrNotExist,
		fmt.Errorf("read proc state: %w", syscall.ESRCH),
	} {
		if !processGoneError(err) {
			t.Fatalf("processGoneError(%v) = false", err)
		}
	}
	if processGoneError(syscall.EPERM) {
		t.Fatal("permission failure must not be classified as a gone process")
	}
}

func TestControlHelperProcess(t *testing.T) {
	role := os.Getenv(controlHelperRole)
	if role == "" {
		return
	}
	switch role {
	case "leader":
		command := exec.Command(os.Args[0], "-test.run=TestControlHelperProcess")
		command.Env = append(os.Environ(), controlHelperRole+"=child")
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			os.Exit(91)
		}
		if err := emitHelperIdentity("leader"); err != nil {
			os.Exit(92)
		}
		waitForControlSignal()
	case "child":
		if err := emitHelperIdentity("child"); err != nil {
			os.Exit(93)
		}
		waitForControlSignal()
	default:
		os.Exit(94)
	}
}

type helperIdentity struct {
	Role     string          `json:"role"`
	Identity ProcessIdentity `json:"identity"`
}

func startControlProcessTree(t *testing.T) (ProcessIdentity, ProcessIdentity, *exec.Cmd) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=TestControlHelperProcess")
	command.Env = append(os.Environ(), controlHelperRole+"=leader")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("create helper pipe: %v", err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start helper leader: %v", err)
	}

	identities := make(map[string]ProcessIdentity, 2)
	scanner := bufio.NewScanner(stdout)
	for len(identities) < 2 && scanner.Scan() {
		var message helperIdentity
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			t.Fatalf("decode helper identity: %v", err)
		}
		identities[message.Role] = message.Identity
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read helper identities: %v", err)
	}
	if len(identities) != 2 {
		t.Fatalf("helper identities = %+v; want leader and child", identities)
	}
	if identities["leader"].PID != command.Process.Pid {
		t.Fatalf("reported leader PID = %d; want %d", identities["leader"].PID, command.Process.Pid)
	}
	if identities["leader"].ProcessGroupID != identities["child"].ProcessGroupID {
		t.Fatalf("process groups differ: leader=%+v child=%+v", identities["leader"], identities["child"])
	}
	return identities["leader"], identities["child"], command
}

func controlTarget(leader, child ProcessIdentity) ControlTarget {
	return ControlTarget{
		SessionID: "session-1", Generation: 1, OwnerTokenHash: "owner-hash",
		Leader: leader, Members: []ProcessIdentity{leader, child},
	}
}

func emitHelperIdentity(role string) error {
	identity, err := CaptureIdentity(os.Getpid())
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(helperIdentity{Role: role, Identity: identity})
}

func waitForControlSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	os.Exit(0)
}

func assertEventuallyDisposition(t *testing.T, identity ProcessIdentity, want Disposition) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, err := Probe(identity)
		if err == nil && got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("process %+v disposition = %q, %v; want %q", identity, got, err, want)
		case <-ticker.C:
		}
	}
}

func assertEventuallyProcessState(t *testing.T, pid int, want byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, err := processState(pid)
		if err == nil && got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("process %d state = %q, %v; want %q", pid, got, err, want)
		case <-ticker.C:
		}
	}
}

func killControlProcessGroup(processGroupID int) {
	if processGroupID > 1 {
		_ = syscall.Kill(-processGroupID, syscall.SIGKILL)
	}
}

func (h helperIdentity) String() string {
	return fmt.Sprintf("%s/%d", h.Role, h.Identity.PID)
}
