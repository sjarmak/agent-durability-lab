package agentprocess

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentsim"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

const (
	testProcessWaitTimeout  = 10 * time.Second
	testProcessPollInterval = 10 * time.Millisecond
)

func TestDetachedAgentSurvivesLauncherSIGKILL(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process survival evidence currently targets Linux")
	}
	root := repositoryRoot(t)
	temporary := t.TempDir()
	agentBinary := filepath.Join(temporary, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build agent simulator: %v\n%s", err, output)
	}

	store, err := workstore.Open(filepath.Join(temporary, "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	coordinator := failureinject.NewCoordinator()
	barrierServer := httptest.NewServer(coordinator.Handler())
	t.Cleanup(barrierServer.Close)

	request := LaunchRequest{
		StorePath: store.Path(), BarrierURL: barrierServer.URL,
		Config: agentsim.Config{
			Lease: decision.Lease, ActorID: "agent-1",
			Effect: workstore.Effect{ID: "effect-1", Value: "changed"}, Outcome: workstore.Outcome{Value: "done"},
		},
	}
	requestPath := filepath.Join(temporary, "launch-request.json")
	writeJSONFile(t, requestPath, request)

	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("create helper pipe: %v", err)
	}
	helper := exec.Command(os.Args[0], "-test.run=TestLauncherHelperProcess")
	helper.Env = append(os.Environ(),
		"ADL_LAUNCHER_HELPER=1",
		"ADL_AGENT_BINARY="+agentBinary,
		"ADL_LAUNCH_REQUEST="+requestPath,
	)
	helper.ExtraFiles = []*os.File{writePipe}
	if err := helper.Start(); err != nil {
		t.Fatalf("start launcher helper: %v", err)
	}
	_ = writePipe.Close()
	t.Cleanup(func() {
		_ = helper.Process.Kill()
		_, _ = helper.Process.Wait()
	})

	var childPID int
	if _, err := fmt.Fscan(readPipe, &childPID); err != nil {
		t.Fatalf("read child PID: %v", err)
	}
	_ = readPipe.Close()
	t.Cleanup(func() {
		_ = syscall.Kill(childPID, syscall.SIGKILL)
	})
	if _, err := coordinator.WaitForArrivals(context.Background(), "before-effect/1", 1); err != nil {
		t.Fatalf("wait for child progress: %v", err)
	}
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("kill launcher helper: %v", err)
	}
	processState, err := helper.Process.Wait()
	if err != nil {
		t.Fatalf("wait for killed launcher helper: %v", err)
	}
	waitStatus, ok := processState.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("launcher helper status = %v; want SIGKILL", processState)
	}
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatalf("child PID %d did not survive launcher: %v", childPID, err)
	}

	if err := coordinator.Release("before-effect/1"); err != nil {
		t.Fatalf("release effect barrier: %v", err)
	}
	if _, err := coordinator.WaitForArrivals(context.Background(), "before-completion/1", 1); err != nil {
		t.Fatalf("wait for completion barrier: %v", err)
	}
	if err := coordinator.Release("before-completion/1"); err != nil {
		t.Fatalf("release completion barrier: %v", err)
	}

	snapshot := waitForOutcome(t, store, "session-1")
	if snapshot.Outcome == nil || snapshot.Outcome.Value != "done" {
		t.Fatalf("outcome = %+v; want done", snapshot.Outcome)
	}
	if len(snapshot.Executors) != 1 || snapshot.Executors[0].PID != childPID {
		t.Fatalf("executors = %+v; want surviving child PID %d", snapshot.Executors, childPID)
	}
}

func TestLauncherPublishesPrivateRequestAndDetachedProcess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process identity evidence currently targets Linux")
	}
	temporary := t.TempDir()
	binary := filepath.Join(temporary, "blocking-agent")
	t.Setenv("ADL_WORKER_SECRET", "must-not-inherit")
	script := "#!/bin/sh\nprintf '%s' \"${ADL_WORKER_SECRET-unset}\" > \"$2.environment\"\nexec /usr/bin/tail -f /dev/null\n"
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatalf("write blocking agent: %v", err)
	}
	launcher := NewLauncher(binary, filepath.Join(temporary, "runs"))
	process, err := launcher.Launch(LaunchRequest{
		StorePath: filepath.Join(temporary, "work.db"), BarrierURL: "http://127.0.0.1",
		Config: agentsim.Config{
			Lease:   workstore.Lease{SessionID: "session-1", Generation: 1, OwnerToken: "owner-1"},
			ActorID: "agent-1", Effect: workstore.Effect{ID: "effect-1"}, Outcome: workstore.Outcome{Value: "done"},
		},
	})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Kill(process.PID, syscall.SIGKILL) })
	if process.PID <= 0 || process.StartIdentity == "" || process.ProcessGroupID != process.PID {
		t.Fatalf("process = %+v; want PID and start identity", process)
	}
	if err := syscall.Kill(process.PID, 0); err != nil {
		t.Fatalf("detached process is not alive: %v", err)
	}
	if inherited := waitForFileContent(t, process.RequestPath+".environment"); inherited != "unset" {
		t.Fatalf("detached process inherited Worker secret %q", inherited)
	}
	for _, path := range []string{process.RequestPath, process.LogPath} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if permissions := info.Mode().Perm(); permissions != 0o600 {
			t.Fatalf("%s permissions = %#o; want 0600", path, permissions)
		}
	}
	requestFile, err := os.Open(process.RequestPath)
	if err != nil {
		t.Fatalf("open request: %v", err)
	}
	defer func() {
		if err := requestFile.Close(); err != nil {
			t.Errorf("close request: %v", err)
		}
	}()
	var decoded LaunchRequest
	if err := json.NewDecoder(requestFile).Decode(&decoded); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if decoded.Config.Lease.OwnerToken != "owner-1" {
		t.Fatalf("decoded lease = %+v", decoded.Config.Lease)
	}
}

func TestLauncherValidation(t *testing.T) {
	temporary := t.TempDir()
	nonExecutable := filepath.Join(temporary, "not-executable")
	if err := os.WriteFile(nonExecutable, []byte("data"), 0o600); err != nil {
		t.Fatalf("write non-executable: %v", err)
	}
	valid := LaunchRequest{
		StorePath: "work.db", BarrierURL: "http://127.0.0.1",
		Config: agentsim.Config{Lease: workstore.Lease{SessionID: "session", Generation: 1, OwnerToken: "owner"}},
	}
	tests := []struct {
		name     string
		launcher *Launcher
		request  LaunchRequest
	}{
		{name: "missing launcher", launcher: NewLauncher("", ""), request: valid},
		{name: "missing binary", launcher: NewLauncher(filepath.Join(temporary, "missing"), temporary), request: valid},
		{name: "not executable", launcher: NewLauncher(nonExecutable, temporary), request: valid},
		{name: "incomplete lease", launcher: NewLauncher(os.Args[0], temporary), request: LaunchRequest{StorePath: "work.db", BarrierURL: "http://127.0.0.1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.launcher.Launch(test.request); err == nil {
				t.Fatal("Launch returned nil error")
			}
		})
	}
}

func TestProcessStartIdentity(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process identity evidence currently targets Linux")
	}
	identity, err := CurrentProcessStartIdentity()
	if err != nil {
		t.Fatalf("current process identity: %v", err)
	}
	byPID, err := ProcessStartIdentity(os.Getpid())
	if err != nil {
		t.Fatalf("process identity by PID: %v", err)
	}
	if identity == "" || identity != byPID {
		t.Fatalf("identities = %q and %q", identity, byPID)
	}
	if _, err := ProcessStartIdentity(-1); err == nil {
		t.Fatal("invalid PID returned nil error")
	}
}

func TestLauncherHelperProcess(t *testing.T) {
	if os.Getenv("ADL_LAUNCHER_HELPER") != "1" {
		return
	}
	requestFile, err := os.Open(os.Getenv("ADL_LAUNCH_REQUEST"))
	if err != nil {
		t.Fatal(err)
	}
	var request LaunchRequest
	if err := json.NewDecoder(requestFile).Decode(&request); err != nil {
		t.Fatal(err)
	}
	if err := requestFile.Close(); err != nil {
		t.Fatal(err)
	}
	launcher := NewLauncher(os.Getenv("ADL_AGENT_BINARY"), filepath.Dir(os.Getenv("ADL_LAUNCH_REQUEST")))
	process, err := launcher.Launch(request)
	if err != nil {
		t.Fatal(err)
	}
	pipe := os.NewFile(3, "launcher-result")
	if _, err := fmt.Fprintln(pipe, process.PID); err != nil {
		t.Fatal(err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatal(err)
	}
	select {}
}

func waitForOutcome(t *testing.T, store *workstore.Store, sessionID string) workstore.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testProcessWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(testProcessPollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := store.Snapshot(ctx, sessionID)
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if snapshot.Outcome != nil {
			return snapshot
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for outcome: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func waitForFileContent(t *testing.T, path string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testProcessWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(testProcessPollInterval)
	defer ticker.Stop()
	for {
		content, err := os.ReadFile(path)
		if err == nil {
			return string(content)
		}
		if !os.IsNotExist(err) {
			t.Fatalf("read %s: %v", path, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %s: %v", path, ctx.Err())
		case <-ticker.C:
		}
	}
}
