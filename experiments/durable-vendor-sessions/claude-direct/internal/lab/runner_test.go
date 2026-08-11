package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDirectRunnerPreservesRawStreamsAndProcessIdentity(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	promptPath := filepath.Join(directory, "stdin.txt")
	binary := writeExecutable(t, directory, "fake-claude", strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"cat > " + promptPath,
		"printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"vendor-session-1\"}'",
		"printf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"session_id\":\"vendor-session-1\",\"is_error\":false,\"structured_output\":{\"status\":\"EFFECT_COMPLETE\"}}'",
		"printf '%s\\n' 'diagnostic' >&2",
	}, "\n"))
	result, err := RunInvocation(context.Background(), Invocation{
		Binary: binary, Args: []string{"-p"}, WorkDir: directory, Stdin: "controlled prompt\n",
	}, RunInvocationInput{
		Directory: directory, AttemptID: "activity-attempt-1", ActorID: "worker-1",
	})
	if err != nil {
		t.Fatalf("run invocation: %v", err)
	}
	if result.Claude.SessionID != "vendor-session-1" || result.Claude.Result != "EFFECT_COMPLETE" {
		t.Fatalf("Claude result = %+v", result.Claude)
	}
	if result.Process.PID <= 0 || result.Process.StartIdentity == "" || result.Process.Identity == "" {
		t.Fatalf("process = %+v", result.Process)
	}
	if result.Process.Binary != binary || !slices.Equal(result.Process.Args, []string{"-p"}) ||
		result.Process.WorkDir != directory {
		t.Fatalf("recorded invocation = %+v", result.Process)
	}
	for _, path := range []string{result.StdoutPath, result.StderrPath, result.ProcessStartedPath, result.ProcessCompletedPath} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("stat %s: %v", filepath.Base(path), err)
		}
	}
	prompt, err := os.ReadFile(promptPath)
	if err != nil {
		t.Fatalf("read captured prompt: %v", err)
	}
	if string(prompt) != "controlled prompt\n" {
		t.Fatalf("captured prompt = %q", prompt)
	}
	stderr, err := os.ReadFile(result.StderrPath)
	if err != nil || string(stderr) != "diagnostic\n" {
		t.Fatalf("stderr = %q, err = %v", stderr, err)
	}
}

func TestDirectRunnerPreservesFailureAndHonorsPreStartCancellation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	binary := writeExecutable(t, directory, "failing-claude", "#!/bin/sh\necho failed >&2\nexit 7\n")
	_, err := RunInvocation(context.Background(), Invocation{
		Binary: binary, WorkDir: directory, Stdin: "prompt\n",
	}, RunInvocationInput{Directory: directory, AttemptID: "attempt-1", ActorID: "worker-1"})
	if err == nil || !strings.Contains(err.Error(), "exit status 7") {
		t.Fatalf("failure = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "attempt-1.stderr")); statErr != nil {
		t.Fatalf("failure stderr was not preserved: %v", statErr)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelDirectory := filepath.Join(directory, "canceled")
	_, err = RunInvocation(canceled, Invocation{
		Binary: binary, WorkDir: directory, Stdin: "prompt\n",
	}, RunInvocationInput{Directory: cancelDirectory, AttemptID: "attempt-2", ActorID: "worker-1"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled run error = %v", err)
	}
	if _, statErr := os.Stat(cancelDirectory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("canceled run created artifacts: %v", statErr)
	}
}

func writeExecutable(t *testing.T, directory, name, body string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	file, err := os.CreateTemp(directory, "."+name+"-*")
	if err != nil {
		t.Fatalf("create executable staging file: %v", err)
	}
	stagingPath := file.Name()
	closeWithFailure := func(action string, actionErr error) {
		_ = file.Close()
		_ = os.Remove(stagingPath)
		t.Fatalf("%s executable: %v", action, actionErr)
	}
	if _, err := file.Write([]byte(body)); err != nil {
		closeWithFailure("write", err)
	}
	if err := file.Chmod(0o700); err != nil {
		closeWithFailure("chmod", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(stagingPath)
		t.Fatalf("close executable: %v", err)
	}
	if err := os.Rename(stagingPath, path); err != nil {
		_ = os.Remove(stagingPath)
		t.Fatalf("publish executable: %v", err)
	}
	return path
}
