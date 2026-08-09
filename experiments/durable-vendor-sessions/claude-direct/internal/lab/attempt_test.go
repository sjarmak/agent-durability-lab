package lab

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareAttemptPublishesExactControlledCommand(t *testing.T) {
	t.Parallel()

	directory := filepath.Join(t.TempDir(), "attempt-1")
	prepared, err := PrepareAttempt(context.Background(), AttemptInput{
		Directory:         directory,
		EffectBinary:      "/opt/lab/controlled-effect",
		DestinationPath:   "/tmp/lab/destination.db",
		WorkspacePath:     "/tmp/lab/workspace/effects.jsonl",
		Payload:           "controlled-edit",
		BarrierURL:        "http://127.0.0.1:8080",
		BarrierPoint:      "claude-tool-effect-committed",
		LogicalSessionID:  "logical-session-1",
		LogicalTurnID:     "turn-1",
		LogicalEffectID:   "effect-1",
		PhysicalAttemptID: "activity-attempt-1",
		ActorID:           "claude-attempt-1",
	})
	if err != nil {
		t.Fatalf("prepare attempt: %v", err)
	}
	wantCommand := "/opt/lab/controlled-effect --request " + filepath.Join(directory, "effect-request.json")
	if prepared.Command != wantCommand || prepared.AllowedTool != "Bash("+wantCommand+")" {
		t.Fatalf("prepared command = %+v", prepared)
	}
	if !strings.Contains(prepared.Prompt, wantCommand) || strings.Contains(prepared.Prompt, "--session-id") {
		t.Fatalf("prompt = %q", prepared.Prompt)
	}
	info, err := os.Stat(prepared.RequestPath)
	if err != nil {
		t.Fatalf("stat request: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("request mode = %o, want 600", info.Mode().Perm())
	}
	request, err := ReadControlledEffectRequest(prepared.RequestPath)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	if request.PhysicalAttemptID != "activity-attempt-1" || request.DestinationPath != "/tmp/lab/destination.db" ||
		request.WorkspacePath != "/tmp/lab/workspace/effects.jsonl" || request.Payload != "controlled-edit" {
		t.Fatalf("request = %+v", request)
	}
	if _, err := PrepareAttempt(context.Background(), AttemptInput{
		Directory: directory, EffectBinary: "/opt/lab/controlled-effect",
	}); err == nil {
		t.Fatal("incomplete attempt returned nil error")
	}
}

func TestPrepareAttemptRejectsShellAmbiguousPaths(t *testing.T) {
	t.Parallel()

	base := AttemptInput{
		Directory: t.TempDir(), EffectBinary: "/opt/lab/effect binary",
		DestinationPath: "/tmp/lab/destination.db", WorkspacePath: "/tmp/lab/workspace/effects.jsonl",
		Payload: "controlled-edit", BarrierURL: "http://127.0.0.1:8080",
		BarrierPoint: "point", LogicalSessionID: "session", LogicalTurnID: "turn",
		LogicalEffectID: "effect", PhysicalAttemptID: "attempt", ActorID: "actor",
	}
	if _, err := PrepareAttempt(context.Background(), base); err == nil {
		t.Fatal("path containing whitespace returned nil error")
	}
}
