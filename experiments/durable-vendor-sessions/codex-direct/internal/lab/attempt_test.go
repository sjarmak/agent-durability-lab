package lab

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareAttemptPublishesExactCodexCommandAndImmutableRequest(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "attempt-1")
	prepared, err := PrepareAttempt(context.Background(), AttemptInput{
		Directory: directory, EffectBinary: "/opt/lab/controlled-effect",
		DestinationPath: "/tmp/lab/destination.db", WorkspacePath: "/tmp/lab/effects.jsonl",
		ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json"),
		Payload:           "controlled-edit", BarrierURL: "http://127.0.0.1:8080", BarrierPoint: committedEffectBarrier,
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "codex-attempt-1",
	})
	if err != nil {
		t.Fatalf("prepare attempt: %v", err)
	}
	wantCommand := "/opt/lab/controlled-effect --request " + filepath.Join(directory, effectRequestFile)
	if prepared.Command != wantCommand || !strings.Contains(prepared.Prompt, wantCommand) {
		t.Fatalf("prepared = %+v", prepared)
	}
	if !strings.Contains(prepared.Prompt, "Do not emit the final JSON before the command exits with status 0") ||
		!strings.Contains(prepared.Prompt, "If the command cannot run or fails, do not claim EFFECT_COMPLETE") ||
		!strings.Contains(prepared.Prompt, "You must use the shell execution tool now") ||
		!strings.Contains(prepared.Prompt, "not permission to skip tool use") {
		t.Fatalf("prompt does not make effect-before-result ordering explicit: %q", prepared.Prompt)
	}
	request, err := ReadControlledEffectRequest(prepared.RequestPath)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	if request.PhysicalAttemptID != "attempt-1" || request.DestinationPath != "/tmp/lab/destination.db" {
		t.Fatalf("request = %+v", request)
	}
	if _, err := PrepareAttempt(context.Background(), AttemptInput{
		Directory: directory, EffectBinary: "/opt/lab/effect binary",
	}); err == nil {
		t.Fatal("unsafe incomplete attempt unexpectedly succeeded")
	}
}

func TestAttemptRequestRejectsSymlinkedFilesAndDirectories(t *testing.T) {
	root := t.TempDir()
	targetDirectory := filepath.Join(root, "target")
	if err := os.Mkdir(targetDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	aliasDirectory := filepath.Join(root, "attempt-alias")
	if err := os.Symlink(targetDirectory, aliasDirectory); err != nil {
		t.Fatal(err)
	}
	input := AttemptInput{
		Directory: aliasDirectory, EffectBinary: "/opt/lab/effect",
		DestinationPath: filepath.Join(root, "destination.db"), WorkspacePath: filepath.Join(root, "fixture", "effects.jsonl"),
		ThreadReceiptPath: filepath.Join(aliasDirectory, threadReceiptFile), Payload: "edit",
		BarrierDirectory: filepath.Join(root, "barrier"), BarrierPoint: committedEffectBarrier,
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "actor-1",
	}
	if _, err := PrepareAttempt(context.Background(), input); err == nil {
		t.Fatal("symlinked attempt directory was accepted")
	}
	if _, err := os.Lstat(filepath.Join(targetDirectory, effectRequestFile)); !os.IsNotExist(err) {
		t.Fatalf("request escaped through symlinked directory: %v", err)
	}

	realRequest := filepath.Join(root, "real-request.json")
	input.Directory = filepath.Join(root, "real-attempt")
	input.ThreadReceiptPath = filepath.Join(input.Directory, threadReceiptFile)
	prepared, err := PrepareAttempt(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(prepared.RequestPath, realRequest); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realRequest, prepared.RequestPath); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadControlledEffectRequest(prepared.RequestPath); err == nil {
		t.Fatal("symlinked controlled-effect request was accepted")
	}

	ancestorAlias := filepath.Join(root, "request-parent-alias")
	if err := os.Symlink(input.Directory, ancestorAlias); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadControlledEffectRequest(filepath.Join(ancestorAlias, effectRequestFile)); err == nil {
		t.Fatal("controlled-effect request beneath a symlinked ancestor was accepted")
	}
}
