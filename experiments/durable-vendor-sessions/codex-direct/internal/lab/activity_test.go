package lab

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

func TestRunCodexActivityDerivesStableAndPhysicalIdentities(t *testing.T) {
	root := t.TempDir()
	var capturedInvocation Invocation
	var capturedInput RunInvocationInput
	activities := testActivities(root)
	activities.Invoke = func(_ context.Context, invocation Invocation, input RunInvocationInput,
		hooks StreamHooks,
	) (InvocationResult, error) {
		capturedInvocation, capturedInput = invocation, input
		if err := hooks.ThreadStarted(testThreadID); err != nil {
			return InvocationResult{}, err
		}
		return testInvocationResult(input, testThreadID), nil
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.RunCodex)
	encoded, err := environment.ExecuteActivity(activities.RunCodex, CodexActivityInput{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
	})
	if err != nil {
		t.Fatalf("execute Activity: %v", err)
	}
	var result CodexActivityResult
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode Activity result: %v", err)
	}
	if result.TemporalAttempt != 1 || result.ThreadID != testThreadID || result.Result != "EFFECT_COMPLETE" {
		t.Fatalf("result = %+v", result)
	}
	if !strings.HasSuffix(capturedInput.AttemptID, "-attempt-1") || capturedInput.ActorID != "worker-one-attempt-1" ||
		capturedInput.ThreadReceiptPath != filepath.Join(capturedInput.Directory, threadReceiptFile) {
		t.Fatalf("physical identity = %+v", capturedInput)
	}
	joined := strings.Join(capturedInvocation.Args, " ")
	if strings.Contains(joined, " resume ") || strings.Contains(joined, "--last") {
		t.Fatalf("unsafe invocation contains recovery controls: %s", joined)
	}
}

func TestResumeOnlyLearnsThreadThenUsesExactExplicitResume(t *testing.T) {
	root := t.TempDir()
	activities := testActivities(root)
	var invocations []Invocation
	activities.Invoke = func(_ context.Context, invocation Invocation, input RunInvocationInput,
		hooks StreamHooks,
	) (InvocationResult, error) {
		invocations = append(invocations, invocation)
		if err := hooks.ThreadStarted(testThreadID); err != nil {
			return InvocationResult{}, err
		}
		return testInvocationResult(input, testThreadID), nil
	}
	input := CodexActivityInput{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		RecoveryMode: RecoveryModeResumeOnly,
	}
	for attempt := int32(1); attempt <= 2; attempt++ {
		physical := "physical-attempt-" + string(rune('0'+attempt))
		if _, err := activities.executeAttempt(context.Background(), input, physical,
			"actor-"+string(rune('0'+attempt)), attempt); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if len(invocations) != 2 {
		t.Fatalf("invocations = %d", len(invocations))
	}
	first := strings.Join(invocations[0].Args, " ")
	second := strings.Join(invocations[1].Args, " ")
	if strings.Contains(first, " resume ") || !strings.Contains(second, "exec --sandbox workspace-write resume") ||
		!strings.Contains(second, testThreadID) || strings.Contains(second, "--last") {
		t.Fatalf("initial=%q resume=%q", first, second)
	}
	canonical, err := ReadCanonicalThread(canonicalThreadPath(activities.RunRoot, input.LogicalSessionID))
	if err != nil || canonical.ThreadID != testThreadID || canonical.FirstPhysicalAttemptID != "physical-attempt-1" {
		t.Fatalf("canonical = %+v err=%v", canonical, err)
	}
}

func TestPreThreadFaultWrapsCodexWithoutChangingArguments(t *testing.T) {
	activities := Activities{
		Command: CodexCommand{Binary: "/opt/codex"}, LauncherBinary: "/opt/codex-launcher",
		BarrierURL: "http://127.0.0.1:8080",
	}
	original := Invocation{Binary: "/opt/codex", Args: []string{"exec", "--json"}, Env: []string{"CODEX_HOME=/profile"}}
	wrapped := activities.preThreadInvocation(original, "session-1", "attempt-1", "worker-1", 2)
	if wrapped.Binary != activities.LauncherBinary || strings.Join(wrapped.Args, " ") != strings.Join(original.Args, " ") {
		t.Fatalf("wrapped = %+v", wrapped)
	}
	environment := strings.Join(wrapped.Env, "\n")
	for _, want := range []string{
		"CODEX_DIRECT_REAL_BINARY=/opt/codex", "CODEX_DIRECT_PRE_THREAD_BARRIER_URL=http://127.0.0.1:8080",
		"CODEX_DIRECT_PHYSICAL_ATTEMPT_ID=attempt-1", "CODEX_DIRECT_LOGICAL_SESSION_ID=session-1",
		"CODEX_DIRECT_ACTOR_ID=worker-1",
		"CODEX_DIRECT_GENERATION=2",
	} {
		if !strings.Contains(environment, want) {
			t.Fatalf("environment %q lacks %q", environment, want)
		}
	}
}

func TestActivityValidationRequiresSupervisorAndPreThreadLauncher(t *testing.T) {
	input := CodexActivityInput{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		RecoveryMode: RecoveryModeUnsafeFresh,
	}
	activities := testActivities(t.TempDir())
	if err := activities.validate(input); err != nil {
		t.Fatalf("valid Activity: %v", err)
	}
	invalid := activities
	invalid.Command.Binary = ""
	if err := invalid.validate(input); err == nil {
		t.Fatal("incomplete Activity configuration was accepted")
	}
	fenced := activities
	input.RecoveryMode = RecoveryModeFenced
	if err := fenced.validate(input); err == nil {
		t.Fatal("fenced Activity without supervisor was accepted")
	}
	preThread := activities
	preThread.FaultBoundary = FaultBeforeThreadObservation
	preThread.LauncherBinary = ""
	input.RecoveryMode = RecoveryModeUnsafeFresh
	if err := preThread.validate(input); err == nil {
		t.Fatal("pre-thread fault without launcher was accepted")
	}
}

func testActivities(root string) Activities {
	return Activities{
		Command: CodexCommand{
			Binary: "/opt/codex", WorkDir: root, CodexHome: filepath.Join(root, "codex-home"),
			Model: "gpt-5.6-sol", ReasoningEffort: "low",
			OutputSchema: filepath.Join(root, "result.schema.json"), Sandbox: "workspace-write",
		},
		FaultBoundary: FaultNone, EffectBinary: "/opt/controlled-effect",
		DestinationPath: filepath.Join(root, "destination.db"), WorkspacePath: filepath.Join(root, "effects.jsonl"),
		EffectPayload: "controlled-edit", BarrierURL: "http://127.0.0.1:8080",
		BarrierPoint: committedEffectBarrier, RunRoot: filepath.Join(root, "attempts"), WorkerID: "worker-one",
	}
}

func testInvocationResult(input RunInvocationInput, threadID string) InvocationResult {
	return InvocationResult{
		Codex: CodexStreamResult{ThreadID: threadID, Result: "EFFECT_COMPLETE"},
		Process: ProcessRecord{
			AttemptID: input.AttemptID, ActorID: input.ActorID, PID: 101,
			StartIdentity: "start-one", Identity: "pid:101:start:start-one",
		},
	}
}
