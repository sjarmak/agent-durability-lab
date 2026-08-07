package lab

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestActivityRetriesAfterReleasedPostEffectBarrier(t *testing.T) {
	barriers, err := startBarrierService()
	if err != nil {
		t.Fatalf("start barrier: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := barriers.stop(ctx); err != nil {
			t.Errorf("stop barrier: %v", err)
		}
	}()
	root := t.TempDir()
	input := WorkflowInput{
		Destination: DestinationDatabase,
		Mode:        ModeProtected,
		EffectID:    "effect-activity-test",
		Payload:     "payload",
		Config: DestinationConfig{
			DatabasePath: filepath.Join(root, "effects.db"),
		},
		BarrierURL: barriers.URL(),
		StorePath:  filepath.Join(root, "observations.db"),
	}
	if err := prepareDestination(context.Background(), input.Destination, input.Config); err != nil {
		t.Fatalf("prepare destination: %v", err)
	}
	released := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := barriers.coordinator.WaitForArrivals(ctx, "after-effect/attempt-1", 1); err != nil {
			released <- err
			return
		}
		released <- barriers.coordinator.Release("after-effect/attempt-1")
	}()

	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivityWithOptions(
		Activities{WorkerID: "worker-1"}.Apply,
		activity.RegisterOptions{Name: activityName},
	)
	environment.ExecuteWorkflow(externalEffectWorkflow, input)
	if err := <-released; err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	var receipt string
	if err := environment.GetWorkflowResult(&receipt); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if receipt != "database:effect-activity-test" {
		t.Fatalf("receipt = %q", receipt)
	}
	attempts, err := readAttempts(input.StorePath)
	if err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if len(attempts) != 2 || attempts[0].Outcome != OutcomeApplied || attempts[1].Outcome != OutcomeDeduplicated {
		t.Fatalf("attempts = %+v", attempts)
	}
}

func TestActivityRejectsMissingRuntimeConfiguration(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(Activities{}.Apply)
	if _, err := environment.ExecuteActivity(Activities{}.Apply, WorkflowInput{}); err == nil {
		t.Fatal("Activity accepted missing configuration")
	}
}
