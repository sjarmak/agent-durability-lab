package lab

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestArtifactWorkflowPersistsCompactReferenceBeforeAcknowledgement(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	reference := ArtifactReference{
		LogicalID:     "artifact-1",
		Digest:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BlobName:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.blob",
		ReferenceName: "artifact-1.json",
		Size:          8 << 20,
	}
	producerInfo := make(chan activity.Info, 1)
	consumerInfo := make(chan activity.Info, 1)
	environment.RegisterActivityWithOptions(
		func(ctx context.Context, _ WorkflowInput) (ArtifactReference, error) {
			producerInfo <- activity.GetInfo(ctx)
			return reference, nil
		},
		activity.RegisterOptions{Name: produceActivityName},
	)
	environment.RegisterActivityWithOptions(
		func(ctx context.Context, input ConsumeInput) (Acknowledgement, error) {
			consumerInfo <- activity.GetInfo(ctx)
			if input.Reference != reference {
				t.Fatalf("consumer reference = %+v, want %+v", input.Reference, reference)
			}
			return Acknowledgement{
				LogicalID:     reference.LogicalID,
				Digest:        reference.Digest,
				ConsumerID:    input.ConsumerID,
				ReferenceName: reference.ReferenceName,
			}, nil
		},
		activity.RegisterOptions{Name: acknowledgeActivityName},
	)

	input := WorkflowInput{
		StoreRoot:       "/durable/artifacts",
		SourcePath:      "/sealed/input/large-output.bin",
		LogicalID:       "artifact-1",
		ConsumerID:      "consumer-1",
		Mode:            ModeProtected,
		FailureBoundary: BoundaryReferencePublished,
	}
	environment.ExecuteWorkflow(artifactWorkflow, input)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("Workflow failed: %v", err)
	}
	var result WorkflowResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatalf("Workflow result: %v", err)
	}
	if result.Reference != reference || result.Acknowledgement.Digest != reference.Digest {
		t.Fatalf("Workflow result = %+v", result)
	}

	producer := <-producerInfo
	consumer := <-consumerInfo
	if producer.ActivityID != produceActivityID || consumer.ActivityID != acknowledgeActivityID {
		t.Fatalf("Activity IDs = %q, %q", producer.ActivityID, consumer.ActivityID)
	}
	if producer.StartToCloseTimeout != 10*time.Second || consumer.StartToCloseTimeout != 10*time.Second {
		t.Fatalf("StartToCloseTimeouts = %s, %s", producer.StartToCloseTimeout, consumer.StartToCloseTimeout)
	}
}

func TestWorkflowRejectsInvalidInputAndActivityFailures(t *testing.T) {
	t.Parallel()

	valid := WorkflowInput{
		StoreRoot: "/durable/artifacts", SourcePath: "/sealed/input.bin",
		LogicalID: "artifact-1", ConsumerID: "consumer-1", Mode: ModeProtected,
		FailureBoundary: BoundaryReferencePublished,
	}
	for name, mutate := range map[string]func(*WorkflowInput){
		"mode":        func(input *WorkflowInput) { input.Mode = "other" },
		"logical ID":  func(input *WorkflowInput) { input.LogicalID = "../escape" },
		"consumer":    func(input *WorkflowInput) { input.ConsumerID = "" },
		"store path":  func(input *WorkflowInput) { input.StoreRoot = "relative" },
		"source path": func(input *WorkflowInput) { input.SourcePath = "relative" },
		"boundary":    func(input *WorkflowInput) { input.FailureBoundary = "other" },
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if err := validateWorkflowInput(changed); err == nil {
				t.Fatal("invalid Workflow input accepted")
			}
		})
	}

	want := errors.New("producer failed")
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivityWithOptions(
		func(context.Context, WorkflowInput) (ArtifactReference, error) { return ArtifactReference{}, want },
		activity.RegisterOptions{Name: produceActivityName},
	)
	environment.ExecuteWorkflow(artifactWorkflow, valid)
	if err := environment.GetWorkflowError(); err == nil {
		t.Fatal("producer Activity failure was swallowed")
	}
}

func TestWorkflowInputCarriesPathsNotArtifactBytes(t *testing.T) {
	t.Parallel()

	input := WorkflowInput{
		StoreRoot:  "/durable/artifacts",
		SourcePath: "/sealed/input/large-output.bin",
		LogicalID:  "artifact-1",
		ConsumerID: "consumer-1",
		Mode:       ModeProtected,
	}
	if err := validateWorkflowInput(input); err != nil {
		t.Fatalf("validateWorkflowInput: %v", err)
	}
	if input.SourcePath == "" || input.StoreRoot == "" {
		t.Fatal("Workflow input lost external artifact paths")
	}
}
