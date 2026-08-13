package lab

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"go.temporal.io/sdk/activity"
)

type ArrivalFunc func(context.Context, failureinject.Arrival) error

type Activities struct {
	WorkerID string
	Arrive   ArrivalFunc
}

func (a Activities) Produce(ctx context.Context, input WorkflowInput) (ArtifactReference, error) {
	if err := validateWorkflowInput(input); err != nil {
		return ArtifactReference{}, err
	}
	if a.WorkerID == "" {
		return ArtifactReference{}, errors.New("produce Activity requires a Worker identity")
	}
	content, err := readSourceArtifact(input.SourcePath)
	if err != nil {
		return ArtifactReference{}, err
	}
	store, err := NewArtifactStore(input.StoreRoot)
	if err != nil {
		return ArtifactReference{}, err
	}
	attempt := activity.GetInfo(ctx).Attempt
	return store.Produce(ctx, ProduceRequest{
		LogicalID: input.LogicalID,
		Content:   content,
		Attempt:   attempt,
		Mode:      input.Mode,
	}, a.hook(input.LogicalID, input.FailureBoundary, attempt))
}

func (a Activities) Acknowledge(ctx context.Context, input ConsumeInput) (Acknowledgement, error) {
	if a.WorkerID == "" || !input.Mode.valid() || !safeComponent(input.ConsumerID) ||
		!input.FailureBoundary.valid() || input.StoreRoot == "" {
		return Acknowledgement{}, errors.New("acknowledgement Activity requires Worker, store, identities, mode, and boundary")
	}
	if err := validateReference(input.Reference); err != nil {
		return Acknowledgement{}, err
	}
	attempt := activity.GetInfo(ctx).Attempt
	if input.FailureBoundary == BoundaryActivityCompleted && attempt == 1 {
		if err := a.arrive(ctx, input.Reference.LogicalID, input.FailureBoundary, attempt); err != nil {
			return Acknowledgement{}, err
		}
		return Acknowledgement{}, errors.New("activity-completed barrier unexpectedly released")
	}
	store, err := NewArtifactStore(input.StoreRoot)
	if err != nil {
		return Acknowledgement{}, err
	}
	return store.Acknowledge(ctx, AcknowledgeRequest{
		Reference:  input.Reference,
		ConsumerID: input.ConsumerID,
		Attempt:    attempt,
		Mode:       input.Mode,
	}, a.hook(input.Reference.LogicalID, input.FailureBoundary, attempt))
}

func (a Activities) hook(logicalID string, target Boundary, attempt int32) BoundaryHook {
	return func(ctx context.Context, observed Boundary, _ StoreSnapshot) error {
		if observed != target || attempt != 1 {
			return nil
		}
		if err := a.arrive(ctx, logicalID, observed, attempt); err != nil {
			return err
		}
		return errors.New("failure barrier unexpectedly released")
	}
}

func (a Activities) arrive(ctx context.Context, logicalID string, boundary Boundary, attempt int32) error {
	if a.Arrive == nil {
		return errors.New("failure boundary requires an authenticated arrival client")
	}
	return a.Arrive(ctx, failureinject.Arrival{
		ID:         logicalID + "/" + string(boundary) + "/attempt-" + fmt.Sprint(attempt),
		Point:      string(boundary),
		SessionID:  logicalID,
		Generation: 1,
		ActorID:    a.WorkerID,
		PID:        os.Getpid(),
	})
}

func readSourceArtifact(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect source artifact: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > MaxArtifactBytes {
		return nil, fmt.Errorf("%w: source artifact must be a bounded regular file", ErrInvalidArtifact)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open source artifact: %w", err)
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, MaxArtifactBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read source artifact: %w", err)
	}
	if len(content) < 1 || len(content) > MaxArtifactBytes {
		return nil, fmt.Errorf("%w: source artifact changed size while reading", ErrInvalidArtifact)
	}
	return content, nil
}
