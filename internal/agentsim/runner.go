package agentsim

import (
	"context"
	"errors"
	"fmt"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type Config struct {
	Lease                    workstore.Lease   `json:"lease"`
	ActorID                  string            `json:"actor_id"`
	PID                      int               `json:"pid"`
	ProcessStart             string            `json:"process_start"`
	ProcessGroupID           int               `json:"process_group_id,omitempty"`
	Effect                   workstore.Effect  `json:"effect"`
	Outcome                  workstore.Outcome `json:"outcome"`
	BlockBeforeRegistration  bool              `json:"block_before_registration,omitempty"`
	SpawnToolChild           bool              `json:"spawn_tool_child,omitempty"`
	// BypassAuthorityForEffect is restricted to unsafe benchmark controls.
	BypassAuthorityForEffect bool `json:"bypass_authority_for_effect,omitempty"`
}

type Result struct {
	EffectAccepted      bool   `json:"effect_accepted"`
	CompletionAccepted  bool   `json:"completion_accepted"`
	EffectRejection     string `json:"effect_rejection,omitempty"`
	CompletionRejection string `json:"completion_rejection,omitempty"`
}

type Runner struct {
	store   *workstore.Store
	barrier *failureinject.Client
}

func New(store *workstore.Store, barrier *failureinject.Client) *Runner {
	return &Runner{store: store, barrier: barrier}
}

func (r *Runner) Run(ctx context.Context, config Config) (Result, error) {
	if err := r.validate(config); err != nil {
		return Result{}, err
	}
	if config.BlockBeforeRegistration {
		if err := r.arrive(ctx, config, "before-registration"); err != nil {
			return Result{}, err
		}
	}
	if err := r.store.RegisterProcess(ctx, config.Lease, workstore.Process{
		PID: config.PID, StartIdentity: config.ProcessStart, ProcessGroupID: config.ProcessGroupID,
	}); err != nil {
		return Result{}, fmt.Errorf("register agent process: %w", err)
	}
	if err := r.store.RecordProgress(ctx, config.Lease, "externally-observable-work-started"); err != nil {
		return Result{}, fmt.Errorf("record agent progress: %w", err)
	}

	if err := r.arrive(ctx, config, "before-effect"); err != nil {
		return Result{}, err
	}
	result := Result{EffectAccepted: true}
	commitEffect := r.store.CommitEffect
	if config.BypassAuthorityForEffect {
		commitEffect = r.store.CommitEffectWithoutAuthority
	}
	if err := commitEffect(ctx, config.Lease, config.Effect); err != nil {
		switch {
		case errors.Is(err, workstore.ErrStaleOwner):
			result.EffectAccepted = false
			result.EffectRejection = "stale_owner"
		case errors.Is(err, workstore.ErrSessionCanceled):
			result.EffectAccepted = false
			result.EffectRejection = "session_canceled"
		default:
			return Result{}, fmt.Errorf("commit agent effect: %w", err)
		}
	}

	if err := r.arrive(ctx, config, "before-completion"); err != nil {
		return Result{}, err
	}
	result.CompletionAccepted = true
	if err := r.store.Complete(ctx, config.Lease, config.Outcome); err != nil {
		switch {
		case errors.Is(err, workstore.ErrStaleOwner):
			result.CompletionAccepted = false
			result.CompletionRejection = "stale_owner"
		case errors.Is(err, workstore.ErrOutcomeAlreadyAccepted):
			result.CompletionAccepted = false
			result.CompletionRejection = "terminal_outcome_exists"
		case errors.Is(err, workstore.ErrSessionCanceled):
			result.CompletionAccepted = false
			result.CompletionRejection = "session_canceled"
		default:
			return Result{}, fmt.Errorf("complete agent session: %w", err)
		}
	}
	return result, nil
}

func (r *Runner) validate(config Config) error {
	if r.store == nil || r.barrier == nil {
		return errors.New("agent runner requires a store and barrier client")
	}
	if config.ActorID == "" || config.PID <= 0 || config.ProcessStart == "" {
		return errors.New("agent runner requires actor, PID, and process start identity")
	}
	if config.Lease.SessionID == "" || config.Lease.Generation == 0 || config.Lease.OwnerToken == "" {
		return errors.New("agent runner requires a complete lease")
	}
	if config.Effect.ID == "" || config.Outcome.Value == "" {
		return errors.New("agent runner requires an effect and outcome")
	}
	return nil
}

func (r *Runner) arrive(ctx context.Context, config Config, phase string) error {
	point := fmt.Sprintf("%s/%d", phase, config.Lease.Generation)
	arrival := failureinject.Arrival{
		ID: fmt.Sprintf("%s:%s", config.ActorID, point), Point: point,
		SessionID: config.Lease.SessionID, OwnerTokenHash: workstore.HashToken(config.Lease.OwnerToken),
		Generation: config.Lease.Generation, ActorID: config.ActorID, PID: config.PID,
		ProcessStart: config.ProcessStart,
	}
	if err := r.barrier.Arrive(ctx, arrival); err != nil {
		return fmt.Errorf("arrive at barrier %q: %w", point, err)
	}
	return nil
}
