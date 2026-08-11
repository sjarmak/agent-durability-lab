package lab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

var errSupervisorExecutionUnavailable = errors.New("supervised execution is unavailable")

type capabilityGenerator func() (string, error)

type supervisedRunFunc func(
	context.Context,
	*workstore.Store,
	workstore.Lease,
) (supervisedResult, error)

type supervisedResult struct {
	VendorSessionID   string
	PhysicalAttemptID string
	ProcessIdentity   string
	Outcome           workstore.Outcome
}

type supervisorStartRequest struct {
	SessionID               string `json:"session_id"`
	WorkerID                string `json:"worker_id"`
	AgentBuild              string `json:"agent_build,omitempty"`
	Attempt                 int32  `json:"attempt"`
	Replace                 bool   `json:"replace,omitempty"`
	LogicalTurnID           string `json:"logical_turn_id,omitempty"`
	LogicalEffectID         string `json:"logical_effect_id,omitempty"`
	SelectedVendorSessionID string `json:"selected_vendor_session_id,omitempty"`
}

type supervisorReceipt struct {
	Action            workstore.Action  `json:"action"`
	Generation        uint64            `json:"generation"`
	OwnerTokenHash    string            `json:"owner_token_hash"`
	VendorSessionID   string            `json:"vendor_session_id,omitempty"`
	PhysicalAttemptID string            `json:"physical_attempt_id,omitempty"`
	ProcessIdentity   string            `json:"process_identity,omitempty"`
	Outcome           workstore.Outcome `json:"outcome"`
}

type supervisorDecision struct {
	Action         workstore.Action
	SessionID      string
	Generation     uint64
	OwnerTokenHash string
	WorkerID       string
	Attempt        int32
}

type supervisorOption func(*turnSupervisor)

type supervisorStartValidator func(supervisorStartRequest) error

func withSupervisorDecisionObserver(observer func(supervisorDecision)) supervisorOption {
	return func(supervisor *turnSupervisor) {
		supervisor.observeDecision = observer
	}
}

func withSupervisorStartValidator(validate supervisorStartValidator) supervisorOption {
	return func(supervisor *turnSupervisor) {
		supervisor.validateStart = validate
	}
}

type turnSupervisor struct {
	ctx             context.Context
	store           *workstore.Store
	run             supervisedRunFunc
	newCapability   capabilityGenerator
	observeDecision func(supervisorDecision)
	validateStart   supervisorStartValidator

	mu         sync.Mutex
	executions map[workstore.Lease]*supervisedExecution
}

type supervisedExecution struct {
	lease  workstore.Lease
	cancel context.CancelFunc
	done   chan struct{}
	result supervisedResult
	err    error
}

func newTurnSupervisor(
	ctx context.Context,
	store *workstore.Store,
	run supervisedRunFunc,
	newCapability capabilityGenerator,
	options ...supervisorOption,
) *turnSupervisor {
	if ctx == nil {
		ctx = context.Background()
	}
	if newCapability == nil {
		newCapability = newOwnerCapability
	}
	supervisor := &turnSupervisor{
		ctx: ctx, store: store, run: run, newCapability: newCapability,
		executions: make(map[workstore.Lease]*supervisedExecution),
	}
	for _, option := range options {
		option(supervisor)
	}
	return supervisor
}

func (s *turnSupervisor) StartOrAttach(ctx context.Context, request supervisorStartRequest) (supervisorReceipt, error) {
	if ctx == nil || request.SessionID == "" || request.WorkerID == "" || request.Attempt < 1 ||
		s == nil || s.store == nil || s.run == nil || s.newCapability == nil {
		return supervisorReceipt{}, fmt.Errorf("%w: complete supervisor and start request are required", workstore.ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return supervisorReceipt{}, err
	}
	if s.validateStart != nil {
		if err := s.validateStart(request); err != nil {
			return supervisorReceipt{}, fmt.Errorf("%w: %v", workstore.ErrInvalidRequest, err)
		}
	}
	candidate, err := s.newCapability()
	if err != nil {
		return supervisorReceipt{}, fmt.Errorf("generate owner capability: %w", err)
	}
	if candidate == "" {
		return supervisorReceipt{}, fmt.Errorf("%w: generated owner capability is empty", workstore.ErrInvalidRequest)
	}

	s.mu.Lock()
	replaceOwner := request.Replace || s.failedExecutionRequiresReplacement(request.SessionID)
	decision, err := s.store.StartOrAttach(ctx, workstore.StartRequest{
		SessionID: request.SessionID, Mode: workstore.ModeFenced, CandidateOwner: candidate,
		WorkerID: request.WorkerID, AgentBuild: request.AgentBuild, Attempt: request.Attempt,
		ReplaceOwner: replaceOwner,
	})
	if err != nil {
		s.mu.Unlock()
		return supervisorReceipt{}, fmt.Errorf("decide supervised turn: %w", err)
	}

	var execution *supervisedExecution
	var superseded []*supervisedExecution
	switch decision.Action {
	case workstore.ActionLaunch:
		for lease, candidate := range s.executions {
			if lease.SessionID == decision.Lease.SessionID && lease.Generation < decision.Lease.Generation {
				superseded = append(superseded, candidate)
			}
		}
		executionContext, cancel := context.WithCancel(s.ctx)
		execution = &supervisedExecution{
			lease: decision.Lease, cancel: cancel, done: make(chan struct{}),
		}
		s.executions[decision.Lease] = execution
		go s.runExecution(executionContext, execution)
	case workstore.ActionAttach:
		execution = s.executions[decision.Lease]
		if execution == nil {
			s.mu.Unlock()
			return supervisorReceipt{}, fmt.Errorf(
				"%w: session %q generation %d",
				errSupervisorExecutionUnavailable, decision.Lease.SessionID, decision.Lease.Generation,
			)
		}
	case workstore.ActionComplete:
		execution = s.executions[decision.Lease]
	default:
		s.mu.Unlock()
		return supervisorReceipt{}, fmt.Errorf("%w: unsupported supervisor action %q", workstore.ErrInvalidRequest, decision.Action)
	}
	observer := s.observeDecision
	observed := supervisorDecision{
		Action: decision.Action, SessionID: decision.Lease.SessionID,
		Generation: decision.Lease.Generation, OwnerTokenHash: workstore.HashToken(decision.Lease.OwnerToken),
		WorkerID: request.WorkerID, Attempt: request.Attempt,
	}
	s.mu.Unlock()
	for _, stale := range superseded {
		stale.cancel()
	}
	if observer != nil {
		observer(observed)
	}

	if execution == nil {
		if decision.Outcome == nil {
			return supervisorReceipt{}, fmt.Errorf("%w: terminal decision has no outcome", errSupervisorExecutionUnavailable)
		}
		return supervisorReceipt{
			Action: decision.Action, Generation: decision.Lease.Generation,
			OwnerTokenHash: workstore.HashToken(decision.Lease.OwnerToken), Outcome: *decision.Outcome,
		}, nil
	}
	select {
	case <-ctx.Done():
		return supervisorReceipt{}, ctx.Err()
	case <-execution.done:
		if execution.err != nil {
			return supervisorReceipt{}, execution.err
		}
		return supervisorReceipt{
			Action: decision.Action, Generation: decision.Lease.Generation,
			OwnerTokenHash:    workstore.HashToken(decision.Lease.OwnerToken),
			VendorSessionID:   execution.result.VendorSessionID,
			PhysicalAttemptID: execution.result.PhysicalAttemptID,
			ProcessIdentity:   execution.result.ProcessIdentity,
			Outcome:           execution.result.Outcome,
		}, nil
	}
}

func (s *turnSupervisor) failedExecutionRequiresReplacement(sessionID string) bool {
	var current *supervisedExecution
	for lease, execution := range s.executions {
		if lease.SessionID == sessionID && (current == nil || lease.Generation > current.lease.Generation) {
			current = execution
		}
	}
	if current == nil {
		return false
	}
	select {
	case <-current.done:
		return current.err != nil
	default:
		return false
	}
}

func (s *turnSupervisor) Cancel(ctx context.Context, sessionID, requestID string) (workstore.CancelDecision, error) {
	if s == nil || s.store == nil {
		return workstore.CancelDecision{}, fmt.Errorf("%w: supervisor store is required", workstore.ErrInvalidRequest)
	}
	decision, err := s.store.CancelSession(ctx, workstore.CancelRequest{SessionID: sessionID, RequestID: requestID})
	if err != nil {
		return workstore.CancelDecision{}, fmt.Errorf("commit supervisor cancellation: %w", err)
	}
	if decision.Cancellation == nil {
		return decision, nil
	}
	cancellation := decision.Cancellation
	s.mu.Lock()
	var execution *supervisedExecution
	for lease, candidate := range s.executions {
		if lease.SessionID == sessionID && lease.Generation == cancellation.Generation &&
			workstore.HashToken(lease.OwnerToken) == cancellation.OwnerTokenHash {
			execution = candidate
			break
		}
	}
	s.mu.Unlock()
	if execution != nil {
		execution.cancel()
	}
	return decision, nil
}

func (s *turnSupervisor) runExecution(ctx context.Context, execution *supervisedExecution) {
	defer execution.cancel()
	result, err := s.run(ctx, s.store, execution.lease)
	if err == nil {
		if completeErr := s.store.Complete(s.ctx, execution.lease, result.Outcome); completeErr != nil {
			err = fmt.Errorf("publish supervised turn: %w", completeErr)
		}
	}
	if errors.Is(err, workstore.ErrSessionCanceled) {
		if acknowledgeErr := s.acknowledgeCancellation(execution.lease); acknowledgeErr != nil {
			err = errors.Join(err, acknowledgeErr)
		}
	}
	execution.result = result
	execution.err = err
	close(execution.done)
}

func (s *turnSupervisor) acknowledgeCancellation(lease workstore.Lease) error {
	operationContext := context.WithoutCancel(s.ctx)
	snapshot, err := s.store.Snapshot(operationContext, lease.SessionID)
	if err != nil {
		return fmt.Errorf("read cancellation for acknowledgement: %w", err)
	}
	if snapshot.Cancellation == nil || snapshot.Cancellation.Target.Generation != lease.Generation ||
		snapshot.Cancellation.Target.OwnerTokenHash != workstore.HashToken(lease.OwnerToken) ||
		snapshot.Cancellation.Target.Process.PID <= 0 {
		return nil
	}
	if err := s.store.AcknowledgeCancellation(operationContext, workstore.CancellationAcknowledgementRequest{
		RequestID: snapshot.Cancellation.RequestID, Lease: lease,
		Process: snapshot.Cancellation.Target.Process,
	}); err != nil {
		return fmt.Errorf("acknowledge supervised cancellation: %w", err)
	}
	return nil
}

func newOwnerCapability() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
