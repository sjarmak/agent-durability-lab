package workstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const storeLockTimeout = 5 * time.Second

var (
	sessionsBucket = []byte("sessions")
	eventsBucket   = []byte("events")
)

type Store struct {
	path string
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrInvalidRequest)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}

	store := &Store{path: path}
	if err := store.update(context.Background(), func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(sessionsBucket); err != nil {
			return fmt.Errorf("create sessions bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(eventsBucket); err != nil {
			return fmt.Errorf("create events bucket: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) StartOrAttach(ctx context.Context, request StartRequest) (Decision, error) {
	if err := validateStartRequest(request); err != nil {
		return Decision{}, err
	}

	var decision Decision
	var domainErr error
	err := s.update(ctx, func(tx *bolt.Tx) error {
		sessions := tx.Bucket(sessionsBucket)
		record, found, err := loadSession(sessions, request.SessionID)
		if err != nil {
			return err
		}
		if found && record.Mode != request.Mode {
			return fmt.Errorf("%w: session %q mode is %q, requested %q", ErrInvalidRequest, request.SessionID, record.Mode, request.Mode)
		}
		if found && record.Outcome != nil {
			outcome := *record.Outcome
			decision = Decision{Action: ActionComplete, Lease: record.ActiveLease, Outcome: &outcome}
			return appendEvent(tx, Event{
				Kind: "terminal_outcome_observed", SessionID: request.SessionID,
				Generation: record.ActiveLease.Generation, OwnerTokenHash: HashToken(record.ActiveLease.OwnerToken),
				WorkerID: request.WorkerID, Attempt: request.Attempt,
			})
		}
		if found && record.Cancellation != nil {
			domainErr = ErrSessionCanceled
			return appendEvent(tx, Event{
				Kind: "start_rejected_canceled", SessionID: request.SessionID,
				Generation: record.Cancellation.Generation, OwnerTokenHash: record.Cancellation.OwnerTokenHash,
				WorkerID: request.WorkerID, Attempt: request.Attempt,
				Details: map[string]string{"request_id": record.Cancellation.RequestID},
			})
		}

		var eventKind string
		switch {
		case !found:
			record = newSession(request, 1)
			eventKind = "executor_launch_decided"
		case request.Mode == ModeUnsafe:
			record = addExecutor(record, request, record.ActiveLease.Generation+1, false)
			eventKind = "executor_launch_decided"
		case request.ReplaceOwner:
			if request.Attempt <= activeExecutorAttempt(record) {
				domainErr = ErrStaleOwner
				return appendReplacementRejectedEvent(tx, request, record.ActiveLease, "owner_replacement_rejected_stale_attempt")
			}
			record = addExecutor(record, request, record.ActiveLease.Generation+1, true)
			eventKind = "owner_replaced"
		case request.ReplacePendingLaunch && activeExecutorStatus(record) == ExecutorStatusLaunchPending:
			if request.Attempt <= activeExecutorAttempt(record) {
				domainErr = ErrStaleOwner
				return appendReplacementRejectedEvent(tx, request, record.ActiveLease, "pending_launch_replacement_rejected_stale_attempt")
			}
			record = addExecutor(record, request, record.ActiveLease.Generation+1, true)
			eventKind = "pending_launch_replaced"
		default:
			decision = Decision{Action: ActionAttach, Lease: record.ActiveLease}
			return appendAttachEvent(tx, request, record.ActiveLease, activeExecutorStatus(record))
		}
		decision = Decision{Action: ActionLaunch, Lease: record.ActiveLease}
		if err := saveSession(sessions, record); err != nil {
			return err
		}
		return appendDecisionEvent(tx, request, record.ActiveLease, eventKind)
	})
	if err != nil {
		return Decision{}, err
	}
	if domainErr != nil {
		return Decision{}, domainErr
	}
	return decision, nil
}

func (s *Store) RegisterProcess(ctx context.Context, lease Lease, process Process) error {
	if process.PID <= 0 || process.StartIdentity == "" {
		return fmt.Errorf("%w: process PID and start identity are required", ErrInvalidRequest)
	}
	return s.changeExecutor(ctx, lease, func(record sessionRecord, index int) (sessionRecord, Event, error) {
		executor := record.Executors[index]
		if record.Cancellation != nil {
			event := canceledEventWithExecutor("process_registration_rejected_canceled", lease, executor, record.Cancellation)
			event.PID = process.PID
			event.Details["process_start"] = process.StartIdentity
			event.Details["process_group_id"] = fmt.Sprint(process.ProcessGroupID)
			return record, event, ErrSessionCanceled
		}
		if record.Mode == ModeFenced && lease != record.ActiveLease {
			event := staleEventWithExecutor("process_registration_rejected_stale", lease, executor)
			event.PID = process.PID
			event.Details = map[string]string{
				"process_start": process.StartIdentity, "process_group_id": fmt.Sprint(process.ProcessGroupID),
			}
			return record, event, ErrStaleOwner
		}
		executors := append([]executorRecord(nil), record.Executors...)
		executor = executors[index]
		executor.PID = process.PID
		executor.ProcessStart = process.StartIdentity
		executor.ProcessGroupID = process.ProcessGroupID
		executor.Status = ExecutorStatusRunning
		executors[index] = executor
		record.Executors = executors
		return record, Event{
			Kind: "process_registered", SessionID: lease.SessionID, Generation: lease.Generation,
			OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: executor.WorkerID,
			Attempt: executor.Attempt, PID: process.PID,
			Details: map[string]string{
				"process_start": process.StartIdentity, "process_group_id": fmt.Sprint(process.ProcessGroupID),
			},
		}, nil
	})
}

func (s *Store) RecordProgress(ctx context.Context, lease Lease, phase string) error {
	if phase == "" {
		return fmt.Errorf("%w: progress phase is required", ErrInvalidRequest)
	}
	return s.changeExecutor(ctx, lease, func(record sessionRecord, index int) (sessionRecord, Event, error) {
		executor := record.Executors[index]
		if record.Cancellation != nil {
			return record, canceledEventWithExecutor("progress_rejected_canceled", lease, executor, record.Cancellation), ErrSessionCanceled
		}
		if record.Mode == ModeFenced && lease != record.ActiveLease {
			return record, staleEventWithExecutor("progress_rejected_stale", lease, executor), ErrStaleOwner
		}
		if executor.Status != ExecutorStatusRunning {
			return record, staleEventWithExecutor("progress_rejected_not_running", lease, executor), ErrExecutorNotRunning
		}
		return record, Event{
			Kind: "agent_progress", SessionID: lease.SessionID, Generation: lease.Generation,
			OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: executor.WorkerID,
			Attempt: executor.Attempt, PID: executor.PID, Details: map[string]string{"phase": phase},
		}, nil
	})
}

func (s *Store) RecordObservation(ctx context.Context, event Event) error {
	if event.Kind == "" || event.SessionID == "" {
		return fmt.Errorf("%w: observation kind and session are required", ErrInvalidRequest)
	}
	return s.update(ctx, func(tx *bolt.Tx) error {
		return appendEvent(tx, event)
	})
}

func (s *Store) CommitEffect(ctx context.Context, lease Lease, effect Effect) error {
	return s.commitEffect(ctx, lease, effect, false)
}

// CommitEffectOnce makes Effect.ID an idempotency key inside one logical
// session. Reissuing the same ID and value succeeds without another accepted
// effect; reusing the ID for different content fails closed.
func (s *Store) CommitEffectOnce(ctx context.Context, lease Lease, effect Effect) error {
	return s.commitEffect(ctx, lease, effect, true)
}

func (s *Store) commitEffect(ctx context.Context, lease Lease, effect Effect, once bool) error {
	if effect.ID == "" {
		return fmt.Errorf("%w: effect ID is required", ErrInvalidRequest)
	}
	return s.changeExecutor(ctx, lease, func(record sessionRecord, index int) (sessionRecord, Event, error) {
		executor := record.Executors[index]
		if record.Cancellation != nil {
			return record, canceledEventWithExecutor("effect_rejected_canceled", lease, executor, record.Cancellation), ErrSessionCanceled
		}
		if record.Mode == ModeFenced && lease != record.ActiveLease {
			return record, staleEventWithExecutor("effect_rejected_stale", lease, executor), ErrStaleOwner
		}
		if executor.Status != ExecutorStatusRunning {
			return record, staleEventWithExecutor("effect_rejected_not_running", lease, executor), ErrExecutorNotRunning
		}
		if once {
			for _, accepted := range record.Effects {
				if accepted.ID != effect.ID {
					continue
				}
				event := Event{
					SessionID: lease.SessionID, Generation: lease.Generation,
					OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: executor.WorkerID,
					Attempt: executor.Attempt, PID: executor.PID,
					Details: map[string]string{"effect_id": effect.ID, "accepted_generation": fmt.Sprint(accepted.Generation)},
				}
				if accepted.Value == effect.Value {
					event.Kind = "effect_idempotently_observed"
					return record, event, nil
				}
				event.Kind = "effect_conflict_rejected"
				return record, event, ErrEffectConflict
			}
		}
		accepted := AcceptedEffect{
			Effect: effect, Generation: lease.Generation, OwnerTokenHash: HashToken(lease.OwnerToken), AcceptedAt: time.Now().UTC(),
		}
		record.Effects = append(append([]AcceptedEffect(nil), record.Effects...), accepted)
		return record, Event{
			Kind: "effect_accepted", SessionID: lease.SessionID, Generation: lease.Generation,
			OwnerTokenHash: accepted.OwnerTokenHash, WorkerID: executor.WorkerID,
			Attempt: executor.Attempt, PID: executor.PID, Details: map[string]string{"effect_id": effect.ID},
		}, nil
	})
}

// CommitEffectWithoutAuthority exists only for unsafe benchmark controls. It
// records the external mutation while deliberately bypassing generation,
// cancellation, and executor-state checks so the protected path has a real
// falsifier.
func (s *Store) CommitEffectWithoutAuthority(ctx context.Context, lease Lease, effect Effect) error {
	if effect.ID == "" {
		return fmt.Errorf("%w: effect ID is required", ErrInvalidRequest)
	}
	return s.changeExecutor(ctx, lease, func(record sessionRecord, index int) (sessionRecord, Event, error) {
		executor := record.Executors[index]
		accepted := AcceptedEffect{
			Effect: effect, Generation: lease.Generation,
			OwnerTokenHash: HashToken(lease.OwnerToken), AcceptedAt: time.Now().UTC(),
		}
		record.Effects = append(append([]AcceptedEffect(nil), record.Effects...), accepted)
		return record, Event{
			Kind: "effect_accepted_without_authority", SessionID: lease.SessionID,
			Generation: lease.Generation, OwnerTokenHash: accepted.OwnerTokenHash,
			WorkerID: executor.WorkerID, Attempt: executor.Attempt, PID: executor.PID,
			Details: map[string]string{"effect_id": effect.ID},
		}, nil
	})
}

func (s *Store) Complete(ctx context.Context, lease Lease, outcome Outcome) error {
	if outcome.Value == "" {
		return fmt.Errorf("%w: outcome value is required", ErrInvalidRequest)
	}
	return s.changeExecutor(ctx, lease, func(record sessionRecord, index int) (sessionRecord, Event, error) {
		executor := record.Executors[index]
		if record.Cancellation != nil {
			return record, canceledEventWithExecutor("completion_rejected_canceled", lease, executor, record.Cancellation), ErrSessionCanceled
		}
		if record.Mode == ModeFenced && lease != record.ActiveLease {
			return record, staleEventWithExecutor("completion_rejected_stale", lease, executor), ErrStaleOwner
		}
		if record.Outcome != nil {
			kind := "completion_rejected_terminal"
			status := ExecutorStatusTerminalRejected
			domainErr := ErrOutcomeAlreadyAccepted
			if *record.Outcome == outcome {
				kind = "completion_duplicate"
				status = ExecutorStatusTerminalDuplicate
				domainErr = nil
			}
			if executor.Status == ExecutorStatusRunning || executor.Status == ExecutorStatusLaunchPending {
				executors := append([]executorRecord(nil), record.Executors...)
				executor.Status = status
				executors[index] = executor
				record.Executors = executors
			}
			return record, Event{
				Kind: kind, SessionID: lease.SessionID, Generation: lease.Generation,
				OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: executor.WorkerID,
				Attempt: executor.Attempt, PID: executor.PID,
			}, domainErr
		}
		if executor.Status != ExecutorStatusRunning {
			return record, staleEventWithExecutor("completion_rejected_not_running", lease, executor), ErrExecutorNotRunning
		}
		accepted := outcome
		record.Outcome = &accepted
		executors := append([]executorRecord(nil), record.Executors...)
		executor.Status = ExecutorStatusCompleted
		executors[index] = executor
		record.Executors = executors
		return record, Event{
			Kind: "outcome_accepted", SessionID: lease.SessionID, Generation: lease.Generation,
			OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: executor.WorkerID,
			Attempt: executor.Attempt, PID: executor.PID,
		}, nil
	})
}

func (s *Store) CancelSession(ctx context.Context, request CancelRequest) (CancelDecision, error) {
	if request.SessionID == "" || request.RequestID == "" {
		return CancelDecision{}, fmt.Errorf("%w: cancellation session and request IDs are required", ErrInvalidRequest)
	}

	var decision CancelDecision
	err := s.update(ctx, func(tx *bolt.Tx) error {
		sessions := tx.Bucket(sessionsBucket)
		record, found, err := loadSession(sessions, request.SessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: %s", ErrSessionNotFound, request.SessionID)
		}
		if record.Outcome != nil {
			outcome := *record.Outcome
			decision = CancelDecision{Action: CancelActionAlreadyCompleted, Outcome: &outcome}
			return appendEvent(tx, terminalCancellationEvent("cancellation_observed_completed", request, record))
		}
		if record.Cancellation != nil {
			cancellation := *record.Cancellation
			decision = CancelDecision{Action: CancelActionAlreadyCanceled, Cancellation: &cancellation}
			return appendEvent(tx, terminalCancellationEvent("cancellation_observed_already_canceled", request, record))
		}

		cancellation := Cancellation{
			RequestID: request.RequestID, Generation: record.ActiveLease.Generation,
			OwnerTokenHash: HashToken(record.ActiveLease.OwnerToken), CommittedAt: time.Now().UTC(),
		}
		if index := executorIndex(record.Executors, record.ActiveLease); index >= 0 {
			executor := record.Executors[index]
			cancellation.Target = CancellationTarget{
				SessionID: request.SessionID, Generation: executor.Generation,
				OwnerTokenHash: HashToken(executor.OwnerToken),
				Process: Process{
					PID: executor.PID, StartIdentity: executor.ProcessStart, ProcessGroupID: executor.ProcessGroupID,
				},
			}
		}
		record.Cancellation = &cancellation
		executors := append([]executorRecord(nil), record.Executors...)
		for index := range executors {
			if executors[index].Status == ExecutorStatusRunning || executors[index].Status == ExecutorStatusLaunchPending {
				executors[index].Status = ExecutorStatusCanceled
			}
		}
		record.Executors = executors
		if err := saveSession(sessions, record); err != nil {
			return err
		}
		decision = CancelDecision{Action: CancelActionCommitted, Cancellation: &cancellation}
		return appendEvent(tx, terminalCancellationEvent("cancellation_committed", request, record))
	})
	if err != nil {
		return CancelDecision{}, err
	}
	return decision, nil
}

func (s *Store) AcknowledgeCancellation(ctx context.Context, request CancellationAcknowledgementRequest) error {
	if request.RequestID == "" || request.Process.PID <= 0 || request.Process.StartIdentity == "" {
		return fmt.Errorf("%w: cancellation request, PID, and process start identity are required", ErrInvalidRequest)
	}
	return s.changeExecutor(ctx, request.Lease, func(record sessionRecord, index int) (sessionRecord, Event, error) {
		executor := record.Executors[index]
		if record.Cancellation == nil {
			return record, staleEventWithExecutor(
				"cancellation_ack_rejected_not_canceled", request.Lease, executor,
			), ErrSessionNotCanceled
		}
		cancellation := *record.Cancellation
		target := CancellationTarget{
			SessionID: request.Lease.SessionID, Generation: request.Lease.Generation,
			OwnerTokenHash: HashToken(request.Lease.OwnerToken), Process: request.Process,
		}
		if request.RequestID != cancellation.RequestID || target != cancellation.Target {
			return record, canceledEventWithExecutor(
				"cancellation_ack_rejected_stale", request.Lease, executor, record.Cancellation,
			), ErrStaleOwner
		}
		if cancellation.Acknowledgement != nil {
			return record, canceledEventWithExecutor(
				"cancellation_acknowledgement_duplicate", request.Lease, executor, record.Cancellation,
			), nil
		}
		cancellation.Acknowledgement = &CancellationAcknowledgement{
			Generation: request.Lease.Generation, OwnerTokenHash: HashToken(request.Lease.OwnerToken),
			Process: request.Process, AcknowledgedAt: time.Now().UTC(),
		}
		record.Cancellation = &cancellation
		return record, canceledEventWithExecutor(
			"cancellation_acknowledged", request.Lease, executor, record.Cancellation,
		), nil
	})
}

func (s *Store) Snapshot(ctx context.Context, sessionID string) (Snapshot, error) {
	var snapshot Snapshot
	err := s.view(ctx, func(tx *bolt.Tx) error {
		record, found, err := loadSession(tx.Bucket(sessionsBucket), sessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: %s", ErrSessionNotFound, sessionID)
		}
		events, err := readEvents(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		snapshot = snapshotFromRecord(record, events)
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (s *Store) changeExecutor(
	ctx context.Context,
	lease Lease,
	change func(sessionRecord, int) (sessionRecord, Event, error),
) error {
	if lease.SessionID == "" || lease.Generation == 0 || lease.OwnerToken == "" {
		return fmt.Errorf("%w: complete lease is required", ErrInvalidRequest)
	}
	var domainErr error
	err := s.update(ctx, func(tx *bolt.Tx) error {
		sessions := tx.Bucket(sessionsBucket)
		record, found, err := loadSession(sessions, lease.SessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: %s", ErrSessionNotFound, lease.SessionID)
		}
		index := executorIndex(record.Executors, lease)
		if index < 0 {
			return fmt.Errorf("%w: owner is not registered", ErrStaleOwner)
		}
		updated, event, changeErr := change(record, index)
		if err := saveSession(sessions, updated); err != nil {
			return err
		}
		if err := appendEvent(tx, event); err != nil {
			return err
		}
		domainErr = changeErr
		return nil
	})
	if err != nil {
		return err
	}
	return domainErr
}

func (s *Store) update(ctx context.Context, fn func(*bolt.Tx) error) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := bolt.Open(s.path, 0o600, &bolt.Options{Timeout: storeLockTimeout})
	if err != nil {
		return fmt.Errorf("open work store: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, db.Close())
	}()
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(tx)
	}); err != nil {
		return fmt.Errorf("update work store: %w", err)
	}
	return nil
}

func (s *Store) view(ctx context.Context, fn func(*bolt.Tx) error) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := bolt.Open(s.path, 0o600, &bolt.Options{Timeout: storeLockTimeout, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open work store: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, db.Close())
	}()
	if err := db.View(fn); err != nil {
		return fmt.Errorf("view work store: %w", err)
	}
	return nil
}

func validateStartRequest(request StartRequest) error {
	if request.SessionID == "" || request.CandidateOwner == "" || request.WorkerID == "" || request.Attempt < 1 || !request.Mode.Valid() {
		return fmt.Errorf("%w: session, mode, candidate owner, worker, and positive attempt are required", ErrInvalidRequest)
	}
	if request.ReplaceOwner && request.Mode != ModeFenced {
		return fmt.Errorf("%w: replacement requires fenced mode", ErrInvalidRequest)
	}
	if request.ReplacePendingLaunch && request.Mode != ModeFenced {
		return fmt.Errorf("%w: pending launch replacement requires fenced mode", ErrInvalidRequest)
	}
	if request.ReplaceOwner && request.ReplacePendingLaunch {
		return fmt.Errorf("%w: replacement policies are mutually exclusive", ErrInvalidRequest)
	}
	return nil
}

func newSession(request StartRequest, generation uint64) sessionRecord {
	lease := Lease{SessionID: request.SessionID, Generation: generation, OwnerToken: request.CandidateOwner}
	return sessionRecord{
		SessionID:   request.SessionID,
		Mode:        request.Mode,
		ActiveLease: lease,
		Executors:   []executorRecord{newExecutor(request, lease)},
	}
}

func addExecutor(record sessionRecord, request StartRequest, generation uint64, supersede bool) sessionRecord {
	executors := append([]executorRecord(nil), record.Executors...)
	if supersede {
		for index := range executors {
			if executors[index].Lease == record.ActiveLease {
				executors[index].Status = ExecutorStatusSuperseded
				break
			}
		}
	}
	lease := Lease{SessionID: request.SessionID, Generation: generation, OwnerToken: request.CandidateOwner}
	executors = append(executors, newExecutor(request, lease))
	record.ActiveLease = lease
	record.Executors = executors
	return record
}

func newExecutor(request StartRequest, lease Lease) executorRecord {
	return executorRecord{
		Lease: lease, WorkerID: request.WorkerID, AgentBuild: request.AgentBuild,
		Attempt: request.Attempt, Status: ExecutorStatusLaunchPending, StartedAt: time.Now().UTC(),
	}
}

func activeExecutorStatus(record sessionRecord) string {
	index := executorIndex(record.Executors, record.ActiveLease)
	if index < 0 {
		return ""
	}
	return record.Executors[index].Status
}

func activeExecutorAttempt(record sessionRecord) int32 {
	index := executorIndex(record.Executors, record.ActiveLease)
	if index < 0 {
		return 0
	}
	return record.Executors[index].Attempt
}

func loadSession(bucket *bolt.Bucket, sessionID string) (sessionRecord, bool, error) {
	data := bucket.Get([]byte(sessionID))
	if data == nil {
		return sessionRecord{}, false, nil
	}
	var record sessionRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return sessionRecord{}, false, fmt.Errorf("decode session %q: %w", sessionID, err)
	}
	return record, true, nil
}

func saveSession(bucket *bolt.Bucket, record sessionRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode session %q: %w", record.SessionID, err)
	}
	if err := bucket.Put([]byte(record.SessionID), data); err != nil {
		return fmt.Errorf("save session %q: %w", record.SessionID, err)
	}
	return nil
}

func appendDecisionEvent(tx *bolt.Tx, request StartRequest, lease Lease, kind string) error {
	return appendEvent(tx, Event{
		Kind: kind, SessionID: lease.SessionID, Generation: lease.Generation,
		OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: request.WorkerID, Attempt: request.Attempt,
	})
}

func appendAttachEvent(tx *bolt.Tx, request StartRequest, lease Lease, status string) error {
	return appendEvent(tx, Event{
		Kind: "activity_reattached", SessionID: lease.SessionID, Generation: lease.Generation,
		OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: request.WorkerID, Attempt: request.Attempt,
		Details: map[string]string{"executor_status": status},
	})
}

func appendReplacementRejectedEvent(tx *bolt.Tx, request StartRequest, lease Lease, kind string) error {
	return appendEvent(tx, Event{
		Kind: kind, SessionID: lease.SessionID, Generation: lease.Generation,
		OwnerTokenHash: HashToken(lease.OwnerToken), WorkerID: request.WorkerID, Attempt: request.Attempt,
		Details: map[string]string{"reason": "attempt_must_increase"},
	})
}

func appendEvent(tx *bolt.Tx, event Event) error {
	bucket := tx.Bucket(eventsBucket)
	sequence, err := bucket.NextSequence()
	if err != nil {
		return fmt.Errorf("allocate event sequence: %w", err)
	}
	event.Sequence = sequence
	event.Time = time.Now().UTC()
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], sequence)
	if err := bucket.Put(key[:], data); err != nil {
		return fmt.Errorf("save event: %w", err)
	}
	return nil
}

func readEvents(ctx context.Context, tx *bolt.Tx, sessionID string) ([]Event, error) {
	events := make([]Event, 0)
	cursor := tx.Bucket(eventsBucket).Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var event Event
		if err := json.Unmarshal(value, &event); err != nil {
			return nil, fmt.Errorf("decode event sequence %d: %w", binary.BigEndian.Uint64(key), err)
		}
		if event.SessionID == sessionID {
			events = append(events, event)
		}
	}
	return events, nil
}

func executorIndex(executors []executorRecord, lease Lease) int {
	for index, executor := range executors {
		if executor.Lease == lease {
			return index
		}
	}
	return -1
}

func snapshotFromRecord(record sessionRecord, events []Event) Snapshot {
	executors := make([]Executor, 0, len(record.Executors))
	for _, executor := range record.Executors {
		executors = append(executors, Executor{
			Generation: executor.Generation, OwnerTokenHash: HashToken(executor.OwnerToken),
			WorkerID: executor.WorkerID, AgentBuild: executor.AgentBuild, Attempt: executor.Attempt,
			PID: executor.PID, ProcessStart: executor.ProcessStart, ProcessGroupID: executor.ProcessGroupID,
			Status: executor.Status, StartedAt: executor.StartedAt,
		})
	}
	var outcome *Outcome
	if record.Outcome != nil {
		copy := *record.Outcome
		outcome = &copy
	}
	var cancellation *Cancellation
	if record.Cancellation != nil {
		copy := *record.Cancellation
		if record.Cancellation.Acknowledgement != nil {
			acknowledgement := *record.Cancellation.Acknowledgement
			copy.Acknowledgement = &acknowledgement
		}
		cancellation = &copy
	}
	return Snapshot{
		SessionID: record.SessionID, Mode: record.Mode, ActiveGeneration: record.ActiveLease.Generation,
		ActiveOwnerTokenHash: HashToken(record.ActiveLease.OwnerToken), Executors: executors,
		Effects: append([]AcceptedEffect(nil), record.Effects...), Outcome: outcome,
		Cancellation: cancellation, Events: events,
	}
}

func staleEvent(kind string, lease Lease) Event {
	return Event{Kind: kind, SessionID: lease.SessionID, Generation: lease.Generation, OwnerTokenHash: HashToken(lease.OwnerToken)}
}

func staleEventWithExecutor(kind string, lease Lease, executor executorRecord) Event {
	event := staleEvent(kind, lease)
	event.WorkerID = executor.WorkerID
	event.Attempt = executor.Attempt
	event.PID = executor.PID
	return event
}

func canceledEventWithExecutor(kind string, lease Lease, executor executorRecord, cancellation *Cancellation) Event {
	event := staleEventWithExecutor(kind, lease, executor)
	event.Details = map[string]string{"cancellation_request_id": cancellation.RequestID}
	return event
}

func terminalCancellationEvent(kind string, request CancelRequest, record sessionRecord) Event {
	event := Event{
		Kind: kind, SessionID: request.SessionID, Generation: record.ActiveLease.Generation,
		OwnerTokenHash: HashToken(record.ActiveLease.OwnerToken),
		Details:        map[string]string{"request_id": request.RequestID},
	}
	index := executorIndex(record.Executors, record.ActiveLease)
	if index >= 0 {
		executor := record.Executors[index]
		event.WorkerID = executor.WorkerID
		event.Attempt = executor.Attempt
		event.PID = executor.PID
	}
	return event
}
