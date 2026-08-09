package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"
)

const databaseTimeout = 10 * time.Second

var (
	stateBucket = []byte("provider-state")
	stateKey    = []byte("current")
)

type Mode string

const (
	ModeUnsafe     Mode = "unsafe"
	ModeIdempotent Mode = "idempotent"
	ModeFenced     Mode = "fenced"
)

func (m Mode) Valid() bool {
	return m == ModeUnsafe || m == ModeIdempotent || m == ModeFenced
}

type Operation string

const (
	OperationStart             Operation = "start"
	OperationCommand           Operation = "command"
	OperationSnapshot          Operation = "snapshot"
	OperationStartFromSnapshot Operation = "start-from-snapshot"
	OperationStop              Operation = "stop"
)

func (o Operation) Valid() bool {
	return o == OperationStart || o == OperationCommand || o == OperationSnapshot ||
		o == OperationStartFromSnapshot || o == OperationStop
}

var (
	ErrInvalidRequest = errors.New("invalid provider request")
	ErrStaleAuthority = errors.New("stale provider authority")
	ErrStateNotFound  = errors.New("provider state not found")
)

type Authority struct {
	Generation uint64
	Capability string
}

type AuthorityState struct {
	Generation       uint64 `json:"generation"`
	CapabilitySHA256 string `json:"capability_sha256"`
}

type Request struct {
	Kind              Operation
	OperationID       string
	PhysicalAttemptID string
	InstanceID        string
	SnapshotID        string
	LogicalEffectID   string
	Payload           string
	Generation        uint64
	Capability        string
	WorkerIdentity    string
	TemporalAttempt   int32
}

type Result struct {
	Applied      bool   `json:"applied"`
	InstanceID   string `json:"instance_id,omitempty"`
	SnapshotID   string `json:"snapshot_id,omitempty"`
	ReceiptID    string `json:"receipt_id"`
	WorkspaceSHA string `json:"workspace_sha256,omitempty"`
}

type Attempt struct {
	Kind              Operation `json:"kind"`
	OperationID       string    `json:"operation_id"`
	PhysicalAttemptID string    `json:"physical_attempt_id"`
	LogicalEffectID   string    `json:"logical_effect_id,omitempty"`
	Generation        uint64    `json:"generation,omitempty"`
	CapabilitySHA256  string    `json:"capability_sha256,omitempty"`
	WorkerIdentity    string    `json:"worker_identity,omitempty"`
	TemporalAttempt   int32     `json:"temporal_attempt,omitempty"`
	Applied           bool      `json:"applied"`
	Rejection         string    `json:"rejection,omitempty"`
	DuplicateOf       string    `json:"duplicate_of,omitempty"`
	ObservedAt        time.Time `json:"observed_at"`
	Result            Result    `json:"result"`
}

type InstanceState struct {
	InstanceID       string   `json:"instance_id"`
	ParentSnapshotID string   `json:"parent_snapshot_id,omitempty"`
	Active           bool     `json:"active"`
	Effects          []string `json:"effects"`
	WorkspaceSHA256  string   `json:"workspace_sha256"`
}

type SnapshotState struct {
	SnapshotID       string   `json:"snapshot_id"`
	SourceInstanceID string   `json:"source_instance_id"`
	ParentSnapshotID string   `json:"parent_snapshot_id,omitempty"`
	Effects          []string `json:"effects"`
	WorkspaceSHA256  string   `json:"workspace_sha256"`
}

type State struct {
	Mode         Mode            `json:"mode"`
	Authority    AuthorityState  `json:"authority"`
	Instances    []InstanceState `json:"instances"`
	Snapshots    []SnapshotState `json:"snapshots"`
	Attempts     []Attempt       `json:"attempts"`
	NextInstance int             `json:"next_instance"`
	NextSnapshot int             `json:"next_snapshot"`
}

func (s State) Instance(instanceID string) InstanceState {
	for _, instance := range s.Instances {
		if instance.InstanceID == instanceID {
			return cloneInstance(instance)
		}
	}
	return InstanceState{}
}

func (s State) Snapshot(snapshotID string) SnapshotState {
	for _, snapshot := range s.Snapshots {
		if snapshot.SnapshotID == snapshotID {
			return cloneSnapshot(snapshot)
		}
	}
	return SnapshotState{}
}

type Store struct {
	path string
}

func Create(path string, mode Mode) (*Store, error) {
	if path == "" || !mode.Valid() {
		return nil, fmt.Errorf("%w: path and supported mode are required", ErrInvalidRequest)
	}
	resolved, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve provider database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0o750); err != nil {
		return nil, fmt.Errorf("create provider database directory: %w", err)
	}
	file, err := os.OpenFile(resolved, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create provider database: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close provider database placeholder: %w", err)
	}
	store := &Store{path: resolved}
	initial := State{Mode: mode}
	if err := store.update(func(State) (State, error) { return initial, nil }); err != nil {
		return nil, err
	}
	return store, nil
}

func Open(path string) (*Store, error) {
	resolved, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve provider database path: %w", err)
	}
	if info, err := os.Stat(resolved); err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s", ErrStateNotFound, resolved)
	}
	return &Store{path: resolved}, nil
}

func (s *Store) SetAuthority(ctx context.Context, authority Authority) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if authority.Generation == 0 || authority.Capability == "" {
		return fmt.Errorf("%w: authority generation and capability are required", ErrInvalidRequest)
	}
	return s.update(func(state State) (State, error) {
		capabilityHash := hashString(authority.Capability)
		if authority.Generation < state.Authority.Generation ||
			(authority.Generation == state.Authority.Generation &&
				state.Authority.CapabilitySHA256 != "" && state.Authority.CapabilitySHA256 != capabilityHash) {
			return state, ErrStaleAuthority
		}
		state.Authority = AuthorityState{
			Generation: authority.Generation, CapabilitySHA256: capabilityHash,
		}
		return state, nil
	})
}

func (s *Store) Apply(ctx context.Context, request Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	var result Result
	var applyErr error
	err := s.update(func(state State) (State, error) {
		state, result, applyErr = applyToState(state, request)
		return state, nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, applyErr
}

func (s *Store) Snapshot(ctx context.Context) (state State, returnErr error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	database, err := bolt.Open(s.path, 0o600, &bolt.Options{ReadOnly: true, Timeout: databaseTimeout})
	if err != nil {
		return State{}, fmt.Errorf("open provider database: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()
	err = database.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(stateBucket)
		if bucket == nil || bucket.Get(stateKey) == nil {
			return ErrStateNotFound
		}
		return json.Unmarshal(bucket.Get(stateKey), &state)
	})
	if err != nil {
		return State{}, fmt.Errorf("read provider state: %w", err)
	}
	return cloneState(state), nil
}

func (s *Store) update(change func(State) (State, error)) (returnErr error) {
	database, err := bolt.Open(s.path, 0o600, &bolt.Options{Timeout: databaseTimeout})
	if err != nil {
		return fmt.Errorf("open provider database: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()
	return database.Update(func(transaction *bolt.Tx) error {
		bucket, err := transaction.CreateBucketIfNotExists(stateBucket)
		if err != nil {
			return fmt.Errorf("create provider state bucket: %w", err)
		}
		var current State
		if encoded := bucket.Get(stateKey); encoded != nil {
			if err := json.Unmarshal(encoded, &current); err != nil {
				return fmt.Errorf("decode provider state: %w", err)
			}
		}
		next, err := change(cloneState(current))
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(next)
		if err != nil {
			return fmt.Errorf("encode provider state: %w", err)
		}
		return bucket.Put(stateKey, encoded)
	})
}

func applyToState(state State, request Request) (State, Result, error) {
	if prior, found := attemptByPhysicalID(state.Attempts, request.PhysicalAttemptID); found {
		if prior.OperationID != request.OperationID {
			return state, Result{}, fmt.Errorf("%w: physical attempt ID reused", ErrInvalidRequest)
		}
		return state, prior.Result, rejectionError(prior.Rejection)
	}
	if state.Mode == ModeFenced && request.Kind == OperationCommand && !authorized(state, request) {
		attempt := newAttempt(state, request, Result{}, false)
		attempt.Rejection = "stale_authority"
		state.Attempts = append(state.Attempts, attempt)
		return state, Result{}, ErrStaleAuthority
	}
	if state.Mode != ModeUnsafe {
		if prior, found := appliedAttemptByOperation(state.Attempts, request.OperationID); found {
			result := prior.Result
			result.Applied = false
			attempt := newAttempt(state, request, result, false)
			attempt.DuplicateOf = prior.PhysicalAttemptID
			state.Attempts = append(state.Attempts, attempt)
			return state, result, nil
		}
	}
	next, result, err := applyOperation(state, request)
	if err != nil {
		return state, Result{}, err
	}
	next.Attempts = append(next.Attempts, newAttempt(next, request, result, true))
	return next, result, nil
}

func applyOperation(state State, request Request) (State, Result, error) {
	switch request.Kind {
	case OperationStart:
		return startInstance(state, "")
	case OperationCommand:
		return applyCommand(state, request)
	case OperationSnapshot:
		return createSnapshot(state, request.InstanceID)
	case OperationStartFromSnapshot:
		return startFromSnapshot(state, request.SnapshotID)
	case OperationStop:
		return stopInstance(state, request.InstanceID)
	default:
		return state, Result{}, fmt.Errorf("%w: unsupported operation", ErrInvalidRequest)
	}
}

func startInstance(state State, parentSnapshotID string) (State, Result, error) {
	state.NextInstance++
	instance := InstanceState{
		InstanceID:       fmt.Sprintf("instance-%06d", state.NextInstance),
		ParentSnapshotID: parentSnapshotID, Active: true, WorkspaceSHA256: workspaceHash(nil),
	}
	state.Instances = append(state.Instances, instance)
	result := Result{Applied: true, InstanceID: instance.InstanceID, WorkspaceSHA: instance.WorkspaceSHA256}
	result.ReceiptID = receiptID(OperationStart, instance.InstanceID, "")
	return state, result, nil
}

func applyCommand(state State, request Request) (State, Result, error) {
	index := instanceIndex(state.Instances, request.InstanceID)
	if index < 0 || !state.Instances[index].Active {
		return state, Result{}, fmt.Errorf("%w: active instance is required", ErrInvalidRequest)
	}
	effects := append(slices.Clone(state.Instances[index].Effects), request.LogicalEffectID)
	instance := cloneInstance(state.Instances[index])
	instance.Effects = effects
	instance.WorkspaceSHA256 = workspaceHash(effects)
	state.Instances[index] = instance
	result := Result{Applied: true, InstanceID: instance.InstanceID, WorkspaceSHA: instance.WorkspaceSHA256}
	result.ReceiptID = receiptID(OperationCommand, instance.InstanceID, request.LogicalEffectID)
	return state, result, nil
}

func createSnapshot(state State, instanceID string) (State, Result, error) {
	index := instanceIndex(state.Instances, instanceID)
	if index < 0 || !state.Instances[index].Active {
		return state, Result{}, fmt.Errorf("%w: active instance is required", ErrInvalidRequest)
	}
	instance := state.Instances[index]
	state.NextSnapshot++
	snapshot := SnapshotState{
		SnapshotID:       fmt.Sprintf("snapshot-%06d", state.NextSnapshot),
		SourceInstanceID: instance.InstanceID, ParentSnapshotID: instance.ParentSnapshotID,
		Effects: slices.Clone(instance.Effects), WorkspaceSHA256: instance.WorkspaceSHA256,
	}
	state.Snapshots = append(state.Snapshots, snapshot)
	result := Result{
		Applied: true, InstanceID: instance.InstanceID, SnapshotID: snapshot.SnapshotID,
		WorkspaceSHA: snapshot.WorkspaceSHA256,
	}
	result.ReceiptID = receiptID(OperationSnapshot, instance.InstanceID, snapshot.SnapshotID)
	return state, result, nil
}

func startFromSnapshot(state State, snapshotID string) (State, Result, error) {
	snapshot := state.Snapshot(snapshotID)
	if snapshot.SnapshotID == "" {
		return state, Result{}, fmt.Errorf("%w: snapshot is required", ErrInvalidRequest)
	}
	next, result, err := startInstance(state, snapshotID)
	if err != nil {
		return state, Result{}, err
	}
	index := instanceIndex(next.Instances, result.InstanceID)
	instance := cloneInstance(next.Instances[index])
	instance.Effects = slices.Clone(snapshot.Effects)
	instance.WorkspaceSHA256 = snapshot.WorkspaceSHA256
	next.Instances[index] = instance
	result.WorkspaceSHA = snapshot.WorkspaceSHA256
	result.ReceiptID = receiptID(OperationStartFromSnapshot, result.InstanceID, snapshotID)
	return next, result, nil
}

func stopInstance(state State, instanceID string) (State, Result, error) {
	index := instanceIndex(state.Instances, instanceID)
	if index < 0 {
		return state, Result{}, fmt.Errorf("%w: instance is required", ErrInvalidRequest)
	}
	instance := cloneInstance(state.Instances[index])
	instance.Active = false
	state.Instances[index] = instance
	result := Result{Applied: true, InstanceID: instanceID, WorkspaceSHA: instance.WorkspaceSHA256}
	result.ReceiptID = receiptID(OperationStop, instanceID, "")
	return state, result, nil
}

func validateRequest(request Request) error {
	if !request.Kind.Valid() || request.OperationID == "" || request.PhysicalAttemptID == "" {
		return fmt.Errorf("%w: operation and attempt identities are required", ErrInvalidRequest)
	}
	switch request.Kind {
	case OperationCommand:
		if request.InstanceID == "" || request.LogicalEffectID == "" {
			return fmt.Errorf("%w: command instance and effect identity are required", ErrInvalidRequest)
		}
	case OperationSnapshot, OperationStop:
		if request.InstanceID == "" {
			return fmt.Errorf("%w: instance identity is required", ErrInvalidRequest)
		}
	case OperationStartFromSnapshot:
		if request.SnapshotID == "" {
			return fmt.Errorf("%w: snapshot identity is required", ErrInvalidRequest)
		}
	}
	return nil
}

func authorized(state State, request Request) bool {
	return request.Generation == state.Authority.Generation && request.Generation > 0 &&
		hashString(request.Capability) == state.Authority.CapabilitySHA256
}

func newAttempt(state State, request Request, result Result, applied bool) Attempt {
	observedAt := time.Now().UTC()
	if count := len(state.Attempts); count > 0 && !observedAt.After(state.Attempts[count-1].ObservedAt) {
		observedAt = state.Attempts[count-1].ObservedAt.Add(time.Nanosecond)
	}
	result.Applied = applied
	return Attempt{
		Kind: request.Kind, OperationID: request.OperationID,
		PhysicalAttemptID: request.PhysicalAttemptID, LogicalEffectID: request.LogicalEffectID,
		Generation: request.Generation, CapabilitySHA256: hashOptional(request.Capability),
		WorkerIdentity: request.WorkerIdentity, TemporalAttempt: request.TemporalAttempt,
		Applied: applied, ObservedAt: observedAt, Result: result,
	}
}

func attemptByPhysicalID(attempts []Attempt, physicalID string) (Attempt, bool) {
	for _, attempt := range attempts {
		if attempt.PhysicalAttemptID == physicalID {
			return attempt, true
		}
	}
	return Attempt{}, false
}

func appliedAttemptByOperation(attempts []Attempt, operationID string) (Attempt, bool) {
	for _, attempt := range attempts {
		if attempt.OperationID == operationID && attempt.Applied {
			return attempt, true
		}
	}
	return Attempt{}, false
}

func instanceIndex(instances []InstanceState, instanceID string) int {
	for index, instance := range instances {
		if instance.InstanceID == instanceID {
			return index
		}
	}
	return -1
}

func rejectionError(rejection string) error {
	if rejection == "stale_authority" {
		return ErrStaleAuthority
	}
	return nil
}

func cloneState(state State) State {
	next := state
	next.Instances = make([]InstanceState, len(state.Instances))
	for index, instance := range state.Instances {
		next.Instances[index] = cloneInstance(instance)
	}
	next.Snapshots = make([]SnapshotState, len(state.Snapshots))
	for index, snapshot := range state.Snapshots {
		next.Snapshots[index] = cloneSnapshot(snapshot)
	}
	next.Attempts = slices.Clone(state.Attempts)
	return next
}

func cloneInstance(instance InstanceState) InstanceState {
	next := instance
	next.Effects = slices.Clone(instance.Effects)
	return next
}

func cloneSnapshot(snapshot SnapshotState) SnapshotState {
	next := snapshot
	next.Effects = slices.Clone(snapshot.Effects)
	return next
}

func workspaceHash(effects []string) string {
	digest := sha256.New()
	for _, effect := range effects {
		_, _ = digest.Write([]byte(effect))
		_, _ = digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func receiptID(kind Operation, first, second string) string {
	return hashString(string(kind) + "\x00" + first + "\x00" + second)
}

func hashOptional(value string) string {
	if value == "" {
		return ""
	}
	return hashString(value)
}

func hashString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
