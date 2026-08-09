package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const ProviderType compute.ProviderType = "temporal-durability-lab"

const (
	configDatabasePath   = "database_path"
	configMode           = "mode"
	configBarrierURL     = "barrier_url"
	configFaultOperation = "fault_operation"
	configSessionID      = "session_id"
	configWorkerIdentity = "worker_identity"
	configGeneration     = "generation"
	configCapability     = "capability"
)

type Config struct {
	DatabasePath   string
	Mode           Mode
	BarrierURL     string
	FaultOperation Operation
	SessionID      string
	WorkerIdentity string
	Generation     uint64
	Capability     string
}

func (c Config) ProviderDetails() compute.ProviderDetails {
	config := map[string]string{
		configDatabasePath:   c.DatabasePath,
		configMode:           string(c.Mode),
		configBarrierURL:     c.BarrierURL,
		configFaultOperation: string(c.FaultOperation),
		configSessionID:      c.SessionID,
		configWorkerIdentity: c.WorkerIdentity,
		configCapability:     c.Capability,
	}
	if c.Generation > 0 {
		config[configGeneration] = strconv.FormatUint(c.Generation, 10)
	}
	return compute.ProviderDetails{Type: ProviderType, Config: config}
}

type CommandEnvelope struct {
	LogicalEffectID string `json:"logical_effect_id"`
	Payload         string `json:"payload"`
	Generation      uint64 `json:"generation,omitempty"`
	Capability      string `json:"capability,omitempty"`
}

func EncodeCommand(command CommandEnvelope) (string, error) {
	if command.LogicalEffectID == "" {
		return "", fmt.Errorf("%w: logical effect identity is required", ErrInvalidRequest)
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		return "", fmt.Errorf("encode command envelope: %w", err)
	}
	return string(encoded), nil
}

type controlledProvider struct {
	store          *Store
	mode           Mode
	barrierURL     string
	faultOperation Operation
	sessionID      string
	workerIdentity string
}

func init() {
	compute.Register(ProviderType, newControlledProvider)
}

func newControlledProvider(raw map[string]string) (compute.Provider, error) {
	config, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	store, err := Open(config.DatabasePath)
	if err != nil {
		return nil, err
	}
	if config.Mode == ModeFenced && config.Generation > 0 {
		if err := store.SetAuthority(context.Background(), Authority{
			Generation: config.Generation, Capability: config.Capability,
		}); err != nil {
			return nil, err
		}
	}
	return &controlledProvider{
		store: store, mode: config.Mode, barrierURL: config.BarrierURL,
		faultOperation: config.FaultOperation, sessionID: config.SessionID,
		workerIdentity: config.WorkerIdentity,
	}, nil
}

func parseConfig(raw map[string]string) (Config, error) {
	config := Config{
		DatabasePath: raw[configDatabasePath], Mode: Mode(raw[configMode]),
		BarrierURL: raw[configBarrierURL], FaultOperation: Operation(raw[configFaultOperation]),
		SessionID: raw[configSessionID], WorkerIdentity: raw[configWorkerIdentity],
		Capability: raw[configCapability],
	}
	if encoded := raw[configGeneration]; encoded != "" {
		generation, err := strconv.ParseUint(encoded, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("%w: parse authority generation: %v", ErrInvalidRequest, err)
		}
		config.Generation = generation
	}
	if config.DatabasePath == "" || !config.Mode.Valid() || config.SessionID == "" || config.WorkerIdentity == "" {
		return Config{}, fmt.Errorf("%w: database, mode, session, and Worker identity are required", ErrInvalidRequest)
	}
	if (config.BarrierURL == "") != (config.FaultOperation == "") {
		return Config{}, fmt.Errorf("%w: barrier URL and fault operation must be configured together", ErrInvalidRequest)
	}
	if config.FaultOperation != "" && !config.FaultOperation.Valid() {
		return Config{}, fmt.Errorf("%w: unsupported fault operation", ErrInvalidRequest)
	}
	if config.Mode == ModeFenced && (config.Generation == 0 || config.Capability == "") {
		return Config{}, fmt.Errorf("%w: fenced mode requires authority", ErrInvalidRequest)
	}
	return config, nil
}

func (p *controlledProvider) Start(ctx context.Context, _ string) (*compute.ProviderStatus, error) {
	result, err := p.apply(ctx, Request{Kind: OperationStart})
	if err != nil {
		return nil, err
	}
	return &compute.ProviderStatus{InstanceID: result.InstanceID}, nil
}

func (p *controlledProvider) Stop(ctx context.Context, status *compute.ProviderStatus) error {
	if status == nil {
		return fmt.Errorf("%w: provider status is required", ErrInvalidRequest)
	}
	_, err := p.apply(ctx, Request{Kind: OperationStop, InstanceID: status.InstanceID})
	return err
}

func (*controlledProvider) Suspend(context.Context, *compute.ProviderStatus) error {
	return errors.ErrUnsupported
}

func (*controlledProvider) Resume(context.Context, *compute.ProviderStatus) error {
	return errors.ErrUnsupported
}

func (p *controlledProvider) Snapshot(
	ctx context.Context,
	status *compute.ProviderStatus,
) (compute.SandboxPostSnapshotState, *compute.ProviderSnapshot, error) {
	if status == nil {
		return compute.SandboxPostSnapshotRunning, nil, fmt.Errorf("%w: provider status is required", ErrInvalidRequest)
	}
	result, err := p.apply(ctx, Request{Kind: OperationSnapshot, InstanceID: status.InstanceID})
	if err != nil {
		return compute.SandboxPostSnapshotRunning, nil, err
	}
	return compute.SandboxPostSnapshotRunning, &compute.ProviderSnapshot{SnapshotID: result.SnapshotID}, nil
}

func (p *controlledProvider) StartFromSnapshot(
	ctx context.Context,
	_ string,
	snapshot *compute.ProviderSnapshot,
) (*compute.ProviderStatus, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: provider snapshot is required", ErrInvalidRequest)
	}
	result, err := p.apply(ctx, Request{
		Kind: OperationStartFromSnapshot, SnapshotID: snapshot.SnapshotID,
	})
	if err != nil {
		return nil, err
	}
	return &compute.ProviderStatus{InstanceID: result.InstanceID}, nil
}

func (*controlledProvider) DeleteSnapshot(context.Context, *compute.ProviderSnapshot) error {
	return errors.ErrUnsupported
}

func (p *controlledProvider) ExecuteCommand(
	ctx context.Context,
	status *compute.ProviderStatus,
	encoded string,
) (*compute.CommandResult, error) {
	if status == nil {
		return nil, fmt.Errorf("%w: provider status is required", ErrInvalidRequest)
	}
	command, err := decodeCommand(encoded)
	if err != nil {
		return nil, err
	}
	result, err := p.apply(ctx, Request{
		Kind: OperationCommand, InstanceID: status.InstanceID,
		LogicalEffectID: command.LogicalEffectID, Payload: command.Payload,
		Generation: command.Generation, Capability: command.Capability,
	})
	if errors.Is(err, ErrStaleAuthority) {
		if barrierErr := p.arriveAtDecision(ctx, "provider-command-stale-rejected", command.Generation, command.Capability); barrierErr != nil {
			return nil, barrierErr
		}
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "StaleProviderAuthority", err)
	}
	if err != nil {
		return nil, err
	}
	receipt, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("encode command receipt: %w", err)
	}
	return &compute.CommandResult{Stdout: string(receipt)}, nil
}

func (p *controlledProvider) arriveAtDecision(
	ctx context.Context,
	point string,
	generation uint64,
	capability string,
) error {
	if p.barrierURL == "" {
		return nil
	}
	info := activity.GetInfo(ctx)
	operationID := fmt.Sprintf("%s/%s/%s", info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID)
	physicalAttemptID := fmt.Sprintf(
		"%s/attempt-%d/worker-%s/pid-%d", operationID, info.Attempt, p.workerIdentity, os.Getpid(),
	)
	arrival := failureinject.Arrival{
		ID: physicalAttemptID + "/stale-rejection", Point: point, SessionID: p.sessionID,
		OwnerTokenHash: hashOptional(capability), Generation: generation,
		ActorID: "sandbox-provider", PID: os.Getpid(), ProcessStart: p.workerIdentity,
	}
	if err := failureinject.NewClient(p.barrierURL).Arrive(ctx, arrival); err != nil {
		return fmt.Errorf("provider decision barrier: %w", err)
	}
	return nil
}

func decodeCommand(encoded string) (CommandEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(encoded))
	decoder.DisallowUnknownFields()
	var command CommandEnvelope
	if err := decoder.Decode(&command); err != nil {
		return CommandEnvelope{}, fmt.Errorf("%w: decode command envelope: %v", ErrInvalidRequest, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return CommandEnvelope{}, fmt.Errorf("%w: trailing command data", ErrInvalidRequest)
	}
	if command.LogicalEffectID == "" {
		return CommandEnvelope{}, fmt.Errorf("%w: logical effect identity is required", ErrInvalidRequest)
	}
	return command, nil
}

func (p *controlledProvider) apply(ctx context.Context, request Request) (Result, error) {
	info := activity.GetInfo(ctx)
	operationID := fmt.Sprintf(
		"%s/%s/%s", info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID,
	)
	physicalAttemptID := fmt.Sprintf(
		"%s/attempt-%d/worker-%s/pid-%d", operationID, info.Attempt, p.workerIdentity, os.Getpid(),
	)
	request.OperationID = operationID
	request.PhysicalAttemptID = physicalAttemptID
	request.WorkerIdentity = fmt.Sprintf("%s/pid-%d", p.workerIdentity, os.Getpid())
	request.TemporalAttempt = info.Attempt
	result, err := p.store.Apply(ctx, request)
	if err != nil {
		return Result{}, err
	}
	if request.Kind == p.faultOperation && info.Attempt == 1 {
		arrival := failureinject.Arrival{
			ID: physicalAttemptID, Point: barrierPoint(request.Kind), SessionID: p.sessionID,
			OwnerTokenHash: hashOptional(request.Capability), Generation: request.Generation,
			ActorID: "sandbox-provider", PID: os.Getpid(), ProcessStart: p.workerIdentity,
		}
		if err := failureinject.NewClient(p.barrierURL).Arrive(ctx, arrival); err != nil {
			return Result{}, fmt.Errorf("provider fault barrier: %w", err)
		}
		return Result{}, temporal.NewApplicationError(
			"injected provider effect/completion loss", "InjectedProviderCompletionLoss",
		)
	}
	return result, nil
}

func barrierPoint(operation Operation) string {
	return "provider-" + string(operation) + "-effect-committed"
}
