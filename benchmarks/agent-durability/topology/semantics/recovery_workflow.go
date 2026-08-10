package semantics

import (
	"fmt"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const protectedInFlightMax = 8

const (
	outageCohortGateChangeID   = "topology-outage-cohort-gate-v1"
	outageCohortBudgetChangeID = "topology-outage-cohort-budget-v1"
)

const (
	silentProgressAdmissionChangeID   = "topology-silent-progress-admission-v1"
	silentProgressReplacementChangeID = "topology-silent-progress-replacement-control-lane-v1"
	silentProgressDetectionChangeID   = "topology-silent-progress-detection-delay-v1"
)

type RecoveryWorkInput struct {
	ProtocolVersion      string          `json:"protocol_version"`
	PairID               string          `json:"pair_id"`
	LogicalOperationID   string          `json:"logical_operation_id"`
	WorkTaskQueue        string          `json:"work_task_queue"`
	EffectTaskQueue      string          `json:"effect_task_queue"`
	Case                 protocol.CaseID `json:"case"`
	Boundary             string          `json:"boundary"`
	Probe                protocol.Probe  `json:"probe"`
	Item                 Item            `json:"item"`
	Authority            Authority       `json:"authority"`
	ReplacementAuthority Authority       `json:"replacement_authority"`
	Replacement          bool            `json:"replacement,omitempty"`
	ReleaseWedged        bool            `json:"release_wedged,omitempty"`
	RecoveryRound        int             `json:"recovery_round,omitempty"`
}

type RecoveryWorkResult struct {
	ItemID             string `json:"item_id"`
	Ordinal            int    `json:"ordinal"`
	Disposition        string `json:"disposition"`
	NeedsReplacement   bool   `json:"needs_replacement,omitempty"`
	NeedsRecoveryRetry bool   `json:"needs_recovery_retry,omitempty"`
}

func (r RecoveryWorkResult) validate(input RecoveryWorkInput) error {
	if r.ItemID != input.Item.ID || r.Ordinal != input.Item.Ordinal ||
		!slices.Contains([]string{
			protocol.RecoveryDispositionSucceeded,
			protocol.RecoveryDispositionQuarantined,
			protocol.RecoveryDispositionUnresolved,
		}, r.Disposition) || (r.NeedsReplacement || r.NeedsRecoveryRetry) && r.Disposition != protocol.RecoveryDispositionUnresolved ||
		r.NeedsReplacement && r.NeedsRecoveryRetry || input.Replacement && (r.NeedsReplacement || r.NeedsRecoveryRetry) ||
		input.ReleaseWedged && (r.NeedsReplacement || r.NeedsRecoveryRetry) || input.RecoveryRound < 0 {
		return fmt.Errorf("%w: recovery Work result", protocol.ErrInvalidEvidence)
	}
	return nil
}

type recoveryCompletion struct {
	Input  RecoveryWorkInput
	Result RecoveryWorkResult
	Error  string
}

type RecoveryAdmissionInput struct {
	ProtocolVersion    string          `json:"protocol_version"`
	PairID             string          `json:"pair_id"`
	LogicalOperationID string          `json:"logical_operation_id"`
	EffectTaskQueue    string          `json:"effect_task_queue"`
	Case               protocol.CaseID `json:"case"`
	Probe              protocol.Probe  `json:"probe"`
	BatchOrdinal       int             `json:"batch_ordinal"`
	Items              []Item          `json:"items"`
	Authority          Authority       `json:"authority"`
}

type RecoveryAdmissionReceipt struct {
	BatchOrdinal int `json:"batch_ordinal"`
	Admitted     int `json:"admitted"`
}

func runRecovery(ctx workflow.Context, input ParentInput) (ParentOutput, error) {
	admissionVersion := workflow.Version(1)
	if input.Case == protocol.CaseSilentProgress {
		admissionVersion = workflow.GetVersion(ctx, silentProgressAdmissionChangeID, workflow.DefaultVersion, 1)
	}
	window := recoveryAdmissionWindowForVersion(input.Case, input.Probe, len(input.Items), admissionVersion)
	results := make([]RecoveryWorkResult, 0, len(input.Items))
	for first, batchOrdinal := 0, 1; first < len(input.Items); first, batchOrdinal = first+window, batchOrdinal+1 {
		last := first + window
		if last > len(input.Items) {
			last = len(input.Items)
		}
		batch := slices.Clone(input.Items[first:last])
		if err := admitRecoveryBatch(ctx, input, batchOrdinal, batch); err != nil {
			return ParentOutput{}, err
		}
		completions := workflow.NewBufferedChannel(ctx, len(batch))
		for _, item := range batch {
			workInput := recoveryWorkInput(input, item)
			future := executeRecoveryWork(ctx, input.Topology, workInput)
			sendRecoveryCompletion(ctx, completions, workInput, future)
		}
		for range batch {
			var completion recoveryCompletion
			completions.Receive(ctx, &completion)
			if completion.Error != "" {
				return ParentOutput{}, fmt.Errorf("recovery item %s failed: %s", completion.Input.Item.ID, completion.Error)
			}
			if err := completion.Result.validate(completion.Input); err != nil {
				return ParentOutput{}, err
			}
			results = append(results, completion.Result)
		}
	}
	slices.SortFunc(results, func(first, second RecoveryWorkResult) int { return first.Ordinal - second.Ordinal })
	return ParentOutput{Topology: input.Topology, Case: input.Case, RecoveryResults: results}, nil
}

func recoveryAdmissionWindow(benchmarkCase protocol.CaseID, probe protocol.Probe, fanout int) int {
	controlSensitive := benchmarkCase == protocol.CaseBackpressureOverload || benchmarkCase == protocol.CaseSilentProgress
	if controlSensitive && probe != protocol.ProbeUnsafe && fanout > protectedInFlightMax {
		return protectedInFlightMax
	}
	return fanout
}

func recoveryAdmissionWindowForVersion(
	benchmarkCase protocol.CaseID,
	probe protocol.Probe,
	fanout int,
	version workflow.Version,
) int {
	if benchmarkCase == protocol.CaseSilentProgress && version == workflow.DefaultVersion {
		return fanout
	}
	return recoveryAdmissionWindow(benchmarkCase, probe, fanout)
}

func admitRecoveryBatch(ctx workflow.Context, input ParentInput, batchOrdinal int, items []Item) error {
	activityInput := RecoveryAdmissionInput{
		ProtocolVersion: input.ProtocolVersion, PairID: input.PairID, LogicalOperationID: input.LogicalOperationID,
		EffectTaskQueue: input.EffectTaskQueue, Case: input.Case, Probe: input.Probe,
		BatchOrdinal: batchOrdinal, Items: slices.Clone(items), Authority: input.InitialAuthority,
	}
	options := workflow.ActivityOptions{
		ActivityID: fmt.Sprintf("recovery-admission/batch-%03d", batchOrdinal), TaskQueue: input.EffectTaskQueue,
		ScheduleToCloseTimeout: time.Minute, StartToCloseTimeout: 30 * time.Second, HeartbeatTimeout: 2 * time.Second,
		WaitForCancellation: true,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 50 * time.Millisecond, BackoffCoefficient: 2, MaximumInterval: time.Second, MaximumAttempts: 4},
	}
	var receipt RecoveryAdmissionReceipt
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), RecoveryAdmissionActivityName, activityInput).Get(ctx, &receipt); err != nil {
		return err
	}
	if receipt.BatchOrdinal != batchOrdinal || receipt.Admitted != len(items) {
		return fmt.Errorf("%w: recovery admission receipt", protocol.ErrInvalidEvidence)
	}
	return nil
}

func recoveryWorkInput(input ParentInput, item Item) RecoveryWorkInput {
	return RecoveryWorkInput{
		ProtocolVersion: input.ProtocolVersion, PairID: input.PairID, LogicalOperationID: input.LogicalOperationID,
		WorkTaskQueue: input.WorkTaskQueue, EffectTaskQueue: input.EffectTaskQueue,
		Case: input.Case, Boundary: input.Boundary, Probe: input.Probe,
		Item: item, Authority: input.InitialAuthority, ReplacementAuthority: input.ReplacementAuthority,
	}
}

func sendRecoveryCompletion(
	ctx workflow.Context,
	completions workflow.Channel,
	input RecoveryWorkInput,
	future workflow.Future,
) {
	workflow.Go(ctx, func(childCtx workflow.Context) {
		var result RecoveryWorkResult
		err := future.Get(childCtx, &result)
		completion := recoveryCompletion{Input: input, Result: result}
		if err != nil {
			completion.Error = err.Error()
		}
		completions.Send(childCtx, completion)
	})
}

func RecoveryItemWorkflow(ctx workflow.Context, input RecoveryWorkInput) (RecoveryWorkResult, error) {
	return runRecoveryItemProcedure(ctx, input)
}

func executeRecoveryWork(ctx workflow.Context, topology protocol.Topology, input RecoveryWorkInput) workflow.Future {
	if topology == protocol.TopologyDirectActivity {
		future, settable := workflow.NewFuture(ctx)
		workflow.Go(ctx, func(childCtx workflow.Context) {
			result, err := runRecoveryItemProcedure(childCtx, input)
			settable.Set(result, err)
		})
		return future
	}
	childOptions := workflow.ChildWorkflowOptions{
		WorkflowID:          fmt.Sprintf("%s/child/%s", input.LogicalOperationID, input.Item.ID),
		WaitForCancellation: true,
	}
	return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, childOptions), RecoveryItemWorkflowName, input)
}

func runRecoveryItemProcedure(ctx workflow.Context, input RecoveryWorkInput) (RecoveryWorkResult, error) {
	current := input
	for {
		var result RecoveryWorkResult
		if err := workflow.ExecuteActivity(withRecoveryActivityOptions(ctx, current), RecoveryWorkActivityName, current).Get(ctx, &result); err != nil {
			return RecoveryWorkResult{}, err
		}
		if err := result.validate(current); err != nil {
			return RecoveryWorkResult{}, err
		}
		switch {
		case result.NeedsRecoveryRetry:
			if current.Case == protocol.CaseOutageBacklogHerdRecovery && current.RecoveryRound == 0 {
				version := workflow.GetVersion(ctx, outageCohortGateChangeID, workflow.DefaultVersion, 1)
				if version != workflow.DefaultVersion {
					if err := awaitRecoveryCohort(ctx, current); err != nil {
						return RecoveryWorkResult{}, err
					}
				}
			}
			current.RecoveryRound++
			if current.RecoveryRound > 128 {
				return RecoveryWorkResult{}, temporal.NewNonRetryableApplicationError(
					"recovery retry rounds exhausted", "recovery_round_budget", nil,
				)
			}
			delay := time.Duration(10+(current.Item.Ordinal*7)%17) * time.Millisecond
			if current.Probe == protocol.ProbeUnsafe {
				delay = time.Millisecond
			}
			if err := workflow.NewTimer(ctx, delay).Get(ctx, nil); err != nil {
				return RecoveryWorkResult{}, err
			}
		case result.NeedsReplacement:
			delayVersion := workflow.DefaultVersion
			if current.Case == protocol.CaseSilentProgress && current.Probe == protocol.ProbeProtected {
				delayVersion = workflow.GetVersion(ctx, silentProgressDetectionChangeID, workflow.DefaultVersion, 2)
			}
			delay := progressReplacementDelayForVersion(current.Probe, delayVersion)
			if err := workflow.NewTimer(ctx, delay).Get(ctx, nil); err != nil {
				return RecoveryWorkResult{}, err
			}
			current.Authority = input.ReplacementAuthority
			if current.Probe == protocol.ProbeUnsafe {
				current.ReleaseWedged = true
			} else {
				current.Replacement = true
			}
		default:
			return result, nil
		}
	}
}

func awaitRecoveryCohort(ctx workflow.Context, input RecoveryWorkInput) error {
	version := workflow.GetVersion(ctx, outageCohortBudgetChangeID, workflow.DefaultVersion, 1)
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, recoveryCohortActivityOptions(input, version)), RecoveryCohortActivityName, input,
	).Get(ctx, nil)
}

func recoveryCohortActivityOptions(input RecoveryWorkInput, version workflow.Version) workflow.ActivityOptions {
	scheduleToClose := 2 * time.Minute
	startToClose := time.Minute
	heartbeat := 2 * time.Second
	if version != workflow.DefaultVersion {
		scheduleToClose = 4 * time.Minute
		startToClose = 3 * time.Minute
		heartbeat = 10 * time.Second
	}
	return workflow.ActivityOptions{
		ActivityID:             fmt.Sprintf("recovery-cohort/%s", input.Item.ID),
		TaskQueue:              input.EffectTaskQueue,
		ScheduleToCloseTimeout: scheduleToClose,
		StartToCloseTimeout:    startToClose,
		HeartbeatTimeout:       heartbeat,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 50 * time.Millisecond, BackoffCoefficient: 2,
			MaximumInterval: time.Second, MaximumAttempts: 4,
		},
	}
}

func progressReplacementDelayForVersion(probe protocol.Probe, version workflow.Version) time.Duration {
	if probe == protocol.ProbeUnsafe {
		return 5200 * time.Millisecond
	}
	if version >= 2 {
		return time.Second
	}
	if version == 1 {
		return 2 * time.Second
	}
	// Leave enough of the registered five-second bound for the replacement
	// Activity to be dispatched and durably record the revocation under load.
	return 3 * time.Second
}

func withRecoveryActivityOptions(ctx workflow.Context, input RecoveryWorkInput) workflow.Context {
	maximumAttempts := int32(4)
	if input.Case == protocol.CasePoisonWorkIsolation {
		maximumAttempts = 3
		if input.Probe == protocol.ProbeUnsafe {
			maximumAttempts = 5
		}
	}
	routingVersion := workflow.Version(1)
	if input.Case == protocol.CaseSilentProgress && input.Probe == protocol.ProbeProtected && input.Replacement {
		routingVersion = workflow.GetVersion(ctx, silentProgressReplacementChangeID, workflow.DefaultVersion, 1)
	}
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             fmt.Sprintf("work/%s/generation-%d%s", input.Item.ID, input.Authority.Generation, recoveryPhaseSuffix(input)),
		TaskQueue:              recoveryActivityTaskQueue(input, routingVersion),
		ScheduleToCloseTimeout: 2 * time.Minute,
		StartToCloseTimeout:    30 * time.Second,
		HeartbeatTimeout:       2 * time.Second,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 50 * time.Millisecond, BackoffCoefficient: 2,
			MaximumInterval: time.Second, MaximumAttempts: maximumAttempts,
		},
	})
}

func recoveryActivityTaskQueue(input RecoveryWorkInput, version workflow.Version) string {
	if version != workflow.DefaultVersion && input.Case == protocol.CaseSilentProgress &&
		input.Probe == protocol.ProbeProtected && input.Replacement {
		return input.EffectTaskQueue
	}
	return input.WorkTaskQueue
}

func recoveryPhaseSuffix(input RecoveryWorkInput) string {
	if input.Replacement {
		return "/replacement"
	}
	if input.ReleaseWedged {
		return "/stale-release"
	}
	if input.RecoveryRound > 0 {
		return fmt.Sprintf("/recovery-%03d", input.RecoveryRound)
	}
	return ""
}
