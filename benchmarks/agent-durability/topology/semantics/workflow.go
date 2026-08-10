// Package semantics defines the topology-neutral Workflow procedure for the
// orchestration-semantics benchmark cases.
package semantics

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ParentWorkflowName            = "TopologySemanticsParentV1"
	ItemWorkflowName              = "TopologySemanticsItemV1"
	WorkActivityName              = "TopologyHermeticAgentWorkV1"
	RecoveryItemWorkflowName      = "TopologyRecoveryItemV1"
	RecoveryWorkActivityName      = "TopologyRecoveryAgentWorkV1"
	RecoveryAdmissionActivityName = "TopologyRecoveryAdmissionV1"
	RecoveryCohortActivityName    = "TopologyRecoveryCohortGateV1"
	CheckpointActivityName        = "TopologyReductionCheckpointV1"
	ContinuationActivityName      = "TopologyContinuationV1"
	SupersedeActivityName         = "TopologySupersedeAuthorityV1"
	CancellationActivityName      = "TopologyObserveSupersessionCancellationV1"
	DestructiveActivityName       = "TopologyDestructiveTransitionV1"
)

const (
	supersessionControlLaneChangeID = "topology-supersession-control-lane-v1"
	supersessionWorkTimeoutChangeID = "topology-supersession-work-timeout-v1"
)

type Item struct {
	ID      string `json:"id"`
	Ordinal int    `json:"ordinal"`
}

type ParentInput struct {
	ProtocolVersion      string            `json:"protocol_version"`
	PairID               string            `json:"pair_id"`
	LogicalOperationID   string            `json:"logical_operation_id"`
	WorkTaskQueue        string            `json:"work_task_queue"`
	EffectTaskQueue      string            `json:"effect_task_queue"`
	Topology             protocol.Topology `json:"topology"`
	Case                 protocol.CaseID   `json:"case"`
	Boundary             string            `json:"boundary"`
	Probe                protocol.Probe    `json:"probe"`
	Items                []Item            `json:"items"`
	InitialAuthority     Authority         `json:"initial_authority"`
	ReplacementAuthority Authority         `json:"replacement_authority"`
}

func (i ParentInput) Validate() error {
	if i.ProtocolVersion != protocol.PublicationProtocolVersion || i.PairID == "" || i.LogicalOperationID == "" ||
		i.WorkTaskQueue == "" || i.EffectTaskQueue == "" ||
		!i.Topology.Valid() || !i.Case.Valid() || !i.Probe.Valid() || i.Boundary == "" ||
		!slices.Contains([]int{8, 32, 128}, len(i.Items)) {
		return fmt.Errorf("%w: semantics Workflow input", protocol.ErrInvalidEvidence)
	}
	if err := i.InitialAuthority.validate(); err != nil {
		return err
	}
	if err := i.ReplacementAuthority.validate(); err != nil {
		return err
	}
	if i.ReplacementAuthority.Generation <= i.InitialAuthority.Generation ||
		i.ReplacementAuthority.CapabilityHash == i.InitialAuthority.CapabilityHash {
		return fmt.Errorf("%w: replacement authority", protocol.ErrInvalidEvidence)
	}
	seen := make(map[string]bool, len(i.Items))
	for index, item := range i.Items {
		if item.ID == "" || item.Ordinal != index+1 || seen[item.ID] {
			return fmt.Errorf("%w: immutable item ledger", protocol.ErrInvalidEvidence)
		}
		seen[item.ID] = true
	}
	return nil
}

type WorkInput struct {
	ProtocolVersion    string          `json:"protocol_version"`
	PairID             string          `json:"pair_id"`
	LogicalOperationID string          `json:"logical_operation_id"`
	WorkTaskQueue      string          `json:"work_task_queue"`
	Case               protocol.CaseID `json:"case"`
	Boundary           string          `json:"boundary"`
	Probe              protocol.Probe  `json:"probe"`
	Item               Item            `json:"item"`
	Authority          Authority       `json:"authority"`
	Replacement        bool            `json:"replacement"`
}

type Contribution struct {
	ItemID  string `json:"item_id"`
	Ordinal int    `json:"ordinal"`
	Attempt int    `json:"attempt"`
}

type WorkResult struct {
	ItemID     string         `json:"item_id"`
	Ordinal    int            `json:"ordinal"`
	Deliveries []Contribution `json:"deliveries"`
}

func (r WorkResult) validate(input WorkInput) error {
	if r.ItemID != input.Item.ID || r.Ordinal != input.Item.Ordinal || len(r.Deliveries) == 0 {
		return fmt.Errorf("%w: work result identity", protocol.ErrInvalidEvidence)
	}
	for _, delivery := range r.Deliveries {
		if delivery.ItemID != r.ItemID || delivery.Ordinal != r.Ordinal || delivery.Attempt < 1 {
			return fmt.Errorf("%w: physical contribution identity", protocol.ErrInvalidEvidence)
		}
	}
	return nil
}

type CheckpointInput struct {
	ProtocolVersion    string          `json:"protocol_version"`
	PairID             string          `json:"pair_id"`
	LogicalOperationID string          `json:"logical_operation_id"`
	Case               protocol.CaseID `json:"case"`
	Probe              protocol.Probe  `json:"probe"`
	CheckpointID       string          `json:"checkpoint_id"`
	Cardinality        int             `json:"cardinality"`
	Members            []string        `json:"members"`
	Value              int64           `json:"value"`
	StableIdentity     bool            `json:"stable_identity"`
}

type CheckpointReceipt struct {
	CheckpointID string `json:"checkpoint_id"`
	ReceiptID    string `json:"receipt_id"`
}

type ContinuationInput struct {
	ProtocolVersion    string          `json:"protocol_version"`
	PairID             string          `json:"pair_id"`
	LogicalOperationID string          `json:"logical_operation_id"`
	Case               protocol.CaseID `json:"case"`
	Probe              protocol.Probe  `json:"probe"`
	ContinuationID     string          `json:"continuation_id"`
	Members            []string        `json:"members"`
	Value              int64           `json:"value"`
}

type ContinuationReceipt struct {
	ContinuationID string `json:"continuation_id"`
	ReceiptID      string `json:"receipt_id"`
}

type SupersedeInput struct {
	ProtocolVersion string         `json:"protocol_version"`
	PairID          string         `json:"pair_id"`
	OperationID     string         `json:"operation_id"`
	Boundary        string         `json:"boundary"`
	Probe           protocol.Probe `json:"probe"`
	ItemID          string         `json:"item_id"`
	Obsolete        Authority      `json:"obsolete"`
	Replacement     Authority      `json:"replacement"`
}

type SupersedeReceipt struct {
	ItemID     string `json:"item_id"`
	Generation uint64 `json:"generation"`
}

type CancellationInput struct {
	ProtocolVersion string         `json:"protocol_version"`
	PairID          string         `json:"pair_id"`
	OperationID     string         `json:"operation_id"`
	Boundary        string         `json:"boundary"`
	Probe           protocol.Probe `json:"probe"`
	ItemID          string         `json:"item_id"`
	Requested       bool           `json:"requested"`
	Obsolete        Authority      `json:"obsolete"`
	Replacement     Authority      `json:"replacement"`
}

type CancellationReceipt struct {
	ItemID      string `json:"item_id"`
	Disposition string `json:"disposition"`
}

type DestructiveActivityInput struct {
	ProtocolVersion string         `json:"protocol_version"`
	PairID          string         `json:"pair_id"`
	OperationID     string         `json:"operation_id"`
	Boundary        string         `json:"boundary"`
	Probe           protocol.Probe `json:"probe"`
	ItemID          string         `json:"item_id"`
	Authority       Authority      `json:"authority"`
	ExpectedVersion uint64         `json:"expected_version"`
}

type ParentOutput struct {
	Topology        protocol.Topology    `json:"topology"`
	Case            protocol.CaseID      `json:"case"`
	Results         []WorkResult         `json:"results"`
	Checkpoints     []CheckpointReceipt  `json:"checkpoints,omitempty"`
	Continuation    ContinuationReceipt  `json:"continuation"`
	Supersession    SupersedeReceipt     `json:"supersession,omitempty"`
	Destructive     DestructiveResult    `json:"destructive,omitempty"`
	ReductionValue  int64                `json:"reduction_value,omitempty"`
	RecoveryResults []RecoveryWorkResult `json:"recovery_results,omitempty"`
}

type workCompletion struct {
	Input  WorkInput  `json:"input"`
	Result WorkResult `json:"result"`
	Error  string     `json:"error,omitempty"`
}

func ParentWorkflow(ctx workflow.Context, input ParentInput) (ParentOutput, error) {
	if err := input.Validate(); err != nil {
		return ParentOutput{}, err
	}
	if input.Case.Suite() == protocol.SuiteRecoveryDynamics {
		return runRecovery(ctx, input)
	}
	if input.Case == protocol.CaseQueuedExecutingSupersession {
		return runSupersession(ctx, input)
	}
	completions := workflow.NewBufferedChannel(ctx, len(input.Items))
	for _, item := range input.Items {
		workInput := parentWorkInput(input, item, input.InitialAuthority, false)
		future := executeWork(ctx, input.Topology, workInput)
		sendWorkCompletion(ctx, completions, workInput, future)
	}
	if input.Case == protocol.CaseJoinBarrier {
		return runJoin(ctx, input, completions)
	}
	if input.Case == protocol.CaseIncrementalPartialReduction {
		return runReduction(ctx, input, completions)
	}
	return runDestructive(ctx, input, completions)
}

func parentWorkInput(input ParentInput, item Item, authority Authority, replacement bool) WorkInput {
	return WorkInput{
		ProtocolVersion: input.ProtocolVersion, PairID: input.PairID, LogicalOperationID: input.LogicalOperationID,
		WorkTaskQueue: input.WorkTaskQueue,
		Case:          input.Case, Boundary: input.Boundary, Probe: input.Probe, Item: item, Authority: authority, Replacement: replacement,
	}
}

func sendWorkCompletion(ctx workflow.Context, completions workflow.Channel, input WorkInput, future workflow.Future) {
	workflow.Go(ctx, func(childCtx workflow.Context) {
		var result WorkResult
		err := future.Get(childCtx, &result)
		completion := workCompletion{Input: input, Result: result}
		if err != nil {
			completion.Error = err.Error()
		}
		completions.Send(childCtx, completion)
	})
}

func ItemWorkflow(ctx workflow.Context, input WorkInput) (WorkResult, error) {
	ctx = withWorkActivityOptions(ctx, input)
	var result WorkResult
	if err := workflow.ExecuteActivity(ctx, WorkActivityName, input).Get(ctx, &result); err != nil {
		return WorkResult{}, err
	}
	if err := result.validate(input); err != nil {
		return WorkResult{}, err
	}
	return result, nil
}

func executeWork(ctx workflow.Context, topology protocol.Topology, input WorkInput) workflow.Future {
	if topology == protocol.TopologyDirectActivity {
		return workflow.ExecuteActivity(withWorkActivityOptions(ctx, input), WorkActivityName, input)
	}
	childOptions := workflow.ChildWorkflowOptions{
		WorkflowID:          fmt.Sprintf("%s/child/%s/generation-%d", input.LogicalOperationID, input.Item.ID, input.Authority.Generation),
		WaitForCancellation: true,
	}
	return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, childOptions), ItemWorkflowName, input)
}

func withWorkActivityOptions(ctx workflow.Context, input WorkInput) workflow.Context {
	timeoutVersion := workflow.DefaultVersion
	if heldSupersessionWork(input) {
		timeoutVersion = workflow.GetVersion(ctx, supersessionWorkTimeoutChangeID, workflow.DefaultVersion, 1)
	}
	scheduleToClose, startToClose := workActivityTimeouts(input, timeoutVersion)
	options := workflow.ActivityOptions{
		ActivityID:             fmt.Sprintf("work/%s/generation-%d", input.Item.ID, input.Authority.Generation),
		TaskQueue:              input.WorkTaskQueue,
		ScheduleToCloseTimeout: scheduleToClose,
		StartToCloseTimeout:    startToClose,
		HeartbeatTimeout:       10 * time.Second,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 50 * time.Millisecond, BackoffCoefficient: 2, MaximumInterval: time.Second, MaximumAttempts: 4,
		},
	}
	return workflow.WithActivityOptions(ctx, options)
}

func heldSupersessionWork(input WorkInput) bool {
	return input.Case == protocol.CaseQueuedExecutingSupersession && input.Item.ID == "item-001" && !input.Replacement &&
		(input.Boundary == "executing-after-process-start-before-effect" || input.Probe == protocol.ProbeUnfaulted)
}

func workActivityTimeouts(input WorkInput, version workflow.Version) (time.Duration, time.Duration) {
	if version != workflow.DefaultVersion && heldSupersessionWork(input) {
		return 3 * time.Minute, 2 * time.Minute
	}
	return time.Minute, 30 * time.Second
}

func withEffectActivityOptions(ctx workflow.Context, activityID, taskQueue string) workflow.Context {
	options := workflow.ActivityOptions{
		ActivityID: activityID, TaskQueue: taskQueue, ScheduleToCloseTimeout: time.Minute, StartToCloseTimeout: 30 * time.Second,
		HeartbeatTimeout: 10 * time.Second, WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 50 * time.Millisecond, BackoffCoefficient: 2, MaximumInterval: time.Second, MaximumAttempts: 4,
		},
	}
	return workflow.WithActivityOptions(ctx, options)
}

func runJoin(ctx workflow.Context, input ParentInput, completions workflow.Channel) (ParentOutput, error) {
	accumulator := newJoinAccumulator(len(input.Items), input.Probe != protocol.ProbeUnsafe)
	results := make([]WorkResult, 0, len(input.Items))
	unsafeTerminalFailure := input.Probe == protocol.ProbeUnsafe && input.Boundary == "required-item-terminal-failure-before-join"
	for range input.Items {
		var completion workCompletion
		completions.Receive(ctx, &completion)
		if completion.Error != "" {
			if unsafeTerminalFailure {
				continue
			}
			return ParentOutput{}, fmt.Errorf("required item %s failed: %s", completion.Input.Item.ID, completion.Error)
		}
		if err := completion.Result.validate(completion.Input); err != nil {
			return ParentOutput{}, err
		}
		results = append(results, completion.Result)
		ready, err := accumulator.accept(completion.Result)
		if err != nil {
			return ParentOutput{}, err
		}
		if ready {
			continuation, err := executeContinuation(ctx, input, accumulator.members(), 0)
			if err != nil {
				return ParentOutput{}, err
			}
			sortResults(results)
			return ParentOutput{Topology: input.Topology, Case: input.Case, Results: results, Continuation: continuation}, nil
		}
	}
	if unsafeTerminalFailure {
		continuation, err := executeContinuation(ctx, input, accumulator.members(), 0)
		if err != nil {
			return ParentOutput{}, err
		}
		sortResults(results)
		return ParentOutput{Topology: input.Topology, Case: input.Case, Results: results, Continuation: continuation}, nil
	}
	return ParentOutput{}, fmt.Errorf("%w: join did not reach immutable membership", protocol.ErrInvalidEvidence)
}

func runReduction(ctx workflow.Context, input ParentInput, completions workflow.Channel) (ParentOutput, error) {
	accumulator := newReductionAccumulator(input.Items, input.Probe != protocol.ProbeUnsafe)
	results := make([]WorkResult, 0, len(input.Items))
	receipts := make([]CheckpointReceipt, 0, 5)
	for range input.Items {
		var completion workCompletion
		completions.Receive(ctx, &completion)
		if completion.Error != "" {
			return ParentOutput{}, fmt.Errorf("required contribution %s failed: %s", completion.Input.Item.ID, completion.Error)
		}
		if err := completion.Result.validate(completion.Input); err != nil {
			return ParentOutput{}, err
		}
		results = append(results, completion.Result)
		checkpoints, err := accumulator.accept(completion.Result)
		if err != nil {
			return ParentOutput{}, err
		}
		for _, checkpoint := range checkpoints {
			var receipt CheckpointReceipt
			checkpoint.ProtocolVersion, checkpoint.PairID = input.ProtocolVersion, input.PairID
			checkpoint.LogicalOperationID, checkpoint.Case, checkpoint.Probe = input.LogicalOperationID, input.Case, input.Probe
			checkpoint.CheckpointID = checkpointID(input, checkpoint.Cardinality)
			checkpoint.StableIdentity = input.Probe != protocol.ProbeUnsafe
			activityID := "checkpoint/" + fmt.Sprintf("%03d", checkpoint.Cardinality)
			if err := workflow.ExecuteActivity(withEffectActivityOptions(ctx, activityID, input.EffectTaskQueue), CheckpointActivityName, checkpoint).Get(ctx, &receipt); err != nil {
				return ParentOutput{}, err
			}
			receipts = append(receipts, receipt)
			if checkpoint.Cardinality == len(input.Items) {
				continuation, err := executeContinuation(ctx, input, checkpoint.Members, checkpoint.Value)
				if err != nil {
					return ParentOutput{}, err
				}
				sortResults(results)
				return ParentOutput{
					Topology: input.Topology, Case: input.Case, Results: results, Checkpoints: receipts,
					Continuation: continuation, ReductionValue: checkpoint.Value,
				}, nil
			}
		}
	}
	return ParentOutput{}, fmt.Errorf("%w: reduction did not reach final threshold", protocol.ErrInvalidEvidence)
}

func runSupersession(ctx workflow.Context, input ParentInput) (ParentOutput, error) {
	designated := input.Items[0]
	obsoleteInput := parentWorkInput(input, designated, input.InitialAuthority, false)
	routingVersion := workflow.GetVersion(ctx, supersessionControlLaneChangeID, workflow.DefaultVersion, 2)
	obsoleteInput.WorkTaskQueue = supersessionObsoleteTaskQueue(input, routingVersion)
	obsoleteCtx, cancelObsolete := workflow.WithCancel(ctx)
	_ = executeWork(obsoleteCtx, input.Topology, obsoleteInput)

	supersedeInput := SupersedeInput{
		ProtocolVersion: input.ProtocolVersion, PairID: input.PairID, OperationID: input.LogicalOperationID,
		Boundary: input.Boundary, Probe: input.Probe, ItemID: designated.ID,
		Obsolete: input.InitialAuthority, Replacement: input.ReplacementAuthority,
	}
	var supersedeFuture workflow.Future
	if supersessionSchedulesControlFirst(routingVersion) {
		supersedeFuture = workflow.ExecuteActivity(
			withEffectActivityOptions(ctx, "supersede/"+designated.ID, input.EffectTaskQueue), SupersedeActivityName, supersedeInput,
		)
	}
	completions := workflow.NewBufferedChannel(ctx, len(input.Items))
	for _, item := range input.Items[1:] {
		workInput := parentWorkInput(input, item, input.InitialAuthority, false)
		sendWorkCompletion(ctx, completions, workInput, executeWork(ctx, input.Topology, workInput))
	}
	if supersedeFuture == nil {
		supersedeFuture = workflow.ExecuteActivity(
			withEffectActivityOptions(ctx, "supersede/"+designated.ID, input.EffectTaskQueue), SupersedeActivityName, supersedeInput,
		)
	}
	var supersession SupersedeReceipt
	if err := supersedeFuture.Get(ctx, &supersession); err != nil {
		return ParentOutput{}, err
	}
	if supersession.ItemID != designated.ID || supersession.Generation != input.ReplacementAuthority.Generation {
		return ParentOutput{}, fmt.Errorf("%w: supersession receipt", protocol.ErrInvalidEvidence)
	}
	if input.Probe != protocol.ProbeUnsafe {
		cancelObsolete()
	}
	cancellationInput := CancellationInput{
		ProtocolVersion: input.ProtocolVersion, PairID: input.PairID, OperationID: input.LogicalOperationID,
		Boundary: input.Boundary, Probe: input.Probe, ItemID: designated.ID, Requested: input.Probe != protocol.ProbeUnsafe,
		Obsolete: input.InitialAuthority, Replacement: input.ReplacementAuthority,
	}
	var cancellation CancellationReceipt
	if err := workflow.ExecuteActivity(
		withEffectActivityOptions(ctx, "observe-cancellation/"+designated.ID, input.EffectTaskQueue),
		CancellationActivityName, cancellationInput,
	).Get(ctx, &cancellation); err != nil {
		return ParentOutput{}, err
	}
	if cancellation.ItemID != designated.ID || cancellation.Disposition == "" {
		return ParentOutput{}, fmt.Errorf("%w: cancellation disposition", protocol.ErrInvalidEvidence)
	}
	replacementInput := parentWorkInput(input, designated, input.ReplacementAuthority, true)
	sendWorkCompletion(ctx, completions, replacementInput, executeWork(ctx, input.Topology, replacementInput))

	accumulator := newJoinAccumulator(len(input.Items), true)
	results := make([]WorkResult, 0, len(input.Items))
	for range input.Items {
		var completion workCompletion
		completions.Receive(ctx, &completion)
		if completion.Error != "" {
			return ParentOutput{}, fmt.Errorf("replacement or healthy item %s failed: %s", completion.Input.Item.ID, completion.Error)
		}
		if err := completion.Result.validate(completion.Input); err != nil {
			return ParentOutput{}, err
		}
		results = append(results, completion.Result)
		if _, err := accumulator.accept(completion.Result); err != nil {
			return ParentOutput{}, err
		}
	}
	if len(accumulator.members()) != len(input.Items) {
		return ParentOutput{}, fmt.Errorf("%w: replacement membership", protocol.ErrInvalidEvidence)
	}
	continuation, err := executeContinuation(ctx, input, accumulator.members(), 0)
	if err != nil {
		return ParentOutput{}, err
	}
	sortResults(results)
	return ParentOutput{
		Topology: input.Topology, Case: input.Case, Results: results, Continuation: continuation, Supersession: supersession,
	}, nil
}

func supersessionSchedulesControlFirst(version workflow.Version) bool {
	return version >= 2
}

func supersessionObsoleteTaskQueue(input ParentInput, version workflow.Version) string {
	if version != workflow.DefaultVersion &&
		(input.Boundary == "executing-after-process-start-before-effect" || input.Probe == protocol.ProbeUnfaulted) {
		return input.EffectTaskQueue
	}
	return input.WorkTaskQueue
}

func runDestructive(ctx workflow.Context, input ParentInput, completions workflow.Channel) (ParentOutput, error) {
	accumulator := newJoinAccumulator(len(input.Items), true)
	results := make([]WorkResult, 0, len(input.Items))
	for range input.Items {
		var completion workCompletion
		completions.Receive(ctx, &completion)
		if completion.Error != "" {
			return ParentOutput{}, fmt.Errorf("destructive prerequisite %s failed: %s", completion.Input.Item.ID, completion.Error)
		}
		if err := completion.Result.validate(completion.Input); err != nil {
			return ParentOutput{}, err
		}
		results = append(results, completion.Result)
		if _, err := accumulator.accept(completion.Result); err != nil {
			return ParentOutput{}, err
		}
	}
	destructiveInput := DestructiveActivityInput{
		ProtocolVersion: input.ProtocolVersion, PairID: input.PairID, OperationID: input.LogicalOperationID + "/destructive",
		Boundary: input.Boundary, Probe: input.Probe, ItemID: input.Items[0].ID, Authority: input.InitialAuthority,
	}
	var destructive DestructiveResult
	if err := workflow.ExecuteActivity(
		withEffectActivityOptions(ctx, "destructive/final", input.EffectTaskQueue), DestructiveActivityName, destructiveInput,
	).Get(ctx, &destructive); err != nil {
		return ParentOutput{}, err
	}
	if destructive.OperationID != destructiveInput.OperationID || destructive.ReceiptID == "" ||
		(destructive.Decision != protocol.DecisionAccepted && destructive.Decision != protocol.DecisionReconciled) {
		return ParentOutput{}, fmt.Errorf("%w: destructive receipt", protocol.ErrInvalidEvidence)
	}
	continuation, err := executeContinuation(ctx, input, accumulator.members(), int64(destructive.ResultingVersion))
	if err != nil {
		return ParentOutput{}, err
	}
	sortResults(results)
	return ParentOutput{
		Topology: input.Topology, Case: input.Case, Results: results, Continuation: continuation, Destructive: destructive,
	}, nil
}

func executeContinuation(ctx workflow.Context, input ParentInput, members []string, value int64) (ContinuationReceipt, error) {
	continuationInput := ContinuationInput{
		ProtocolVersion: input.ProtocolVersion, PairID: input.PairID, LogicalOperationID: input.LogicalOperationID,
		Case: input.Case, Probe: input.Probe, ContinuationID: input.LogicalOperationID + "/continuation",
		Members: slices.Clone(members), Value: value,
	}
	var receipt ContinuationReceipt
	ctx = withEffectActivityOptions(ctx, "continuation/final", input.EffectTaskQueue)
	if err := workflow.ExecuteActivity(ctx, ContinuationActivityName, continuationInput).Get(ctx, &receipt); err != nil {
		return ContinuationReceipt{}, err
	}
	return receipt, nil
}

func checkpointID(input ParentInput, cardinality int) string {
	if input.Probe == protocol.ProbeUnsafe {
		return fmt.Sprintf("%s/checkpoint/physical-%03d", input.LogicalOperationID, cardinality)
	}
	return fmt.Sprintf("%s/checkpoint/%03d", input.LogicalOperationID, cardinality)
}

func sortResults(results []WorkResult) {
	slices.SortFunc(results, func(first, second WorkResult) int { return first.Ordinal - second.Ordinal })
}

type joinAccumulator struct {
	required  int
	protected bool
	count     int
	accepted  map[string]bool
}

func newJoinAccumulator(required int, protected bool) *joinAccumulator {
	return &joinAccumulator{required: required, protected: protected, accepted: make(map[string]bool, required)}
}

func (a *joinAccumulator) accept(result WorkResult) (bool, error) {
	if result.ItemID == "" || result.Ordinal < 1 || len(result.Deliveries) == 0 {
		return false, fmt.Errorf("%w: join result", protocol.ErrInvalidEvidence)
	}
	if a.protected {
		if !a.accepted[result.ItemID] {
			a.accepted[result.ItemID] = true
			a.count++
		}
	} else {
		a.accepted[result.ItemID] = true
		a.count += len(result.Deliveries)
	}
	return a.count >= a.required, nil
}

func (a *joinAccumulator) members() []string {
	members := make([]string, 0, len(a.accepted))
	for item := range a.accepted {
		members = append(members, item)
	}
	slices.Sort(members)
	return members
}

type reductionAccumulator struct {
	protected  bool
	required   int
	ordinals   map[string]int
	accepted   map[string]bool
	members    []string
	value      int64
	thresholds map[int]bool
}

func newReductionAccumulator(items []Item, protected bool) *reductionAccumulator {
	ordinals := make(map[string]int, len(items))
	for _, item := range items {
		ordinals[item.ID] = item.Ordinal
	}
	thresholds := make(map[int]bool, 5)
	for _, threshold := range reductionThresholds(len(items)) {
		thresholds[threshold] = true
	}
	return &reductionAccumulator{
		protected: protected, required: len(items), ordinals: ordinals, accepted: make(map[string]bool, len(items)), thresholds: thresholds,
	}
}

func (a *reductionAccumulator) accept(result WorkResult) ([]CheckpointInput, error) {
	wantOrdinal, known := a.ordinals[result.ItemID]
	if !known || wantOrdinal != result.Ordinal {
		return nil, fmt.Errorf("%w: reduction item", protocol.ErrInvalidEvidence)
	}
	var checkpoints []CheckpointInput
	for _, delivery := range result.Deliveries {
		if delivery.ItemID != result.ItemID || delivery.Ordinal != wantOrdinal || delivery.Attempt < 1 {
			return nil, fmt.Errorf("%w: reduction contribution", protocol.ErrInvalidEvidence)
		}
		if a.protected && a.accepted[delivery.ItemID] {
			continue
		}
		a.accepted[delivery.ItemID] = true
		a.members = append(a.members, delivery.ItemID)
		ordinal := int64(delivery.Ordinal)
		if ordinal > 0 && a.value > math.MaxInt64-ordinal {
			return nil, fmt.Errorf("%w: reduction integer overflow", protocol.ErrInvalidEvidence)
		}
		a.value += ordinal
		cardinality := len(a.members)
		if a.thresholds[cardinality] {
			members := slices.Clone(a.members)
			slices.Sort(members)
			checkpoints = append(checkpoints, CheckpointInput{Cardinality: cardinality, Members: members, Value: a.value})
		}
		if cardinality >= a.required {
			break
		}
	}
	return checkpoints, nil
}

func reductionThresholds(count int) []int {
	values := []int{1, ceilingDivision(count, 4), ceilingDivision(count, 2), ceilingDivision(3*count, 4), count}
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func ceilingDivision(numerator, denominator int) int {
	return (numerator + denominator - 1) / denominator
}
