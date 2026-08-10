package semantics

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSupersessionObsoleteTaskQueueVersionsTheControlLane(t *testing.T) {
	base := ParentInput{
		WorkTaskQueue: "bulk-work", EffectTaskQueue: "control-effects",
		Case: protocol.CaseQueuedExecutingSupersession, Boundary: "executing-after-process-start-before-effect",
		Probe: protocol.ProbeProtected,
	}
	tests := []struct {
		name    string
		input   ParentInput
		version workflow.Version
		want    string
	}{
		{name: "new executing protected", input: base, version: 1, want: base.EffectTaskQueue},
		{name: "old history", input: base, version: workflow.DefaultVersion, want: base.WorkTaskQueue},
		{name: "queued boundary", input: func() ParentInput { value := base; value.Boundary = "queued-before-activity-start"; return value }(), version: 1, want: base.WorkTaskQueue},
		{name: "unfaulted baseline", input: func() ParentInput {
			value := base
			value.Boundary = "unfaulted-baseline"
			value.Probe = protocol.ProbeUnfaulted
			return value
		}(), version: 1, want: base.EffectTaskQueue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := supersessionObsoleteTaskQueue(test.input, test.version); got != test.want {
				t.Fatalf("supersessionObsoleteTaskQueue() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSupersessionVersionSchedulesControlBeforeHealthyCohort(t *testing.T) {
	if supersessionSchedulesControlFirst(workflow.DefaultVersion) {
		t.Fatal("old histories must preserve the original supersession command order")
	}
	if supersessionSchedulesControlFirst(1) {
		t.Fatal("v1 histories must preserve the original supersession command order")
	}
	if !supersessionSchedulesControlFirst(2) {
		t.Fatal("new histories must schedule supersession control before the healthy cohort")
	}
}

func TestSupersessionWorkTimeoutIsVersionedForHeldBoundary(t *testing.T) {
	input := WorkInput{
		Case: protocol.CaseQueuedExecutingSupersession, Boundary: "executing-after-process-start-before-effect",
		Probe: protocol.ProbeProtected, Item: Item{ID: "item-001", Ordinal: 1},
	}
	oldSchedule, oldStart := workActivityTimeouts(input, workflow.DefaultVersion)
	if oldSchedule != time.Minute || oldStart != 30*time.Second {
		t.Fatalf("old-history timeouts = %s/%s", oldSchedule, oldStart)
	}
	newSchedule, newStart := workActivityTimeouts(input, 1)
	if newSchedule != 3*time.Minute || newStart != 2*time.Minute {
		t.Fatalf("new held-boundary timeouts = %s/%s, want 3m/2m", newSchedule, newStart)
	}
	healthy := input
	healthy.Item = Item{ID: "item-002", Ordinal: 2}
	if schedule, start := workActivityTimeouts(healthy, 1); schedule != time.Minute || start != 30*time.Second {
		t.Fatalf("healthy timeouts changed = %s/%s", schedule, start)
	}
}

func TestRecoveryCohortGateBudgetIsVersionedForWorstScaleWait(t *testing.T) {
	input := RecoveryWorkInput{Item: Item{ID: "item-001"}, EffectTaskQueue: "effects"}
	old := recoveryCohortActivityOptions(input, workflow.DefaultVersion)
	if old.ScheduleToCloseTimeout != 2*time.Minute || old.StartToCloseTimeout != time.Minute || old.HeartbeatTimeout != 2*time.Second {
		t.Fatalf("old-history cohort timeouts = %s/%s/%s", old.ScheduleToCloseTimeout, old.StartToCloseTimeout, old.HeartbeatTimeout)
	}
	current := recoveryCohortActivityOptions(input, 1)
	if current.ScheduleToCloseTimeout != 4*time.Minute || current.StartToCloseTimeout != 3*time.Minute || current.HeartbeatTimeout != 10*time.Second {
		t.Fatalf("current cohort timeouts = %s/%s/%s, want 4m/3m/10s", current.ScheduleToCloseTimeout, current.StartToCloseTimeout, current.HeartbeatTimeout)
	}
	if current.ActivityID != "recovery-cohort/item-001" || current.TaskQueue != input.EffectTaskQueue || !current.WaitForCancellation {
		t.Fatalf("cohort identity or routing changed: %+v", current)
	}
}

func TestReplacementDecisionMustCarryDeclaredAuthority(t *testing.T) {
	authority := Authority{Generation: 2, CapabilityHash: workstore.HashToken("owner-2")}
	if err := validateReplacementDecision(workstore.Decision{
		Action: workstore.ActionLaunch,
		Lease:  workstore.Lease{Generation: authority.Generation, OwnerToken: "owner-2"},
	}, authority); err != nil {
		t.Fatalf("matching replacement decision: %v", err)
	}
	if err := validateReplacementDecision(workstore.Decision{
		Action: workstore.ActionComplete,
		Lease:  workstore.Lease{Generation: 1, OwnerToken: "owner-1"},
	}, authority); err == nil {
		t.Fatal("generation-1 terminal decision was accepted as generation-2 replacement authority")
	}
}

func TestParentWorkflowChangesOnlyTheSchedulingEdge(t *testing.T) {
	for _, benchmarkCase := range []protocol.CaseID{
		protocol.CaseJoinBarrier,
		protocol.CaseIncrementalPartialReduction,
		protocol.CaseQueuedExecutingSupersession,
		protocol.CaseDestructiveTransition,
	} {
		t.Run(string(benchmarkCase), func(t *testing.T) {
			outputs := make(map[protocol.Topology]ParentOutput)
			calls := make(map[protocol.Topology][]WorkInput)
			for _, topology := range protocol.Topologies() {
				recorder := newActivityRecorder(benchmarkCase)
				var suite testsuite.WorkflowTestSuite
				environment := suite.NewTestWorkflowEnvironment()
				environment.RegisterWorkflowWithOptions(ParentWorkflow, workflow.RegisterOptions{Name: ParentWorkflowName})
				environment.RegisterWorkflowWithOptions(ItemWorkflow, workflow.RegisterOptions{Name: ItemWorkflowName})
				environment.RegisterActivityWithOptions(recorder.Work, activity.RegisterOptions{Name: WorkActivityName})
				environment.RegisterActivityWithOptions(recorder.Checkpoint, activity.RegisterOptions{Name: CheckpointActivityName})
				environment.RegisterActivityWithOptions(recorder.Continue, activity.RegisterOptions{Name: ContinuationActivityName})
				environment.RegisterActivityWithOptions(recorder.Supersede, activity.RegisterOptions{Name: SupersedeActivityName})
				environment.RegisterActivityWithOptions(recorder.Cancellation, activity.RegisterOptions{Name: CancellationActivityName})
				environment.RegisterActivityWithOptions(recorder.Destructive, activity.RegisterOptions{Name: DestructiveActivityName})
				childStarts := 0
				environment.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) {
					childStarts++
				})
				environment.ExecuteWorkflow(ParentWorkflowName, validInput(benchmarkCase, topology, protocol.ProbeProtected))
				if err := environment.GetWorkflowError(); err != nil {
					t.Fatal(err)
				}
				var output ParentOutput
				if err := environment.GetWorkflowResult(&output); err != nil {
					t.Fatal(err)
				}
				wantChildren := 0
				if topology == protocol.TopologyChildWorkflow {
					wantChildren = 8
					if benchmarkCase == protocol.CaseQueuedExecutingSupersession {
						wantChildren++
					}
				}
				if childStarts != wantChildren {
					t.Fatalf("child starts = %d, want %d", childStarts, wantChildren)
				}
				outputs[topology] = output
				calls[topology] = recorder.workInputs()
			}
			if !reflect.DeepEqual(calls[protocol.TopologyDirectActivity], calls[protocol.TopologyChildWorkflow]) {
				t.Fatalf("work inputs differ by topology:\ndirect=%+v\nchild=%+v", calls[protocol.TopologyDirectActivity], calls[protocol.TopologyChildWorkflow])
			}
			direct, child := outputs[protocol.TopologyDirectActivity], outputs[protocol.TopologyChildWorkflow]
			direct.Topology, child.Topology = "", ""
			if !reflect.DeepEqual(direct, child) {
				t.Fatalf("logical outputs differ:\ndirect=%+v\nchild=%+v", direct, child)
			}
		})
	}
}

func TestRecoveryParentWorkflowChangesOnlyTheSchedulingEdge(t *testing.T) {
	for _, benchmarkCase := range []protocol.CaseID{
		protocol.CaseCrashRecoveryBoundaries,
		protocol.CaseLayeredRetryAmplification,
		protocol.CaseOutageBacklogHerdRecovery,
		protocol.CaseBackpressureOverload,
		protocol.CasePoisonWorkIsolation,
		protocol.CaseSilentProgress,
	} {
		t.Run(string(benchmarkCase), func(t *testing.T) {
			outputs := make(map[protocol.Topology]ParentOutput)
			calls := make(map[protocol.Topology][]RecoveryWorkInput)
			for _, topology := range protocol.Topologies() {
				recorder := &recoveryActivityRecorder{}
				var suite testsuite.WorkflowTestSuite
				environment := suite.NewTestWorkflowEnvironment()
				environment.RegisterWorkflowWithOptions(ParentWorkflow, workflow.RegisterOptions{Name: ParentWorkflowName})
				environment.RegisterWorkflowWithOptions(RecoveryItemWorkflow, workflow.RegisterOptions{Name: RecoveryItemWorkflowName})
				environment.RegisterActivityWithOptions(recorder.Work, activity.RegisterOptions{Name: RecoveryWorkActivityName})
				environment.RegisterActivityWithOptions(recorder.Admit, activity.RegisterOptions{Name: RecoveryAdmissionActivityName})
				childStarts := 0
				environment.SetOnChildWorkflowStartedListener(func(*workflow.Info, workflow.Context, converter.EncodedValues) {
					childStarts++
				})
				environment.ExecuteWorkflow(ParentWorkflowName, validInput(benchmarkCase, topology, protocol.ProbeProtected))
				if err := environment.GetWorkflowError(); err != nil {
					t.Fatal(err)
				}
				var output ParentOutput
				if err := environment.GetWorkflowResult(&output); err != nil {
					t.Fatal(err)
				}
				wantChildren := 0
				if topology == protocol.TopologyChildWorkflow {
					wantChildren = 8
				}
				if childStarts != wantChildren {
					t.Fatalf("child starts = %d, want %d", childStarts, wantChildren)
				}
				outputs[topology] = output
				calls[topology] = recorder.inputs()
			}
			if !reflect.DeepEqual(calls[protocol.TopologyDirectActivity], calls[protocol.TopologyChildWorkflow]) {
				t.Fatalf("recovery Work inputs differ by topology:\ndirect=%+v\nchild=%+v",
					calls[protocol.TopologyDirectActivity], calls[protocol.TopologyChildWorkflow])
			}
			direct, child := outputs[protocol.TopologyDirectActivity], outputs[protocol.TopologyChildWorkflow]
			direct.Topology, child.Topology = "", ""
			if !reflect.DeepEqual(direct, child) {
				t.Fatalf("recovery outputs differ:\ndirect=%+v\nchild=%+v", direct, child)
			}
		})
	}
}

func TestOutageRecoveryWaitsForExactCohortBeforeRetry(t *testing.T) {
	recorder := &cohortActivityRecorder{}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(RecoveryItemWorkflow, workflow.RegisterOptions{Name: RecoveryItemWorkflowName})
	environment.RegisterActivityWithOptions(recorder.Work, activity.RegisterOptions{Name: RecoveryWorkActivityName})
	environment.RegisterActivityWithOptions(recorder.Gate, activity.RegisterOptions{Name: RecoveryCohortActivityName})
	parent := validInput(protocol.CaseOutageBacklogHerdRecovery, protocol.TopologyChildWorkflow, protocol.ProbeUnsafe)
	environment.ExecuteWorkflow(RecoveryItemWorkflowName, recoveryWorkInput(parent, parent.Items[0]))
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result RecoveryWorkResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.Disposition != protocol.RecoveryDispositionSucceeded {
		t.Fatalf("disposition = %q", result.Disposition)
	}
	if got, want := recorder.sequence(), []string{"work-0", "cohort-gate", "work-1"}; !slices.Equal(got, want) {
		t.Fatalf("recovery sequence = %v, want %v", got, want)
	}
}

func TestJoinAccumulatorDistinguishesPhysicalAttemptsFromLogicalMembership(t *testing.T) {
	result := WorkResult{
		ItemID: "item-001", Ordinal: 1,
		Deliveries: []Contribution{{ItemID: "item-001", Ordinal: 1, Attempt: 1}, {ItemID: "item-001", Ordinal: 1, Attempt: 2}},
	}
	protected := newJoinAccumulator(2, true)
	unsafe := newJoinAccumulator(2, false)
	if ready, err := protected.accept(result); err != nil || ready {
		t.Fatalf("protected ready=%v err=%v", ready, err)
	}
	if ready, err := unsafe.accept(result); err != nil || !ready {
		t.Fatalf("unsafe ready=%v err=%v", ready, err)
	}
	if got := unsafe.members(); !reflect.DeepEqual(got, []string{"item-001"}) {
		t.Fatalf("unsafe members = %v", got)
	}
}

func TestReductionAccumulatorRejectsRetryDoubleCountInProtectedTrack(t *testing.T) {
	protected := newReductionAccumulator(items(8), true)
	unsafe := newReductionAccumulator(items(8), false)
	results := []WorkResult{{
		ItemID: "item-001", Ordinal: 1,
		Deliveries: []Contribution{{ItemID: "item-001", Ordinal: 1, Attempt: 1}, {ItemID: "item-001", Ordinal: 1, Attempt: 2}},
	}}
	for ordinal := 2; ordinal <= 8; ordinal++ {
		results = append(results, WorkResult{
			ItemID: fmt.Sprintf("item-%03d", ordinal), Ordinal: ordinal,
			Deliveries: []Contribution{{ItemID: fmt.Sprintf("item-%03d", ordinal), Ordinal: ordinal, Attempt: 1}},
		})
	}
	var protectedCheckpoints, unsafeCheckpoints []CheckpointInput
	for _, result := range results {
		checkpoints, err := protected.accept(result)
		if err != nil {
			t.Fatal(err)
		}
		protectedCheckpoints = append(protectedCheckpoints, checkpoints...)
		checkpoints, err = unsafe.accept(result)
		if err != nil {
			t.Fatal(err)
		}
		unsafeCheckpoints = append(unsafeCheckpoints, checkpoints...)
	}
	if got := protectedCheckpoints[len(protectedCheckpoints)-1]; got.Value != 36 || len(got.Members) != 8 {
		t.Fatalf("protected final checkpoint = %+v", got)
	}
	unsafeFinal := unsafeCheckpoints[len(unsafeCheckpoints)-1]
	if unsafeFinal.Value == 36 || len(unsafeFinal.Members) != 8 || unsafeFinal.Members[0] != unsafeFinal.Members[1] {
		t.Fatalf("unsafe control did not misreduce: %+v", unsafeFinal)
	}
}

func TestReductionAccumulatorFailsClosedOnIntegerOverflow(t *testing.T) {
	accumulator := newReductionAccumulator([]Item{{ID: "item-a", Ordinal: math.MaxInt}, {ID: "item-b", Ordinal: math.MaxInt}}, true)
	for index, item := range []Item{{ID: "item-a", Ordinal: math.MaxInt}, {ID: "item-b", Ordinal: math.MaxInt}} {
		_, err := accumulator.accept(WorkResult{
			ItemID: item.ID, Ordinal: item.Ordinal,
			Deliveries: []Contribution{{ItemID: item.ID, Ordinal: item.Ordinal, Attempt: 1}},
		})
		if index == 0 && err != nil {
			t.Fatal(err)
		}
		if index == 1 && err == nil {
			t.Fatal("overflowing reduction was accepted")
		}
	}
}

func TestParentWorkflowTerminalRequiredFailureDoesNotContinue(t *testing.T) {
	recorder := &activityRecorder{failItem: "item-004"}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflowWithOptions(ParentWorkflow, workflow.RegisterOptions{Name: ParentWorkflowName})
	environment.RegisterWorkflowWithOptions(ItemWorkflow, workflow.RegisterOptions{Name: ItemWorkflowName})
	environment.RegisterActivityWithOptions(recorder.Work, activity.RegisterOptions{Name: WorkActivityName})
	environment.RegisterActivityWithOptions(recorder.Checkpoint, activity.RegisterOptions{Name: CheckpointActivityName})
	environment.RegisterActivityWithOptions(recorder.Continue, activity.RegisterOptions{Name: ContinuationActivityName})
	environment.RegisterActivityWithOptions(recorder.Supersede, activity.RegisterOptions{Name: SupersedeActivityName})
	environment.RegisterActivityWithOptions(recorder.Cancellation, activity.RegisterOptions{Name: CancellationActivityName})
	environment.RegisterActivityWithOptions(recorder.Destructive, activity.RegisterOptions{Name: DestructiveActivityName})
	environment.ExecuteWorkflow(ParentWorkflowName, validInput(protocol.CaseJoinBarrier, protocol.TopologyChildWorkflow, protocol.ProbeProtected))
	if environment.GetWorkflowError() == nil {
		t.Fatal("terminal required-item failure completed the join")
	}
	if recorder.continuationCount() != 0 {
		t.Fatalf("continuations = %d", recorder.continuationCount())
	}
}

type activityRecorder struct {
	mu            sync.Mutex
	work          []WorkInput
	continuations []ContinuationInput
	failItem      string
	oldStarted    chan struct{}
	oldStartOnce  sync.Once
}

type recoveryActivityRecorder struct {
	mu   sync.Mutex
	work []RecoveryWorkInput
}

type cohortActivityRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *cohortActivityRecorder) Work(_ context.Context, input RecoveryWorkInput) (RecoveryWorkResult, error) {
	r.mu.Lock()
	r.calls = append(r.calls, fmt.Sprintf("work-%d", input.RecoveryRound))
	r.mu.Unlock()
	result := RecoveryWorkResult{
		ItemID: input.Item.ID, Ordinal: input.Item.Ordinal, Disposition: protocol.RecoveryDispositionSucceeded,
	}
	if input.RecoveryRound == 0 {
		result.Disposition = protocol.RecoveryDispositionUnresolved
		result.NeedsRecoveryRetry = true
	}
	return result, nil
}

func (r *cohortActivityRecorder) Gate(_ context.Context, _ RecoveryWorkInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "cohort-gate")
	return nil
}

func (r *cohortActivityRecorder) sequence() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *recoveryActivityRecorder) Admit(_ context.Context, input RecoveryAdmissionInput) (RecoveryAdmissionReceipt, error) {
	return RecoveryAdmissionReceipt{BatchOrdinal: input.BatchOrdinal, Admitted: len(input.Items)}, nil
}

func (r *recoveryActivityRecorder) Work(_ context.Context, input RecoveryWorkInput) (RecoveryWorkResult, error) {
	r.mu.Lock()
	r.work = append(r.work, input)
	r.mu.Unlock()
	return RecoveryWorkResult{
		ItemID: input.Item.ID, Ordinal: input.Item.Ordinal, Disposition: protocol.RecoveryDispositionSucceeded,
	}, nil
}

func (r *recoveryActivityRecorder) inputs() []RecoveryWorkInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := slices.Clone(r.work)
	slices.SortFunc(result, func(first, second RecoveryWorkInput) int { return first.Item.Ordinal - second.Item.Ordinal })
	return result
}

func newActivityRecorder(benchmarkCase protocol.CaseID) *activityRecorder {
	recorder := &activityRecorder{}
	if benchmarkCase == protocol.CaseQueuedExecutingSupersession {
		recorder.oldStarted = make(chan struct{})
	}
	return recorder
}

func (r *activityRecorder) Work(_ context.Context, input WorkInput) (WorkResult, error) {
	r.mu.Lock()
	r.work = append(r.work, input)
	r.mu.Unlock()
	if r.oldStarted != nil && input.Item.ID == "item-001" && input.Authority.Generation == 1 {
		r.oldStartOnce.Do(func() { close(r.oldStarted) })
	}
	if input.Item.ID == r.failItem {
		return WorkResult{}, fmt.Errorf("terminal item failure: %s", input.Item.ID)
	}
	return WorkResult{
		ItemID: input.Item.ID, Ordinal: input.Item.Ordinal,
		Deliveries: []Contribution{{ItemID: input.Item.ID, Ordinal: input.Item.Ordinal, Attempt: 1}},
	}, nil
}

func (*activityRecorder) Checkpoint(_ context.Context, input CheckpointInput) (CheckpointReceipt, error) {
	return CheckpointReceipt{CheckpointID: input.CheckpointID, ReceiptID: "receipt/" + input.CheckpointID}, nil
}

func (r *activityRecorder) Continue(_ context.Context, input ContinuationInput) (ContinuationReceipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.continuations = append(r.continuations, input)
	return ContinuationReceipt{ContinuationID: input.ContinuationID, ReceiptID: "receipt/" + input.ContinuationID}, nil
}

func (r *activityRecorder) Supersede(ctx context.Context, input SupersedeInput) (SupersedeReceipt, error) {
	if r.oldStarted != nil {
		select {
		case <-ctx.Done():
			return SupersedeReceipt{}, ctx.Err()
		case <-r.oldStarted:
		}
	}
	return SupersedeReceipt{ItemID: input.ItemID, Generation: input.Replacement.Generation}, nil
}

func (*activityRecorder) Cancellation(_ context.Context, input CancellationInput) (CancellationReceipt, error) {
	disposition := "canceled"
	if !input.Requested {
		disposition = "continued-unsafe"
	}
	return CancellationReceipt{ItemID: input.ItemID, Disposition: disposition}, nil
}

func (*activityRecorder) Destructive(_ context.Context, input DestructiveActivityInput) (DestructiveResult, error) {
	return DestructiveResult{
		OperationID: input.OperationID, Decision: protocol.DecisionAccepted, Applied: true,
		ReceiptID: "receipt/" + input.OperationID, PreviousVersion: input.ExpectedVersion, ResultingVersion: input.ExpectedVersion + 1,
	}, nil
}

func (r *activityRecorder) workInputs() []WorkInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := slices.Clone(r.work)
	slices.SortFunc(result, func(first, second WorkInput) int {
		if first.Item.Ordinal != second.Item.Ordinal {
			return first.Item.Ordinal - second.Item.Ordinal
		}
		if first.Authority.Generation < second.Authority.Generation {
			return -1
		}
		if first.Authority.Generation > second.Authority.Generation {
			return 1
		}
		return 0
	})
	return result
}

func (r *activityRecorder) continuationCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.continuations)
}

func validInput(benchmarkCase protocol.CaseID, topology protocol.Topology, probe protocol.Probe) ParentInput {
	return ParentInput{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		PairID:          "pair-001", LogicalOperationID: "operation-001", WorkTaskQueue: "topology-work-test",
		EffectTaskQueue: "topology-effect-test", Topology: topology,
		Case: benchmarkCase, Boundary: primaryBoundary(benchmarkCase), Probe: probe, Items: items(8),
		InitialAuthority:     Authority{Generation: 1, CapabilityHash: repeatedHash('a')},
		ReplacementAuthority: Authority{Generation: 2, CapabilityHash: repeatedHash('b')},
	}
}

func primaryBoundary(benchmarkCase protocol.CaseID) string {
	return map[protocol.CaseID]string{
		protocol.CaseJoinBarrier:                 "designated-item-result-observed-before-activity-completion",
		protocol.CaseIncrementalPartialReduction: "partial-checkpoint-accepted-before-checkpoint-activity-completion",
		protocol.CaseQueuedExecutingSupersession: "executing-after-process-start-before-effect",
		protocol.CaseDestructiveTransition:       "destination-accepted-before-activity-completion",
		protocol.CaseCrashRecoveryBoundaries:     "result-observed-before-activity-completion",
		protocol.CaseLayeredRetryAmplification:   "dependency-first-request-before-scripted-timeout-500-429-sequence",
		protocol.CaseOutageBacklogHerdRecovery:   "outage-backlog-restoration-and-catchup-worker-crash",
		protocol.CaseBackpressureOverload:        "ready-workers-before-fixed-cohort-release",
		protocol.CasePoisonWorkIsolation:         "mixed-cohort-admitted-before-poison-failure-release",
		protocol.CaseSilentProgress:              "accepted-progress-before-executor-wedge",
	}[benchmarkCase]
}

func TestProgressReplacementDelayReservesDispatchMargin(t *testing.T) {
	deadline := time.Duration(progressDeadlineMS) * time.Millisecond
	if got, want := progressReplacementDelayForVersion(protocol.ProbeProtected, workflow.DefaultVersion), deadline-2*time.Second; got != want {
		t.Fatalf("old-history protected replacement delay = %s, want %s", got, want)
	}
	if got, want := progressReplacementDelayForVersion(protocol.ProbeProtected, 1), deadline-3*time.Second; got != want {
		t.Fatalf("v1 protected replacement delay = %s, want %s", got, want)
	}
	if got, want := progressReplacementDelayForVersion(protocol.ProbeProtected, 2), deadline-4*time.Second; got != want {
		t.Fatalf("current protected replacement delay = %s, want %s", got, want)
	}
	if got := progressReplacementDelayForVersion(protocol.ProbeUnsafe, 1); got <= deadline {
		t.Fatalf("unsafe replacement delay = %s, want greater than %s", got, deadline)
	}
}

func TestEffectiveInputFingerprintsCurrentActivityOptionVariants(t *testing.T) {
	wantOrchestration := hashString("work-and-effect-v2:default-1m:30s:10s:held-supersession-3m:2m:10s:wait-cancel:4:50ms:2x:1s")
	if got := orchestrationActivityOptionsSHA256(); got != wantOrchestration {
		t.Fatalf("orchestration Activity-options hash = %s, want %s", got, wantOrchestration)
	}
	wantRecovery := hashString("recovery-work-v2:work-2m:30s:2s:admission-1m:30s:2s:cohort-4m:3m:10s:silent-detect-1s:wait-cancel:shared-case-policy")
	if got := recoveryActivityOptionsSHA256(); got != wantRecovery {
		t.Fatalf("recovery Activity-options hash = %s, want %s", got, wantRecovery)
	}
}

func items(count int) []Item {
	result := make([]Item, count)
	for index := range result {
		result[index] = Item{ID: fmt.Sprintf("item-%03d", index+1), Ordinal: index + 1}
	}
	return result
}
