package semantics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/workflow"
)

func TestRuntimeBarrierCloseForcesHeldRequestAfterGracePeriod(t *testing.T) {
	barrier, err := newRuntimeBarrier()
	if err != nil {
		t.Fatal(err)
	}
	requestContext, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- failureinject.NewClient(barrier.url).Arrive(requestContext, failureinject.Arrival{
			ID: "held-arrival", Point: "held-point", SessionID: "held-session", ActorID: "held-actor",
		})
	}()
	arrivalContext, cancelArrival := context.WithTimeout(context.Background(), time.Second)
	defer cancelArrival()
	if _, err := barrier.coordinator.WaitForArrivals(arrivalContext, "held-point", 1); err != nil {
		t.Fatal(err)
	}

	if err := barrier.close(); err != nil {
		t.Fatalf("close barrier with a held request: %v", err)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("forced barrier close left the held request running")
	}
}

func TestRuntimeBarrierDoesNotExpireHeldArrivalResponse(t *testing.T) {
	barrier, err := newRuntimeBarrier()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := barrier.close(); err != nil {
			t.Errorf("close barrier: %v", err)
		}
	})
	// Arrival requests are the exact experimental barrier: their response is
	// deliberately held until the registered fault controller releases it.
	// The caller context and Activity/Workflow deadlines bound the wait. A
	// server WriteTimeout would instead create an unregistered transport fault.
	if got := barrier.server.WriteTimeout; got != 0 {
		t.Fatalf("barrier WriteTimeout = %s, want no server response deadline", got)
	}
}

func TestPoisonRetryCannotBypassUncommittedCohortFault(t *testing.T) {
	const fanout = 128
	started := make(map[string]bool, fanout)
	for ordinal := 1; ordinal <= fanout; ordinal++ {
		started[fmt.Sprintf("item-%03d", ordinal)] = true
	}
	runtime := &EpisodeRuntime{
		spec: EpisodeSpec{
			RunID: "poison-retry-boundary", Boundary: "mixed-cohort-admitted-before-poison-failure-release",
			Probe: protocol.ProbeProtected, Fanout: fanout,
		},
		startedAt: time.Now(), faultRequests: make(chan FaultRequest, 1),
		recovery: &recoveryRuntimeState{
			poisonInitialStarted: started,
			poisonRelease:        make(chan struct{}),
		},
	}
	identity := protocol.Identity{
		WorkItemID: "item-001", ActivityAttempt: 2, WorkerID: "worker-retry",
		ProcessIdentity: "process-retry",
	}
	done := make(chan error, 1)
	go func() { done <- runtime.awaitPoisonRelease(context.Background(), identity) }()

	select {
	case request := <-runtime.FaultRequests():
		if request.Identity.ActivityAttempt != 2 {
			t.Fatalf("fault attempt = %d, want retry attempt 2", request.Identity.ActivityAttempt)
		}
		if err := runtime.CommitFault(request); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry bypassed the uncommitted poison cohort fault")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !runtime.fault.Injected {
		t.Fatal("retry returned without a committed fault")
	}
}

func TestPeakRecoveryQPSUsesFrozenTenMillisecondWindow(t *testing.T) {
	requests := []protocol.DependencyRequest{
		{StartedOffsetNS: 15 * int64(time.Millisecond)},
		{StartedOffsetNS: 0},
		{StartedOffsetNS: 5 * int64(time.Millisecond)},
	}
	if got := peakRecoveryQPS(requests, 0); got != 200 {
		t.Fatalf("peakRecoveryQPS() = %d, want 200 requests/second from frozen 10ms window", got)
	}
}

func TestRecoveryAdmissionWindowBoundsProtectedControlSensitiveCases(t *testing.T) {
	tests := []struct {
		benchmarkCase protocol.CaseID
		probe         protocol.Probe
		want          int
	}{
		{protocol.CaseBackpressureOverload, protocol.ProbeProtected, workerActivityConcurrency},
		{protocol.CaseBackpressureOverload, protocol.ProbeUnfaulted, workerActivityConcurrency},
		{protocol.CaseBackpressureOverload, protocol.ProbeUnsafe, 32},
		{protocol.CaseSilentProgress, protocol.ProbeProtected, workerActivityConcurrency},
		{protocol.CaseSilentProgress, protocol.ProbeUnfaulted, workerActivityConcurrency},
		{protocol.CaseSilentProgress, protocol.ProbeUnsafe, 32},
		{protocol.CaseOutageBacklogHerdRecovery, protocol.ProbeProtected, 32},
		{protocol.CasePoisonWorkIsolation, protocol.ProbeProtected, 32},
		{protocol.CaseLayeredRetryAmplification, protocol.ProbeProtected, 32},
	}
	for _, test := range tests {
		if got := recoveryAdmissionWindow(test.benchmarkCase, test.probe, 32); got != test.want {
			t.Fatalf("recoveryAdmissionWindow(%s, %s) = %d, want %d", test.benchmarkCase, test.probe, got, test.want)
		}
	}
	if got := recoveryAdmissionWindowForVersion(
		protocol.CaseSilentProgress, protocol.ProbeProtected, 32, workflow.DefaultVersion,
	); got != 32 {
		t.Fatalf("old-history silent-progress admission window = %d, want 32", got)
	}
	if got := recoveryAdmissionWindowForVersion(protocol.CaseSilentProgress, protocol.ProbeProtected, 32, 1); got != workerActivityConcurrency {
		t.Fatalf("new silent-progress admission window = %d, want %d", got, workerActivityConcurrency)
	}
}

func TestWorkflowTaskConcurrencyCoversTheFrozenScaleLadder(t *testing.T) {
	// Recovery Activities remain fixed at eight. Workflow tasks need enough
	// independent slots for every frozen child to schedule its first Activity;
	// otherwise hot retrying children can starve late cohort members before the
	// exact outage-backlog barrier is reached.
	if workflowTaskConcurrency < 128 {
		t.Fatalf("Workflow task concurrency %d cannot cover frozen fan-out 128", workflowTaskConcurrency)
	}
}

func TestTopologyWorkflowTimeoutCoversBacklogRecovery(t *testing.T) {
	if topologyWorkflowExecutionTimeout != 5*time.Minute {
		t.Fatalf("Workflow execution timeout = %s, want frozen five-minute recovery budget", topologyWorkflowExecutionTimeout)
	}
}

func TestProtectedSilentProgressReplacementUsesControlTaskQueue(t *testing.T) {
	base := RecoveryWorkInput{
		WorkTaskQueue: "bulk-work", EffectTaskQueue: "control-effects",
		Case: protocol.CaseSilentProgress, Probe: protocol.ProbeProtected,
	}
	replacement := base
	replacement.Replacement = true
	if got := recoveryActivityTaskQueue(replacement, 1); got != base.EffectTaskQueue {
		t.Fatalf("protected replacement queue = %q, want %q", got, base.EffectTaskQueue)
	}
	if got := recoveryActivityTaskQueue(replacement, workflow.DefaultVersion); got != base.WorkTaskQueue {
		t.Fatalf("old-history protected replacement queue = %q, want %q", got, base.WorkTaskQueue)
	}
	for name, input := range map[string]RecoveryWorkInput{
		"initial":           base,
		"unsafe-release":    {WorkTaskQueue: base.WorkTaskQueue, EffectTaskQueue: base.EffectTaskQueue, Case: base.Case, Probe: protocol.ProbeUnsafe, ReleaseWedged: true},
		"other-replacement": {WorkTaskQueue: base.WorkTaskQueue, EffectTaskQueue: base.EffectTaskQueue, Case: protocol.CaseCrashRecoveryBoundaries, Probe: base.Probe, Replacement: true},
	} {
		if got := recoveryActivityTaskQueue(input, 1); got != base.WorkTaskQueue {
			t.Fatalf("%s queue = %q, want bulk queue %q", name, got, base.WorkTaskQueue)
		}
	}
}

func TestRecoveryDependencyServiceRejectsUnboundedOrAmbiguousInput(t *testing.T) {
	service, err := startRecoveryDependencyService(protocol.CaseLayeredRetryAmplification)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.close(); err != nil {
			t.Errorf("close dependency service: %v", err)
		}
	})
	valid := `{"request_id":"request-1","item_id":"item-001","ordinal":1,"case":"layered-retry-amplification","probe":"protected","fanout":8}`
	for _, test := range []struct {
		name string
		path string
		body string
	}{
		{name: "unknown field", path: "/", body: valid[:len(valid)-1] + `,"unexpected":true}`},
		{name: "trailing object", path: "/", body: valid + `{}`},
		{name: "wrong path", path: "/other", body: valid},
		{name: "oversized", path: "/", body: `{"padding":"` + string(bytes.Repeat([]byte{'x'}, 65<<10)) + `"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, err := http.Post(service.url+test.path, "application/json", bytes.NewBufferString(test.body))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode < 400 {
				t.Fatalf("status = %d, want rejection", response.StatusCode)
			}
		})
	}
}

func TestRecoveryDependencyCallsSerializeCompleteLineagePerItem(t *testing.T) {
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() { _ = request.Body.Close() }()
		var input dependencyServiceRequest
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			t.Errorf("decode dependency request: %v", err)
			return
		}
		switch input.Ordinal {
		case 1:
			close(firstStarted)
			<-releaseFirst
		case 2:
			close(secondStarted)
		default:
			t.Errorf("request ordinal = %d, want 1 or 2", input.Ordinal)
		}
		_ = json.NewEncoder(writer).Encode(dependencyServiceResponse{
			Outcome: "ok", CostUnits: 1, ConcurrentAtStart: 1,
		})
	}))
	t.Cleanup(server.Close)
	runtime := &EpisodeRuntime{
		spec:      EpisodeSpec{RunID: "dependency-lineage", Case: protocol.CaseOutageBacklogHerdRecovery, Probe: protocol.ProbeUnfaulted, Fanout: 8},
		startedAt: time.Now(),
		recovery: &recoveryRuntimeState{
			items: map[string]*recoveryItemRuntime{
				"item-001": {attempts: make(map[int]bool), processes: make(map[string]bool)},
			},
			lastRequestByItem:  make(map[string]protocol.DependencyRequest),
			requestCountByItem: make(map[string]int),
			requestGates:       map[string]chan struct{}{"item-001": make(chan struct{}, 1)},
			dependency:         &recoveryDependencyService{url: server.URL},
		},
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := runtime.callRecoveryDependency(context.Background(), protocol.Identity{
			WorkItemID: "item-001", ActivityAttempt: 1,
		}, "first-attempt")
		firstDone <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first dependency request did not reach the exact server barrier")
	}
	secondDone := make(chan error, 1)
	go func() {
		_, err := runtime.callRecoveryDependency(context.Background(), protocol.Identity{
			WorkItemID: "item-001", ActivityAttempt: 2,
		}, "retry-attempt")
		secondDone <- err
	}()
	select {
	case <-secondStarted:
		t.Fatal("second same-item request started before the first request finished")
	case <-time.After(250 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	for index, done := range []chan error{firstDone, secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("dependency call %d: %v", index+1, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("dependency call %d did not finish", index+1)
		}
	}
	if len(runtime.requests) != 2 {
		t.Fatalf("dependency requests = %d, want 2", len(runtime.requests))
	}
	first, second := runtime.requests[0], runtime.requests[1]
	if second.ParentRequestID != first.RequestID || second.RetryOrdinal != first.RetryOrdinal+1 ||
		second.StartedOffsetNS < first.FinishedOffsetNS {
		t.Fatalf("same-item lineage overlapped or lost parentage: first=%+v second=%+v", first, second)
	}
}

func TestSilentProgressReplacementRetryAttachesCommittedGeneration(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "replacement.db"))
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "silent-progress/operation/item-001"
	if _, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: sessionID, Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "work-worker", AgentBuild: "topology-recovery-agent-v1", Attempt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &EpisodeRuntime{
		spec:           EpisodeSpec{LogicalOperationID: "silent-progress/operation"},
		startedAt:      time.Now(),
		tokens:         map[uint64]string{1: "owner-1", 2: "owner-2"},
		recoveryStores: map[string]*workstore.Store{"item-001": store},
		recovery: &recoveryRuntimeState{
			replacementGates: map[string]chan struct{}{"item-001": make(chan struct{}, 1)},
		},
	}
	input := RecoveryWorkInput{
		Item:      Item{ID: "item-001", Ordinal: 1},
		Authority: Authority{Generation: 2, CapabilityHash: workstore.HashToken("owner-2")},
	}
	first, err := runtime.claimRecoveryReplacement(context.Background(), input, "effect-worker-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	wedge := recoveryWedge{identity: protocol.Identity{
		LogicalOperationID: "silent-progress/operation", WorkItemID: "item-001",
		Generation: 1, CapabilityHash: workstore.HashToken("owner-1"),
	}}
	firstRevocation := runtime.recordSilentProgressRevocation(wedge, first.Lease)
	second, err := runtime.claimRecoveryReplacement(context.Background(), input, "effect-worker-2", 2)
	if err != nil {
		t.Fatalf("retry after committed replacement: %v", err)
	}
	secondRevocation := runtime.recordSilentProgressRevocation(wedge, second.Lease)
	if first.Action != workstore.ActionLaunch || second.Action != workstore.ActionAttach || first.Lease != second.Lease ||
		first.Lease.Generation != input.Authority.Generation {
		t.Fatalf("replacement decisions = first %+v, retry %+v", first, second)
	}
	if firstRevocation == "" || secondRevocation != firstRevocation {
		t.Fatalf("revocation events = first %q, retry %q", firstRevocation, secondRevocation)
	}
	revocations := 0
	for _, event := range runtime.events {
		if event.Kind != protocol.EventAuthorityRevoked {
			continue
		}
		revocations++
		if event.Generation != 1 || event.Details["replacement_generation"] != "2" ||
			event.Details["replacement_capability_hash"] != workstore.HashToken("owner-2") {
			t.Fatalf("revocation event = %+v", event)
		}
	}
	if revocations != 1 {
		t.Fatalf("revocation event count = %d, want 1", revocations)
	}
	snapshot, err := store.Snapshot(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ActiveGeneration != 2 || len(snapshot.Executors) != 2 {
		t.Fatalf("replacement snapshot = %+v", snapshot)
	}
}
