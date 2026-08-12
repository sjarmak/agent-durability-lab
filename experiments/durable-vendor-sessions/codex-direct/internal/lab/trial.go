package lab

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type trialAttemptEvidence struct {
	PhysicalAttemptID string                      `json:"physical_attempt_id"`
	Process           *ProcessRecord              `json:"process,omitempty"`
	Thread            *ThreadReceipt              `json:"thread,omitempty"`
	Request           *trialEffectRequestEvidence `json:"request,omitempty"`
	StreamComplete    bool                        `json:"stream_complete"`
	ThreadID          string                      `json:"thread_id,omitempty"`
}

const closeHistoryQueryInterval = 50 * time.Millisecond

var errWorkflowClosedBeforeEffect = errors.New("workflow closed before the controlled effect arrived")

type trialEffectRequestEvidence struct {
	ControlledEffectInput
	OwnerCapabilitySHA256 string `json:"owner_capability_sha256,omitempty"`
}

type trialVerdict struct {
	Admitted                 bool     `json:"admitted"`
	SafetyPassed             bool     `json:"safety_passed"`
	NegativeControlTriggered bool     `json:"negative_control_triggered"`
	ReasonCodes              []string `json:"reason_codes"`
}

type trialSummary struct {
	SchemaVersion       string                  `json:"schema_version"`
	Mode                RecoveryMode            `json:"mode"`
	FaultBoundary       FaultBoundary           `json:"fault_boundary"`
	Trial               int                     `json:"trial"`
	LogicalSessionID    string                  `json:"logical_session_id"`
	LogicalTurnID       string                  `json:"logical_turn_id"`
	LogicalEffectID     string                  `json:"logical_effect_id"`
	WorkflowID          string                  `json:"workflow_id"`
	WorkflowRunID       string                  `json:"workflow_run_id"`
	StartedAt           time.Time               `json:"started_at"`
	FaultAt             time.Time               `json:"fault_at,omitempty"`
	CompletedAt         time.Time               `json:"completed_at"`
	BarrierArrivals     []failureinject.Arrival `json:"barrier_arrivals"`
	WorkflowResult      CodexActivityResult     `json:"workflow_result"`
	WorkspaceBeforeHash string                  `json:"workspace_before_sha256"`
	WorkspaceAfterHash  string                  `json:"workspace_after_sha256"`
	WorkspaceEffects    []WorkspaceEffect       `json:"workspace_effects"`
	Destination         DestinationSnapshot     `json:"destination"`
	Authority           *workstore.Snapshot     `json:"authority,omitempty"`
	Attempts            []trialAttemptEvidence  `json:"attempts"`
	ReplayVerified      bool                    `json:"replay_verified"`
	Metadata            experimentMetadata      `json:"metadata"`
	Verdict             trialVerdict            `json:"verdict"`
}

type codexTrial struct {
	ctx                                                                           context.Context
	client                                                                        client.Client
	address                                                                       string
	options                                                                       ExperimentOptions
	metadata                                                                      experimentMetadata
	boundary                                                                      FaultBoundary
	trial                                                                         int
	startedAt                                                                     time.Time
	staging, finalDirectory, fixture, destinationPath, workspacePath, attemptRoot string
	workspaceBefore                                                               string
	coordinator                                                                   *failureinject.Coordinator
	barrierCredential                                                             failureinject.Credential
	effectBarrier                                                                 *fileBarrier
	barrierServer                                                                 *httptest.Server
	authorityStore                                                                *workstore.Store
	supervisorServer                                                              *httptest.Server
	supervisor                                                                    *turnSupervisor
	supervisorCancel                                                              context.CancelFunc
	decisions                                                                     chan supervisorDecision
	workerConfig                                                                  workerProcessConfig
	workerOne, workerTwo                                                          *managedWorker
	logicalSessionID, workflowID                                                  string
	workflowRun                                                                   client.WorkflowRun
	arrivals                                                                      []failureinject.Arrival
	faultAt                                                                       time.Time
	result                                                                        CodexActivityResult
}

func runCodexTrial(ctx context.Context, temporalClient client.Client, address string,
	options ExperimentOptions, metadata experimentMetadata, boundary FaultBoundary, trial int,
) (directory string, runErr error) {
	state, err := newCodexTrial(ctx, temporalClient, address, options, metadata, boundary, trial)
	if err != nil {
		return "", err
	}
	defer state.cleanup(&runErr)
	if err := state.execute(); err != nil {
		return "", err
	}
	return state.publish()
}

func newCodexTrial(ctx context.Context, temporalClient client.Client, address string,
	options ExperimentOptions, metadata experimentMetadata, boundary FaultBoundary, trial int,
) (*codexTrial, error) {
	runID := fmt.Sprintf("codex-direct-%s-%s-trial-%d", options.RecoveryMode.normalized(), boundary, trial)
	state := &codexTrial{
		ctx: ctx, client: temporalClient, address: address, options: options, metadata: metadata,
		boundary: boundary, trial: trial, startedAt: time.Now().UTC(),
		staging:          filepath.Join(options.EvidenceRoot, ".staging-"+runID),
		finalDirectory:   filepath.Join(options.EvidenceRoot, runID),
		logicalSessionID: runID,
	}
	if err := os.Mkdir(state.staging, 0o750); err != nil {
		return nil, err
	}
	state.fixture = filepath.Join(state.staging, "fixture")
	if err := prepareFixture(ctx, state.fixture); err != nil {
		return nil, err
	}
	state.workspacePath = filepath.Join(state.fixture, "effects.jsonl")
	state.destinationPath = filepath.Join(state.staging, "destination.db")
	state.attemptRoot = filepath.Join(state.staging, "attempts")
	before, err := hashWorkspace(state.fixture)
	if err != nil {
		return nil, err
	}
	state.workspaceBefore = before
	if err := state.prepareCoordination(); err != nil {
		return nil, err
	}
	return state, nil
}

func (t *codexTrial) prepareCoordination() error {
	expected := httpBarrierExpectations(t.options.RecoveryMode, t.boundary, t.logicalSessionID)
	if len(expected) == 0 {
		t.coordinator = failureinject.NewCoordinator()
	} else {
		credential, err := failureinject.NewCredential()
		if err != nil {
			return err
		}
		coordinator, err := failureinject.NewAuthenticatedCoordinator(credential, expected...)
		if err != nil {
			return err
		}
		t.barrierCredential = credential
		t.coordinator = coordinator
	}
	t.barrierServer = httptest.NewServer(t.coordinator.Handler())
	effectBarrier, err := newFileBarrier(filepath.Join(t.staging, "effect-barrier"))
	if err != nil {
		return err
	}
	t.effectBarrier = effectBarrier
	taskQueue := fmt.Sprintf("codex-%s-%s-%d", t.options.RecoveryMode, t.boundary, t.trial)
	t.workerConfig = workerProcessConfig{
		Binary: t.options.WorkerBinary, Directory: t.staging, TemporalAddress: t.address,
		TaskQueue: taskQueue, CodexBinary: t.options.CodexBinary, CodexHome: t.options.CodexHome,
		LauncherBinary: t.options.LauncherBinary, EffectBinary: t.options.EffectBinary,
		FixtureDirectory: t.fixture, DestinationPath: t.destinationPath, WorkspacePath: t.workspacePath,
		RunRoot: t.attemptRoot, BarrierURL: t.barrierServer.URL,
		BarrierCredential: t.barrierCredential,
		BarrierDirectory:  t.effectBarrier.directory, BarrierPoint: committedEffectBarrier,
		FaultBoundary: t.boundary, Model: t.options.Model, ReasoningEffort: t.options.ReasoningEffort,
		OutputSchema: t.options.OutputSchema, Hermetic: t.options.Hermetic,
	}
	if t.options.RecoveryMode.normalized() != RecoveryModeFenced {
		return nil
	}
	store, err := workstore.Open(filepath.Join(t.staging, "authority.db"))
	if err != nil {
		return err
	}
	t.authorityStore = store
	supervisorContext, cancel := context.WithCancel(t.ctx)
	t.supervisorCancel = cancel
	t.decisions = make(chan supervisorDecision, 16)
	runConfig := fencedCodexRunConfig{
		Command: CodexCommand{
			Binary: t.options.CodexBinary, WorkDir: t.fixture, CodexHome: t.options.CodexHome,
			Model: t.options.Model, ReasoningEffort: t.options.ReasoningEffort,
			OutputSchema: t.options.OutputSchema, Sandbox: "workspace-write",
		},
		LauncherBinary: t.options.LauncherBinary, FaultBoundary: t.boundary,
		EffectBinary: t.options.EffectBinary, EffectPayload: "controlled-edit", WorkspacePath: t.workspacePath,
		AuthorityStorePath: filepath.Join(t.staging, "authority.db"),
		BarrierURL:         t.barrierServer.URL, BarrierDirectory: t.effectBarrier.directory,
		BarrierCredential: t.barrierCredential,
		BarrierPoint:      committedEffectBarrier, RunRoot: t.attemptRoot,
		LogicalSessionID: t.logicalSessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		Hermetic: t.options.Hermetic,
		SupervisorURL: func() string {
			if t.supervisorServer == nil {
				return ""
			}
			return t.supervisorServer.URL
		},
	}
	supervisor := newTurnSupervisor(supervisorContext, store, runConfig.run, nil,
		withSupervisorStartValidator(runConfig.validateStart),
		withSupervisorDecisionObserver(func(decision supervisorDecision) { t.decisions <- decision }))
	t.supervisor = supervisor
	t.supervisorServer = httptest.NewServer(newSupervisorHandler(supervisor))
	t.workerConfig.SupervisorURL = t.supervisorServer.URL
	return runConfig.validate()
}

func (t *codexTrial) execute() error {
	if err := t.startFirstWorkerAndWorkflow(); err != nil {
		return err
	}
	if t.boundary == FaultNone {
		return t.executeUnfaulted()
	}
	if t.options.RecoveryMode.normalized() == RecoveryModeFenced {
		switch t.boundary {
		case FaultConcurrentRecovery:
			return t.executeConcurrentRecovery()
		case FaultCancellationWhileExecuting:
			return t.executeCancellation()
		case FaultProcessFailureReplacement:
			return t.executeProcessReplacement()
		}
		return t.executeFencedFault()
	}
	return t.executeUnsafeFault()
}

func (t *codexTrial) executeConcurrentRecovery() error {
	if err := t.waitForEffects(1); err != nil {
		return err
	}
	if err := t.killFirstWorker(); err != nil {
		return err
	}
	manual := make(chan error, 1)
	go func() {
		_, err := newSupervisorClient(t.supervisorServer.URL, nil).StartOrAttach(t.ctx, supervisorStartRequest{
			SessionID: t.logicalSessionID, WorkerID: "parallel-recovery", Attempt: 2,
			LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		})
		manual <- err
	}()
	if err := t.startSecondWorker(); err != nil {
		return err
	}
	if err := t.waitForSupervisorAttachments(2, 2, 1); err != nil {
		return err
	}
	if err := t.releaseEffectBarrier(); err != nil {
		return err
	}
	if err := t.awaitWorkflow(); err != nil {
		return err
	}
	return <-manual
}

func (t *codexTrial) executeCancellation() error {
	if err := t.waitForBoundary(threadRegistrationBarrier, 1); err != nil {
		return err
	}
	t.faultAt = time.Now().UTC()
	if err := t.client.CancelWorkflow(t.ctx, t.workflowID, t.workflowRun.GetRunID()); err != nil {
		return err
	}
	return t.awaitCanceledWorkflow()
}

func (t *codexTrial) executeProcessReplacement() error {
	if err := t.waitForBoundary(preThreadBarrier, 1); err != nil {
		return err
	}
	arrival := t.arrivals[len(t.arrivals)-1]
	snapshot, err := t.authorityStore.Snapshot(t.ctx, t.logicalSessionID)
	if err != nil {
		return err
	}
	t.faultAt = time.Now().UTC()
	identity := agentprocess.ProcessIdentity{
		PID: arrival.PID, StartIdentity: arrival.ProcessStart, ProcessGroupID: arrival.PID,
	}
	if _, err := agentprocess.Signal(agentprocess.ControlRequest{
		Target: agentprocess.ControlTarget{
			SessionID: t.logicalSessionID, Generation: snapshot.ActiveGeneration,
			OwnerTokenHash: snapshot.ActiveOwnerTokenHash, Leader: identity,
			Members: []agentprocess.ProcessIdentity{identity},
		}, Scope: agentprocess.ScopeProcessTree, Signal: agentprocess.SignalKill,
	}); err != nil && !errors.Is(err, agentprocess.ErrProcessGone) {
		return err
	}
	if err := t.waitForSupervisorLaunch(2, 2); err != nil {
		return err
	}
	if err := t.waitForBoundary(preThreadBarrier, 2); err != nil {
		return err
	}
	if err := t.coordinator.Release(preThreadBarrier); err != nil {
		return err
	}
	if err := t.waitForEffects(1); err != nil {
		return err
	}
	if err := t.releaseEffectBarrier(); err != nil {
		return err
	}
	return t.awaitWorkflow()
}

func (t *codexTrial) startFirstWorkerAndWorkflow() error {
	t.workerConfig.WorkerID = "worker-one"
	worker, err := startWorkerProcess(t.workerConfig)
	if err != nil {
		return err
	}
	t.workerOne = worker
	t.workflowID = "codex-direct/" + t.logicalSessionID
	t.workflowRun, err = t.client.ExecuteWorkflow(t.ctx, client.StartWorkflowOptions{
		ID: t.workflowID, TaskQueue: t.workerConfig.TaskQueue, WorkflowExecutionTimeout: t.options.Timeout,
	}, CodexWorkflowName, CodexActivityInput{
		LogicalSessionID: t.logicalSessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		RecoveryMode: t.options.RecoveryMode.normalized(),
	})
	return err
}

func (t *codexTrial) executeUnfaulted() error {
	if err := appendTrialProgress(t.staging, "effect-wait-started", "1"); err != nil {
		return err
	}
	if err := t.waitForEffects(1); err != nil {
		return err
	}
	if err := appendTrialProgress(t.staging, "effect-wait-completed", "1"); err != nil {
		return err
	}
	if err := appendTrialProgress(t.staging, "effect-barrier-release-started", committedEffectBarrier); err != nil {
		return err
	}
	if err := t.releaseEffectBarrier(); err != nil {
		return err
	}
	if err := appendTrialProgress(t.staging, "effect-barrier-release-completed", committedEffectBarrier); err != nil {
		return err
	}
	return t.awaitWorkflow()
}

func (t *codexTrial) executeUnsafeFault() error {
	switch t.boundary {
	case FaultBeforeThreadObservation:
		if err := t.waitForBoundary(preThreadBarrier, 1); err != nil {
			return err
		}
		if err := t.killFirstWorker(); err != nil {
			return err
		}
		if err := t.coordinator.Release(preThreadBarrier); err != nil {
			return err
		}
		if err := t.startSecondWorker(); err != nil {
			return err
		}
		if err := t.waitForEffects(1); err != nil {
			return err
		}
		if err := t.releaseEffectBarrier(); err != nil {
			return err
		}
	case FaultAfterToolEffect:
		if err := t.waitForEffects(1); err != nil {
			return err
		}
		if err := t.replaceWorker(); err != nil {
			return err
		}
		if err := t.releaseEffectBarrier(); err != nil {
			return err
		}
	case FaultAfterFinalOutput:
		if err := t.waitForEffects(1); err != nil {
			return err
		}
		if err := t.releaseEffectBarrier(); err != nil {
			return err
		}
		if err := t.waitForBoundary(finalOutputBarrier, 1); err != nil {
			return err
		}
		if err := t.replaceWorker(); err != nil {
			return err
		}
		if err := t.coordinator.Release(finalOutputBarrier); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported unsafe boundary %q", t.boundary)
	}
	return t.awaitWorkflow()
}

func (t *codexTrial) executeFencedFault() error {
	point := ""
	switch t.boundary {
	case FaultAfterClaimBeforeExec:
		point = claimBeforeExecBarrier
	case FaultBeforeThreadObservation:
		point = preThreadBarrier
	case FaultAfterThreadBeforeRegistration:
		point = threadRegistrationBarrier
	case FaultAfterToolEffect:
		point = committedEffectBarrier
	case FaultAfterFinalOutput:
		if err := t.waitForEffects(1); err != nil {
			return err
		}
		if err := t.releaseEffectBarrier(); err != nil {
			return err
		}
		point = finalOutputBarrier
	default:
		return fmt.Errorf("unsupported fenced boundary %q", t.boundary)
	}
	if err := t.waitForBoundary(point, 1); err != nil {
		return err
	}
	if err := t.replaceWorker(); err != nil {
		return err
	}
	if err := t.waitForSupervisorAttach(2); err != nil {
		return err
	}
	if err := t.releaseBoundary(point); err != nil {
		return err
	}
	if t.boundary != FaultAfterToolEffect && t.boundary != FaultAfterFinalOutput {
		if err := t.waitForEffects(1); err != nil {
			return err
		}
		if err := t.releaseEffectBarrier(); err != nil {
			return err
		}
	}
	return t.awaitWorkflow()
}

func (t *codexTrial) waitForSupervisorAttach(attempt int32) error {
	return t.waitForSupervisorAttachments(1, attempt, 1)
}

func (t *codexTrial) waitForSupervisorAttachments(count int, attempt int32, generation uint64) error {
	observed := 0
	for {
		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		case decision := <-t.decisions:
			if decision.Action == workstore.ActionLaunch && decision.Attempt == 1 {
				continue
			}
			if decision.Action != workstore.ActionAttach || decision.Attempt != attempt || decision.Generation != generation {
				return fmt.Errorf("unexpected supervisor recovery decision: %+v", decision)
			}
			observed++
			if observed == count {
				return nil
			}
		}
	}
}

func (t *codexTrial) waitForSupervisorLaunch(attempt int32, generation uint64) error {
	for {
		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		case decision := <-t.decisions:
			if decision.Action == workstore.ActionLaunch && decision.Attempt == 1 && decision.Generation == 1 {
				continue
			}
			if decision.Action != workstore.ActionLaunch || decision.Attempt != attempt || decision.Generation != generation {
				return fmt.Errorf("unexpected supervisor replacement decision: %+v", decision)
			}
			return nil
		}
	}
}

func (t *codexTrial) waitForEffects(count int) error {
	arrivals, err := waitForEffectArrivals(t.ctx, count,
		func(ctx context.Context) ([]failureinject.Arrival, error) {
			return t.effectBarrier.WaitForArrivals(ctx, count)
		},
		func(ctx context.Context) error { return t.workflowRun.Get(ctx, nil) },
		func() ([]failureinject.Arrival, error) { return readFileBarrierArrivals(t.effectBarrier.directory) },
	)
	if err != nil {
		return err
	}
	t.recordArrivals(arrivals)
	if t.authorityStore != nil {
		snapshot, err := t.authorityStore.Snapshot(t.ctx, t.logicalSessionID)
		if err != nil {
			return err
		}
		if len(snapshot.Effects) != count {
			return fmt.Errorf("authority effects=%d want=%d", len(snapshot.Effects), count)
		}
		return nil
	}
	destination, err := ReadDestination(t.ctx, t.destinationPath)
	if err != nil {
		return err
	}
	workspace, err := ReadWorkspaceEffects(t.workspacePath)
	if err != nil {
		return err
	}
	if len(destination.Attempts) != count || len(workspace) != count {
		return fmt.Errorf("effect receipts destination=%d workspace=%d want=%d", len(destination.Attempts), len(workspace), count)
	}
	return nil
}

type effectArrivalResult struct {
	arrivals []failureinject.Arrival
	err      error
}

func waitForEffectArrivals(ctx context.Context, count int,
	waitEffects func(context.Context) ([]failureinject.Arrival, error),
	waitWorkflow func(context.Context) error,
	inspectEffects func() ([]failureinject.Arrival, error),
) ([]failureinject.Arrival, error) {
	if count < 1 || waitEffects == nil || waitWorkflow == nil || inspectEffects == nil {
		return nil, errors.New("effect wait requires a positive count and complete observers")
	}
	waitContext, cancel := context.WithCancel(ctx)
	defer cancel()
	effects := make(chan effectArrivalResult, 1)
	workflow := make(chan error, 1)
	go func() {
		arrivals, err := waitEffects(waitContext)
		effects <- effectArrivalResult{arrivals: arrivals, err: err}
	}()
	go func() { workflow <- waitWorkflow(waitContext) }()
	select {
	case result := <-effects:
		return result.arrivals, result.err
	case workflowErr := <-workflow:
		arrivals, inspectErr := inspectEffects()
		if inspectErr != nil {
			return nil, inspectErr
		}
		if len(arrivals) >= count {
			return arrivals, nil
		}
		if workflowErr != nil {
			return nil, fmt.Errorf("%w: %v", errWorkflowClosedBeforeEffect, workflowErr)
		}
		return nil, errWorkflowClosedBeforeEffect
	}
}

func (t *codexTrial) waitForBoundary(point string, count int) error {
	if point == committedEffectBarrier {
		arrivals, err := t.effectBarrier.WaitForArrivals(t.ctx, count)
		if err == nil {
			t.recordArrivals(arrivals)
		}
		return err
	}
	arrivals, err := t.coordinator.WaitForArrivals(t.ctx, point, count)
	if err == nil {
		t.recordArrivals(arrivals)
	}
	return err
}

func (t *codexTrial) releaseBoundary(point string) error {
	if point == committedEffectBarrier {
		return t.releaseEffectBarrier()
	}
	return t.coordinator.Release(point)
}

func (t *codexTrial) releaseEffectBarrier() error {
	if t.effectBarrier == nil {
		return errors.New("effect barrier is not configured")
	}
	return t.effectBarrier.Release()
}

func (t *codexTrial) recordArrivals(arrivals []failureinject.Arrival) {
	for _, arrival := range arrivals {
		if !slices.ContainsFunc(t.arrivals, func(existing failureinject.Arrival) bool {
			return existing.Point == arrival.Point && existing.ID == arrival.ID
		}) {
			t.arrivals = append(t.arrivals, arrival)
		}
	}
}

func (t *codexTrial) killFirstWorker() error {
	faultAt, err := t.workerOne.killAndWait()
	if err == nil {
		t.faultAt = faultAt
	}
	return err
}

func (t *codexTrial) startSecondWorker() error {
	t.workerConfig.WorkerID = "worker-two"
	worker, err := startWorkerProcess(t.workerConfig)
	if err == nil {
		t.workerTwo = worker
	}
	return err
}

func (t *codexTrial) replaceWorker() error {
	if err := t.killFirstWorker(); err != nil {
		return err
	}
	return t.startSecondWorker()
}

func (t *codexTrial) awaitWorkflow() error {
	if err := appendTrialProgress(t.staging, "await-workflow-entered", ""); err != nil {
		return err
	}
	closeEvent, err := t.awaitWorkflowCloseHistory()
	if err != nil {
		return err
	}
	if err := appendTrialProgress(t.staging, "workflow-close-history-read", closeEvent.GetEventType().String()); err != nil {
		return err
	}
	t.result, err = decodeCompletedCodexWorkflowResult(closeEvent)
	if err != nil {
		return err
	}
	if err := appendTrialProgress(t.staging, "workflow-result-decoded", ""); err != nil {
		return err
	}
	worker := t.workerOne
	if t.workerTwo != nil {
		worker = t.workerTwo
	}
	shutdown, cancel := context.WithTimeout(t.ctx, 5*time.Second)
	defer cancel()
	if err := appendTrialProgress(t.staging, "worker-stop-started", t.workerConfig.WorkerID); err != nil {
		return err
	}
	if err := worker.stop(shutdown); err != nil {
		return err
	}
	return appendTrialProgress(t.staging, "worker-stop-completed", t.workerConfig.WorkerID)
}

func (t *codexTrial) awaitCanceledWorkflow() error {
	if err := appendTrialProgress(t.staging, "await-canceled-workflow-entered", ""); err != nil {
		return err
	}
	closeEvent, err := t.awaitWorkflowCloseHistory()
	if err != nil {
		return err
	}
	if err := validateCanceledCodexWorkflowClose(closeEvent); err != nil {
		return err
	}
	if err := appendTrialProgress(t.staging, "canceled-workflow-close-history-read", closeEvent.GetEventType().String()); err != nil {
		return err
	}
	shutdown, cancel := context.WithTimeout(t.ctx, 5*time.Second)
	defer cancel()
	return t.workerOne.stop(shutdown)
}

func (t *codexTrial) awaitWorkflowCloseHistory() (*historypb.HistoryEvent, error) {
	for {
		queryContext, cancel := context.WithTimeout(t.ctx, 5*time.Second)
		statusClient, err := client.DialContext(queryContext, client.Options{
			HostPort: t.address, Namespace: "default", Identity: "codex-experiment-result-reader",
		})
		if err != nil {
			cancel()
			if !isWorkflowQueryRetryable(err) {
				return nil, fmt.Errorf("connect Codex Workflow status reader: %w", err)
			}
		} else {
			description, describeErr := statusClient.DescribeWorkflowExecution(
				queryContext, t.workflowID, t.workflowRun.GetRunID(),
			)
			statusClient.Close()
			cancel()
			if describeErr != nil {
				if !isWorkflowQueryRetryable(describeErr) {
					return nil, fmt.Errorf("describe Codex Workflow: %w", describeErr)
				}
			} else {
				if description.WorkflowExecutionInfo == nil {
					return nil, errors.New("workflow description is missing execution info")
				}
				if err := appendTrialProgress(t.staging, "workflow-status-observed",
					description.WorkflowExecutionInfo.Status.String()); err != nil {
					return nil, err
				}
				if isCodexWorkflowClosedStatus(description.WorkflowExecutionInfo.Status) {
					event, historyErr := t.readClosedWorkflowHistory()
					if historyErr == nil {
						return event, nil
					}
					if !isWorkflowQueryRetryable(historyErr) {
						return nil, historyErr
					}
				}
			}
		}
		if err := waitForNextCloseHistoryQuery(t.ctx, closeHistoryQueryInterval); err != nil {
			return nil, err
		}
	}
}

func isWorkflowQueryRetryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func (t *codexTrial) readClosedWorkflowHistory() (*historypb.HistoryEvent, error) {
	if err := appendTrialProgress(t.staging, "workflow-close-history-read-started", ""); err != nil {
		return nil, err
	}
	queryContext, cancel := context.WithTimeout(t.ctx, 5*time.Second)
	defer cancel()
	resultClient, err := client.DialContext(queryContext, client.Options{
		HostPort: t.address, Namespace: "default", Identity: "codex-experiment-result-reader",
	})
	if err != nil {
		return nil, fmt.Errorf("connect Codex Workflow history reader: %w", err)
	}
	defer resultClient.Close()
	event, err := readWorkflowCloseHistory(
		queryContext, resultClient, t.workflowID, t.workflowRun.GetRunID(),
	)
	if err != nil {
		return nil, err
	}
	if event == nil {
		return nil, errors.New("closed Codex Workflow has no terminal history event")
	}
	return event, nil
}

func waitForNextCloseHistoryQuery(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (t *codexTrial) cleanup(runErr *error) {
	if t.effectBarrier != nil {
		*runErr = errors.Join(*runErr, t.effectBarrier.Release())
	}
	if t.coordinator != nil {
		for _, point := range []string{claimBeforeExecBarrier, preThreadBarrier, threadRegistrationBarrier, finalOutputBarrier} {
			err := t.coordinator.Release(point)
			if err != nil && !errors.Is(err, failureinject.ErrBarrierNotFound) {
				*runErr = errors.Join(*runErr, err)
			}
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if t.workerTwo != nil {
		*runErr = errors.Join(*runErr, t.workerTwo.stop(shutdown))
	}
	if t.workerOne != nil {
		*runErr = errors.Join(*runErr, t.workerOne.stop(shutdown))
	}
	if t.supervisor != nil {
		executions := t.supervisor.beginShutdown()
		if t.supervisorCancel != nil {
			t.supervisorCancel()
		}
		waitContext, waitCancel := context.WithTimeout(context.Background(), supervisedTerminationGrace+time.Second)
		*runErr = errors.Join(*runErr, waitForSupervisorExecutions(waitContext, executions))
		waitCancel()
	} else if t.supervisorCancel != nil {
		t.supervisorCancel()
	}
	if t.supervisorServer != nil {
		t.supervisorServer.CloseClientConnections()
		t.supervisorServer.Close()
	}
	if t.barrierServer != nil {
		t.barrierServer.CloseClientConnections()
		t.barrierServer.Close()
	}
}
