package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"go.temporal.io/sdk/client"
)

const committedEffectBarrier = "claude-tool-effect-committed"

type trialSummary struct {
	WorkflowID          string                  `json:"workflow_id"`
	WorkflowRunID       string                  `json:"workflow_run_id"`
	Probe               protocol.Probe          `json:"probe"`
	FaultBoundary       FaultBoundary           `json:"fault_boundary"`
	Trial               int                     `json:"trial"`
	FaultAt             time.Time               `json:"fault_at,omitempty"`
	BarrierArrivals     []failureinject.Arrival `json:"barrier_arrivals"`
	WorkflowResult      ClaudeActivityResult    `json:"workflow_result"`
	WorkspaceBeforeHash string                  `json:"workspace_before_sha256"`
	WorkspaceAfterHash  string                  `json:"workspace_after_sha256"`
	WorkspaceEffects    []WorkspaceEffect       `json:"workspace_effects"`
	Destination         DestinationSnapshot     `json:"destination"`
}

type claudeTrial struct {
	ctx                  context.Context
	temporalClient       client.Client
	temporalAddress      string
	options              ExperimentOptions
	metadata             experimentMetadata
	probe                protocol.Probe
	faultBoundary        FaultBoundary
	trial                int
	staging              string
	startedAt            time.Time
	fixtureDirectory     string
	workspacePath        string
	workspaceBeforeHash  string
	destinationPath      string
	attemptRoot          string
	coordinator          *failureinject.Coordinator
	barrierServer        *httptest.Server
	workerConfig         workerProcessConfig
	workerOne            *managedWorker
	workerTwo            *managedWorker
	logicalSessionID     string
	workflowID           string
	workflowRun          client.WorkflowRun
	arrivals             []failureinject.Arrival
	faultAt              time.Time
	faultReadyAt         time.Time
	faultActorID         string
	faultProcessIdentity string
	workflowResult       ClaudeActivityResult
}

func runClaudeTrial(
	ctx context.Context,
	temporalClient client.Client,
	temporalAddress string,
	options ExperimentOptions,
	metadata experimentMetadata,
	probe protocol.Probe,
	faultBoundary FaultBoundary,
	trial int,
) (runDirectory string, runErr error) {
	state, err := newClaudeTrial(ctx, temporalClient, temporalAddress, options, metadata, probe, faultBoundary, trial)
	if err != nil {
		return "", err
	}
	defer state.cleanup(&runErr)
	if err := state.execute(); err != nil {
		return "", err
	}
	return state.publish()
}

func newClaudeTrial(ctx context.Context, temporalClient client.Client, temporalAddress string,
	options ExperimentOptions, metadata experimentMetadata, probe protocol.Probe, faultBoundary FaultBoundary, trial int,
) (*claudeTrial, error) {
	runID := fmt.Sprintf("claude-direct-ambiguous-effect-%s-%s-trial-%d", probe, faultBoundary, trial)
	state := &claudeTrial{
		ctx: ctx, temporalClient: temporalClient, temporalAddress: temporalAddress,
		options: options, metadata: metadata, probe: probe, faultBoundary: faultBoundary, trial: trial,
		staging: filepath.Join(options.EvidenceRoot, ".staging-"+runID), startedAt: time.Now().UTC(),
	}
	if err := state.prepareStorage(); err != nil {
		return nil, err
	}
	state.prepareCoordination()
	return state, nil
}

func (t *claudeTrial) prepareStorage() error {
	if err := os.Mkdir(t.staging, 0o750); err != nil {
		return fmt.Errorf("create trial staging directory: %w", err)
	}
	t.fixtureDirectory = filepath.Join(t.staging, "fixture")
	if err := prepareFixture(t.ctx, t.fixtureDirectory); err != nil {
		return err
	}
	t.workspacePath = filepath.Join(t.fixtureDirectory, "effects.jsonl")
	hash, err := hashWorkspace(t.fixtureDirectory)
	if err != nil {
		return err
	}
	t.workspaceBeforeHash = hash
	t.destinationPath = filepath.Join(t.staging, "destination.db")
	t.attemptRoot = filepath.Join(t.staging, "attempts")
	return nil
}

func (t *claudeTrial) prepareCoordination() {
	t.coordinator = failureinject.NewCoordinator()
	t.barrierServer = httptest.NewServer(t.coordinator.Handler())
	taskQueue := fmt.Sprintf("claude-direct-%s-%s-trial-%d", t.probe, t.faultBoundary, t.trial)
	t.workerConfig = workerProcessConfig{
		Binary: t.options.WorkerBinary, Directory: t.staging, TemporalAddress: t.temporalAddress,
		TaskQueue: taskQueue, ClaudeBinary: t.options.ClaudeBinary, LauncherBinary: t.options.LauncherBinary,
		FaultBoundary: t.faultBoundary, EffectBinary: t.options.EffectBinary,
		FixtureDirectory: t.fixtureDirectory, DestinationPath: t.destinationPath, WorkspacePath: t.workspacePath,
		BarrierURL: t.barrierServer.URL, BarrierPoint: committedEffectBarrier, RunRoot: t.attemptRoot,
		Model: t.options.Model, MaxBudgetUSD: t.options.MaxBudgetUSD, MaxTurns: t.options.MaxTurns,
	}
}

func (t *claudeTrial) execute() error {
	if err := t.startFirstWorkerAndWorkflow(); err != nil {
		return err
	}
	if t.probe == protocol.ProbeUnfaulted {
		return t.executeUnfaulted()
	}
	switch t.faultBoundary {
	case FaultBeforeVendorRegistration:
		return t.executePreRegistrationFault()
	case FaultAfterToolEffect:
		return t.executeEffectFault()
	case FaultAfterFinalOutput:
		return t.executeFinalOutputFault()
	default:
		return fmt.Errorf("unsupported unsafe fault boundary %q", t.faultBoundary)
	}
}

func (t *claudeTrial) executeUnfaulted() error {
	if err := t.waitForEffects(1); err != nil {
		return err
	}
	if err := t.coordinator.Release(committedEffectBarrier); err != nil {
		return err
	}
	return t.awaitWorkflow()
}

func (t *claudeTrial) executeEffectFault() error {
	if err := t.waitForEffects(1); err != nil {
		return err
	}
	if err := t.replaceWorker(); err != nil {
		return err
	}
	if err := t.waitForEffects(2); err != nil {
		return err
	}
	if err := t.coordinator.Release(committedEffectBarrier); err != nil {
		return err
	}
	return t.awaitWorkflow()
}

func (t *claudeTrial) executePreRegistrationFault() error {
	if err := t.waitForBoundary(preRegistrationBarrier, 1); err != nil {
		return err
	}
	if err := t.killFirstWorker(); err != nil {
		return err
	}
	if err := t.coordinator.Release(preRegistrationBarrier); err != nil {
		return err
	}
	if err := t.waitForEffects(1); err != nil {
		return err
	}
	if err := t.startSecondWorker(); err != nil {
		return err
	}
	if err := t.waitForEffects(2); err != nil {
		return err
	}
	if err := t.coordinator.Release(committedEffectBarrier); err != nil {
		return err
	}
	return t.awaitWorkflow()
}

func (t *claudeTrial) executeFinalOutputFault() error {
	if err := t.waitForEffects(1); err != nil {
		return err
	}
	if err := t.coordinator.Release(committedEffectBarrier); err != nil {
		return err
	}
	if err := t.waitForBoundary(finalOutputBarrier, 1); err != nil {
		return err
	}
	if err := t.replaceWorker(); err != nil {
		return err
	}
	if err := t.waitForBoundary(finalOutputBarrier, 2); err != nil {
		return err
	}
	if err := t.waitForEffects(2); err != nil {
		return err
	}
	if err := t.coordinator.Release(finalOutputBarrier); err != nil {
		return err
	}
	return t.awaitWorkflow()
}

func (t *claudeTrial) startFirstWorkerAndWorkflow() error {
	t.workerConfig.WorkerID = "worker-one"
	worker, err := startWorkerProcess(t.workerConfig)
	if err != nil {
		return err
	}
	t.workerOne = worker
	t.logicalSessionID = fmt.Sprintf("claude-direct-%s-%s-trial-%d", t.probe, t.faultBoundary, t.trial)
	t.workflowID = "claude-direct/" + t.logicalSessionID
	t.workflowRun, err = t.temporalClient.ExecuteWorkflow(t.ctx, client.StartWorkflowOptions{
		ID: t.workflowID, TaskQueue: t.workerConfig.TaskQueue, WorkflowExecutionTimeout: t.options.Timeout,
	}, DirectClaudeWorkflowName, ClaudeActivityInput{
		LogicalSessionID: t.logicalSessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
	})
	if err != nil {
		return fmt.Errorf("start Claude direct Workflow: %w", err)
	}
	return nil
}

func (t *claudeTrial) waitForEffects(count int) error {
	arrivals, err := t.coordinator.WaitForArrivals(t.ctx, committedEffectBarrier, count)
	if err != nil {
		return fmt.Errorf("wait for committed effect %d: %w", count, err)
	}
	t.recordArrivals(arrivals)
	return verifyStateAtBarrier(t.ctx, t.destinationPath, t.workspacePath, count)
}

func (t *claudeTrial) killFirstWorker() error {
	t.faultReadyAt = time.Now().UTC()
	t.faultActorID = "worker-one"
	t.faultProcessIdentity = t.workerOne.processIdentity()
	_, killedAt, err := t.workerOne.killAndWait()
	if err != nil {
		return fmt.Errorf("inject Worker SIGKILL: %w", err)
	}
	t.faultAt = killedAt
	return nil
}

func (t *claudeTrial) startSecondWorker() error {
	t.workerConfig.WorkerID = "worker-two"
	worker, err := startWorkerProcess(t.workerConfig)
	if err != nil {
		return err
	}
	t.workerTwo = worker
	return nil
}

func (t *claudeTrial) replaceWorker() error {
	if err := t.killFirstWorker(); err != nil {
		return err
	}
	return t.startSecondWorker()
}

func (t *claudeTrial) waitForBoundary(point string, count int) error {
	arrivals, err := t.coordinator.WaitForArrivals(t.ctx, point, count)
	if err != nil {
		return fmt.Errorf("wait for %s arrival %d: %w", point, count, err)
	}
	t.recordArrivals(arrivals)
	return nil
}

func (t *claudeTrial) recordArrivals(arrivals []failureinject.Arrival) {
	for _, arrival := range arrivals {
		found := slices.ContainsFunc(t.arrivals, func(existing failureinject.Arrival) bool {
			return existing.Point == arrival.Point && existing.ID == arrival.ID
		})
		if !found {
			t.arrivals = append(t.arrivals, arrival)
		}
	}
}

func (t *claudeTrial) awaitWorkflow() error {
	if err := t.workflowRun.Get(t.ctx, &t.workflowResult); err != nil {
		return fmt.Errorf("wait for Claude direct Workflow: %w", err)
	}
	worker := t.workerOne
	if t.workerTwo != nil {
		worker = t.workerTwo
	}
	shutdown, cancel := context.WithTimeout(t.ctx, 5*time.Second)
	defer cancel()
	return worker.stop(shutdown)
}

type collectedTrial struct {
	destination        DestinationSnapshot
	attempts           []ClaudeAttemptCapture
	workspaceEffects   []WorkspaceEffect
	workspaceAfterHash string
	status             []byte
	history            []byte
}

func (t *claudeTrial) collect() (collectedTrial, error) {
	var output collectedTrial
	destination, err := ReadDestination(t.ctx, t.destinationPath)
	if err != nil {
		return output, err
	}
	output.destination = destination
	output.attempts, err = collectClaudeAttempts(t.ctx, t.attemptRoot, destination)
	if err != nil {
		return output, err
	}
	if err := t.verifyAdmission(output.attempts); err != nil {
		return output, err
	}
	output.workspaceEffects, err = ReadWorkspaceEffects(t.workspacePath)
	if err != nil {
		return output, err
	}
	output.workspaceAfterHash, err = hashWorkspace(t.fixtureDirectory)
	if err != nil {
		return output, err
	}
	output.status, err = workspaceStatus(t.ctx, t.fixtureDirectory)
	if err != nil {
		return output, err
	}
	output.history, err = exportWorkflowHistory(t.ctx, t.temporalClient, t.workflowID, t.workflowRun.GetRunID())
	return output, err
}

func (t *claudeTrial) verifyAdmission(attempts []ClaudeAttemptCapture) error {
	want := 1
	if t.probe == protocol.ProbeUnsafe {
		want = 2
	}
	if len(attempts) != want || t.workflowResult.Result != "EFFECT_COMPLETE" {
		return fmt.Errorf("trial admission failed: attempts=%d result=%q", len(attempts), t.workflowResult.Result)
	}
	return nil
}

func (t *claudeTrial) publish() (string, error) {
	collected, err := t.collect()
	if err != nil {
		return "", err
	}
	rawHash, err := writeTrialRawArtifacts(t.staging, collected.history, collected.status, t.summary(collected))
	if err != nil {
		return "", err
	}
	capture := t.buildCapture(collected, rawHash)
	bundle, err := BuildEvidenceBundle(capture)
	if err != nil {
		return "", err
	}
	runDirectory, err := evidence.WriteRun(t.ctx, t.options.EvidenceRoot, bundle)
	if err != nil {
		return "", err
	}
	if err := t.publishRaw(runDirectory, rawHash); err != nil {
		return runDirectory, err
	}
	verdict, err := oracle.EvaluateAndWrite(t.ctx, runDirectory)
	if err != nil {
		return runDirectory, err
	}
	return runDirectory, verifyTrialVerdict(t.probe, verdict)
}

func (t *claudeTrial) buildCapture(collected collectedTrial, rawInventoryHash string) EvidenceCapture {
	native := []NativeCapture{
		{Kind: "temporal-workflow", Detail: t.workflowID + "/" + t.workflowRun.GetRunID()},
		{Kind: "workflow-result", Detail: t.workflowResult.Result},
		{Kind: "workspace-status", Detail: strings.TrimSpace(string(collected.status))},
	}
	for _, attempt := range collected.attempts {
		native = append(native, NativeCapture{
			Kind:   "claude-session",
			Detail: fmt.Sprintf("attempt=%d session=%s process=%s", attempt.TemporalAttempt, attempt.VendorSessionID, attempt.ProcessIdentity),
		})
	}
	native = append(native, NativeCapture{Kind: "raw-evidence-inventory", Detail: rawInventoryHash})
	capture := EvidenceCapture{
		AdapterVersion:     "worker-sha256:" + t.metadata.WorkerSHA256,
		ClaudeBinarySHA256: t.metadata.ClaudeSHA256, ClaudeVersion: t.metadata.ClaudeVersion,
		Model: t.options.Model, Runtime: runtime.GOOS + "/" + runtime.GOARCH,
		Probe: t.probe, Trial: t.trial, LogicalSessionID: t.logicalSessionID,
		FaultBoundary: t.faultBoundary,
		LogicalTurnID: "turn-1", LogicalEffectID: "effect-1", DestinationID: "fixture-" + t.logicalSessionID,
		StartedAt: t.startedAt, Attempts: collected.attempts, FaultAt: t.faultAt, CompletedAt: time.Now().UTC(),
		Settings: map[string]string{
			"fault_selection": string(t.faultBoundary), "permission_mode": "dontAsk",
			"session_identity": "vendor-assigned-after-start", "resume_control": "none",
			"worker_binary_sha256": t.metadata.WorkerSHA256, "effect_binary_sha256": t.metadata.EffectSHA256,
			"launcher_binary_sha256": t.metadata.LauncherSHA256, "raw_inventory_sha256": rawInventoryHash,
			"workspace_before_sha256": t.workspaceBeforeHash, "workspace_after_sha256": collected.workspaceAfterHash,
			"workspace_effect_count": strconv.Itoa(len(collected.workspaceEffects)),
		},
		Native: native,
	}
	if t.probe == protocol.ProbeUnsafe {
		capture.Boundary = BoundaryCapture{
			Point: t.faultBoundary, ActorID: t.faultActorID,
			ProcessIdentity: t.faultProcessIdentity, ReachedAt: t.faultReadyAt,
		}
	}
	return capture
}

func (t *claudeTrial) publishRaw(runDirectory, inventoryHash string) error {
	if err := os.Rename(t.staging, filepath.Join(runDirectory, "raw")); err != nil {
		return fmt.Errorf("publish raw trial artifacts: %w", err)
	}
	return verifyRawInventory(filepath.Join(runDirectory, "raw"), inventoryHash)
}

func (t *claudeTrial) summary(collected collectedTrial) trialSummary {
	return trialSummary{
		WorkflowID: t.workflowID, WorkflowRunID: t.workflowRun.GetRunID(), Probe: t.probe,
		FaultBoundary: t.faultBoundary, Trial: t.trial,
		FaultAt: t.faultAt, BarrierArrivals: t.arrivals, WorkflowResult: t.workflowResult,
		WorkspaceBeforeHash: t.workspaceBeforeHash, WorkspaceAfterHash: collected.workspaceAfterHash,
		WorkspaceEffects: collected.workspaceEffects, Destination: collected.destination,
	}
}

func (t *claudeTrial) cleanup(runErr *error) {
	if t.coordinator != nil {
		for _, point := range []string{committedEffectBarrier, preRegistrationBarrier, finalOutputBarrier} {
			err := t.coordinator.Release(point)
			if err != nil && !errors.Is(err, failureinject.ErrBarrierNotFound) {
				*runErr = errors.Join(*runErr, fmt.Errorf("release cleanup barrier %s: %w", point, err))
			}
		}
	}
	if t.barrierServer != nil {
		t.barrierServer.Close()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if t.workerTwo != nil {
		*runErr = errors.Join(*runErr, t.workerTwo.stop(shutdown))
	}
	if t.workerOne != nil {
		*runErr = errors.Join(*runErr, t.workerOne.stop(shutdown))
	}
}

func verifyStateAtBarrier(ctx context.Context, destinationPath, workspacePath string, count int) error {
	destination, err := ReadDestination(ctx, destinationPath)
	if err != nil {
		return err
	}
	workspace, err := ReadWorkspaceEffects(workspacePath)
	if err != nil {
		return err
	}
	if len(destination.Attempts) != count || len(workspace) != count {
		return fmt.Errorf("exact barrier state mismatch: destination=%d workspace=%d want=%d", len(destination.Attempts), len(workspace), count)
	}
	return nil
}

func collectClaudeAttempts(
	ctx context.Context,
	attemptRoot string,
	destination DestinationSnapshot,
) ([]ClaudeAttemptCapture, error) {
	entries, err := os.ReadDir(attemptRoot)
	if err != nil {
		return nil, fmt.Errorf("read attempt artifacts: %w", err)
	}
	destinationByID := make(map[string]EffectAttempt, len(destination.Attempts))
	for _, attempt := range destination.Attempts {
		destinationByID[attempt.PhysicalAttemptID] = attempt
	}
	captures := make([]ClaudeAttemptCapture, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		capture, err := collectClaudeAttempt(ctx, attemptRoot, entry.Name(), destinationByID)
		if err != nil {
			return nil, err
		}
		captures = append(captures, capture)
	}
	sort.Slice(captures, func(left, right int) bool {
		return captures[left].TemporalAttempt < captures[right].TemporalAttempt
	})
	return captures, nil
}

func collectClaudeAttempt(ctx context.Context, attemptRoot, name string,
	destinationByID map[string]EffectAttempt,
) (ClaudeAttemptCapture, error) {
	directory := filepath.Join(attemptRoot, name)
	process, err := readJSONFile[ProcessRecord](filepath.Join(directory, name+".process-started.json"))
	if err != nil {
		return ClaudeAttemptCapture{}, err
	}
	if err := waitForProcessExit(ctx, process); err != nil {
		return ClaudeAttemptCapture{}, fmt.Errorf("wait for Claude process %s: %w", name, err)
	}
	request, err := ReadControlledEffectRequest(filepath.Join(directory, effectRequestFile))
	if err != nil {
		return ClaudeAttemptCapture{}, err
	}
	stream, err := readClaudeStream(filepath.Join(directory, name+".stdout.jsonl"))
	if err != nil {
		return ClaudeAttemptCapture{}, err
	}
	destinationAttempt, found := destinationByID[request.PhysicalAttemptID]
	if !found || process.ActorID != request.ActorID {
		return ClaudeAttemptCapture{}, fmt.Errorf("attempt %s lacks matching process or destination identity", name)
	}
	attemptNumber, err := parseAttemptNumber(name)
	if err != nil {
		return ClaudeAttemptCapture{}, err
	}
	return ClaudeAttemptCapture{
		TemporalAttempt: attemptNumber, ActorID: request.ActorID, ProcessIdentity: process.Identity,
		VendorSessionID: stream.SessionID, PhysicalAttemptID: request.PhysicalAttemptID,
		StartedAt: process.ObservedAt, AppliedAt: destinationAttempt.AppliedAt,
	}, nil
}

func readClaudeStream(path string) (ClaudeStreamResult, error) {
	stdout, err := os.Open(path)
	if err != nil {
		return ClaudeStreamResult{}, fmt.Errorf("open Claude stream: %w", err)
	}
	stream, parseErr := ParseClaudeStream(stdout)
	return stream, errors.Join(parseErr, stdout.Close())
}

func parseAttemptNumber(value string) (int32, error) {
	index := strings.LastIndex(value, "-attempt-")
	if index < 0 {
		return 0, fmt.Errorf("attempt artifact %q lacks Temporal attempt suffix", value)
	}
	number, err := strconv.ParseInt(value[index+len("-attempt-"):], 10, 32)
	if err != nil || number < 1 {
		return 0, fmt.Errorf("parse Temporal attempt from %q", value)
	}
	return int32(number), nil
}

func readJSONFile[T any](path string) (T, error) {
	var value T
	data, err := os.ReadFile(path)
	if err != nil {
		return value, fmt.Errorf("read %s: %w", filepath.Base(path), err)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	return value, nil
}

func verifyTrialVerdict(probe protocol.Probe, verdict protocol.Verdict) error {
	if probe == protocol.ProbeUnfaulted && verdict.Class == protocol.VerdictValidPass {
		return nil
	}
	if probe == protocol.ProbeUnsafe && verdict.Class == protocol.VerdictValidFail &&
		slices.Contains(verdict.ReasonCodes, protocol.ReasonDuplicateEffect) {
		return nil
	}
	return fmt.Errorf("unexpected %s verdict: %+v", probe, verdict)
}
