package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type Scenario string

const (
	ScenarioAutoCompatible   Scenario = "auto-compatible"
	ScenarioPinnedCompatible Scenario = "pinned-compatible"
	ScenarioAutoIncompatible Scenario = "auto-incompatible"
)

const trialsPerScenario = 3

type ScenarioResult struct {
	Scenario             Scenario          `json:"scenario"`
	Trial                int               `json:"trial"`
	WorkflowID           string            `json:"workflow_id"`
	RunID                string            `json:"run_id"`
	WorkflowResult       WorkflowResult    `json:"workflow_result"`
	ActivityReceipts     []ActivityReceipt `json:"activity_receipts"`
	Registry             AgentRecord       `json:"registry"`
	HistoryWorkerBuilds  []string          `json:"history_worker_builds"`
	ExpectedFailure      bool              `json:"expected_failure"`
	IncompatibleRejected bool              `json:"incompatible_rejected"`
	History              *history.History  `json:"-"`
}

type ExperimentResult struct {
	Environment                  Environment      `json:"environment"`
	Scenarios                    []ScenarioResult `json:"scenarios"`
	CompatibleHistoriesReplay    bool             `json:"compatible_histories_replay"`
	IncompatibleWorkflowRejected bool             `json:"incompatible_workflow_rejected"`
}

type RunOptions struct {
	Client      client.Client
	Root        string
	Environment Environment
}

type Environment struct {
	CapturedAt       time.Time `json:"captured_at"`
	GoVersion        string    `json:"go_version"`
	SDKVersion       string    `json:"sdk_version"`
	TemporalCLI      string    `json:"temporal_cli"`
	ExecutableSHA256 string    `json:"executable_sha256"`
	RunLabel         string    `json:"run_label"`
	OS               string    `json:"os"`
	Architecture     string    `json:"architecture"`
}

func RunExperiment(ctx context.Context, options RunOptions) (ExperimentResult, error) {
	if options.Client == nil || strings.TrimSpace(options.Root) == "" || filepath.IsAbs(options.Root) || unsafeRegistryPath(options.Root) {
		return ExperimentResult{}, errors.New("client and relative root are required")
	}
	if err := os.Mkdir(options.Root, 0o750); err != nil {
		return ExperimentResult{}, fmt.Errorf("create experiment root: %w", err)
	}
	return runAndPreserve(ctx, options)
}

func runAndPreserve(ctx context.Context, options RunOptions) (ExperimentResult, error) {
	result := ExperimentResult{Environment: options.Environment, CompatibleHistoriesReplay: true, IncompatibleWorkflowRejected: true}
	result.Environment.RunLabel = filepath.Base(options.Root)
	for _, scenario := range []Scenario{ScenarioAutoCompatible, ScenarioPinnedCompatible, ScenarioAutoIncompatible} {
		for trial := 1; trial <= trialsPerScenario; trial++ {
			scenarioResult, err := runScenario(ctx, options, scenario, trial)
			if err != nil {
				runErr := fmt.Errorf("run %s trial %d: %w", scenario, trial, err)
				preserveFailure(options.Root, runErr)
				return ExperimentResult{}, runErr
			}
			if err := replayCompatible(scenarioResult.History); err != nil {
				result.CompatibleHistoriesReplay = false
				runErr := fmt.Errorf("replay %s trial %d: %w", scenario, trial, err)
				preserveFailure(options.Root, runErr)
				return ExperimentResult{}, runErr
			}
			result.Scenarios = append(result.Scenarios, scenarioResult)
		}
	}
	if err := replayIncompatible(result.Scenarios[0].History); err == nil {
		result.IncompatibleWorkflowRejected = false
		runErr := errors.New("deliberately incompatible Workflow replay succeeded")
		preserveFailure(options.Root, runErr)
		return ExperimentResult{}, runErr
	}
	if err := preserveExperiment(options.Root, result); err != nil {
		preserveFailure(options.Root, err)
		return ExperimentResult{}, fmt.Errorf("preserve experiment: %w", err)
	}
	return result, nil
}

func runScenario(ctx context.Context, options RunOptions, scenario Scenario, trial int) (ScenarioResult, error) {
	name := strings.ReplaceAll(fmt.Sprintf("%s-%s-trial-%d", filepath.Base(options.Root), scenario, trial), "_", "-")
	taskQueue, deployment := "worker-versioning-"+name, "agent-session-"+name
	registryPath := filepath.Join(options.Root, evidencePrefix(scenario, trial)+"-registry.db")
	behavior := workflow.VersioningBehaviorAutoUpgrade
	if scenario == ScenarioPinnedCompatible {
		behavior = workflow.VersioningBehaviorPinned
	}

	v1, err := startVersionedWorker(options.Client, taskQueue, deployment, "worker-v1", "agent-v1", []string{"agent-v1"}, behavior)
	if err != nil {
		return ScenarioResult{}, err
	}
	defer v1.Stop()
	if err := makeCurrent(ctx, options.Client, deployment, "worker-v1"); err != nil {
		return ScenarioResult{}, err
	}

	workflowID := name
	run, err := options.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID, TaskQueue: taskQueue}, WorkflowName,
		WorkflowInput{SessionID: "session-1", RegistryPath: registryPath, Phases: 2})
	if err != nil {
		return ScenarioResult{}, fmt.Errorf("start workflow: %w", err)
	}
	if err := waitForRegistry(ctx, registryPath); err != nil {
		return ScenarioResult{}, err
	}

	nextBuild, nextAgent, compatible := "worker-v2", "agent-v2", []string{"agent-v1", "agent-v2"}
	if scenario == ScenarioAutoIncompatible {
		nextBuild, nextAgent, compatible = "worker-v3", "agent-v3", []string{"agent-v3"}
	}
	next, err := startVersionedWorker(options.Client, taskQueue, deployment, nextBuild, nextAgent, compatible, behavior)
	if err != nil {
		return ScenarioResult{}, err
	}
	defer next.Stop()
	if err := makeCurrent(ctx, options.Client, deployment, nextBuild); err != nil {
		return ScenarioResult{}, err
	}
	if scenario != ScenarioPinnedCompatible {
		v1.Stop()
	}
	if err := options.Client.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), continueSignal, struct{}{}); err != nil {
		return ScenarioResult{}, fmt.Errorf("signal workflow: %w", err)
	}

	var workflowResult WorkflowResult
	workflowErr := run.Get(ctx, &workflowResult)
	expectedFailure := scenario == ScenarioAutoIncompatible
	if expectedFailure == (workflowErr == nil) {
		return ScenarioResult{}, fmt.Errorf("workflow failure=%v, expected failure=%v", workflowErr, expectedFailure)
	}
	if expectedFailure && !strings.Contains(workflowErr.Error(), "incompatible detached-agent build") {
		return ScenarioResult{}, fmt.Errorf("unexpected incompatible failure: %w", workflowErr)
	}
	record, err := (Registry{Path: registryPath}).Read()
	if err != nil {
		return ScenarioResult{}, err
	}
	if err := validateScenario(scenario, workflowResult, record); err != nil {
		return ScenarioResult{}, err
	}
	historyValue, err := readHistory(ctx, options.Client, run.GetID(), run.GetRunID())
	if err != nil {
		return ScenarioResult{}, err
	}
	historyBuilds := historyWorkerBuilds(historyValue)
	if err := validateHistoryBuilds(scenario, historyBuilds); err != nil {
		return ScenarioResult{}, err
	}
	activityReceipts, err := inspectActivityHistory(historyValue, scenario, run.GetID(), run.GetRunID(), registryPath, expectedFailure)
	if err != nil {
		return ScenarioResult{}, err
	}
	if !expectedFailure && !slices.EqualFunc(activityReceipts, workflowResult.Receipts, func(left, right ActivityReceipt) bool {
		return left == right
	}) {
		return ScenarioResult{}, errors.New("workflow result and history activity receipts disagree")
	}
	return ScenarioResult{
		Scenario: scenario, Trial: trial, WorkflowID: run.GetID(), RunID: run.GetRunID(), WorkflowResult: workflowResult,
		ActivityReceipts: activityReceipts,
		Registry:         record, HistoryWorkerBuilds: historyBuilds,
		ExpectedFailure: expectedFailure, IncompatibleRejected: expectedFailure, History: historyValue,
	}, nil
}

func evidencePrefix(scenario Scenario, trial int) string {
	return fmt.Sprintf("%s-trial-%d", scenario, trial)
}

func startVersionedWorker(temporalClient client.Client, taskQueue, deployment, workerBuild, agentBuild string, compatible []string, behavior workflow.VersioningBehavior) (worker.Worker, error) {
	versionedWorker := worker.New(temporalClient, taskQueue, worker.Options{
		Identity:          workerBuild,
		DeploymentOptions: worker.DeploymentOptions{UseVersioning: true, Version: worker.WorkerDeploymentVersion{DeploymentName: deployment, BuildID: workerBuild}},
	})
	versionedWorker.RegisterWorkflowWithOptions(CompatibleWorkflow, workflow.RegisterOptions{Name: WorkflowName, VersioningBehavior: behavior})
	versionedWorker.RegisterActivityWithOptions(VersionedActivities{
		WorkerBuild: workerBuild, AgentBuild: agentBuild, CompatibleAgentBuilds: compatible,
	}.Attach, activityRegistrationOptions())
	if err := versionedWorker.Start(); err != nil {
		return nil, fmt.Errorf("start worker %s: %w", workerBuild, err)
	}
	return versionedWorker, nil
}

func makeCurrent(ctx context.Context, temporalClient client.Client, deployment, build string) error {
	handle := temporalClient.WorkerDeploymentClient().GetHandle(deployment)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		description, err := handle.Describe(ctx, client.WorkerDeploymentDescribeOptions{})
		lastErr = err
		if err == nil {
			for _, summary := range description.Info.VersionSummaries {
				if summary.Version.BuildID == build {
					_, err = handle.SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{BuildID: build})
					if err == nil {
						return nil
					}
					lastErr = err
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for deployment version %s: %w (last service error: %v)", build, ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func waitForRegistry(ctx context.Context, path string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := (Registry{Path: path}).Read(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for phase-one registry: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func validateScenario(scenario Scenario, result WorkflowResult, record AgentRecord) error {
	if record.AgentBuild != "agent-v1" {
		return fmt.Errorf("stored agent build = %q", record.AgentBuild)
	}
	if scenario == ScenarioAutoIncompatible {
		if len(result.WorkflowBuilds) != 0 || len(result.Receipts) != 0 {
			return errors.New("incompatible Workflow returned fabricated success data")
		}
		if len(record.Attachments) != 0 {
			return fmt.Errorf("incompatible attachment mutated registry: %+v", record.Attachments)
		}
		return nil
	}
	if len(result.Receipts) != 2 || len(record.Attachments) != 1 {
		return fmt.Errorf("compatible result receipts=%d attachments=%d", len(result.Receipts), len(record.Attachments))
	}
	wantWorker := "worker-v2"
	wantWorkflowBuilds := []string{"worker-v1", "worker-v2"}
	if scenario == ScenarioPinnedCompatible {
		wantWorker = "worker-v1"
		wantWorkflowBuilds = []string{"worker-v1", "worker-v1"}
	}
	if !slices.Equal(result.WorkflowBuilds, wantWorkflowBuilds) {
		return fmt.Errorf("workflow build observations = %v, want %v", result.WorkflowBuilds, wantWorkflowBuilds)
	}
	if result.Receipts[1].WorkerBuild != wantWorker || result.Receipts[1].Action != ActionAttached {
		return fmt.Errorf("phase-two receipt = %+v, want worker %s attached", result.Receipts[1], wantWorker)
	}
	if err := validateRegistryReceipts(record, result.Receipts); err != nil {
		return err
	}
	return nil
}

func validateRegistryReceipts(record AgentRecord, receipts []ActivityReceipt) error {
	if len(receipts) == 0 || record.SessionID != receipts[0].SessionID || record.StartedByWorker != receipts[0].WorkerBuild || record.AgentBuild != receipts[0].AgentBuild {
		return errors.New("registry start identity differs from the first Activity receipt")
	}
	if len(receipts) != len(record.Attachments)+1 {
		return errors.New("registry attachment count differs from Activity receipts")
	}
	for index, attachment := range record.Attachments {
		if attachment.WorkerBuild != receipts[index+1].WorkerBuild {
			return errors.New("registry attachment Worker differs from Activity receipt")
		}
	}
	return nil
}

func historyWorkerBuilds(historyValue *history.History) []string {
	var builds []string
	for _, event := range historyValue.Events {
		attributes := event.GetWorkflowTaskCompletedEventAttributes()
		if attributes == nil || attributes.GetDeploymentVersion() == nil {
			continue
		}
		build := attributes.GetDeploymentVersion().GetBuildId()
		if build != "" && (len(builds) == 0 || builds[len(builds)-1] != build) {
			builds = append(builds, build)
		}
	}
	return builds
}

func validateHistoryBuilds(scenario Scenario, builds []string) error {
	want := []string{"worker-v1", "worker-v2"}
	if scenario == ScenarioPinnedCompatible {
		want = []string{"worker-v1"}
	} else if scenario == ScenarioAutoIncompatible {
		want = []string{"worker-v1", "worker-v3"}
	}
	if !slices.Equal(builds, want) {
		return fmt.Errorf("history worker builds = %v, want %v", builds, want)
	}
	return nil
}

func readHistory(ctx context.Context, temporalClient client.Client, workflowID, runID string) (*history.History, error) {
	iterator := temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	value := &history.History{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, fmt.Errorf("read history: %w", err)
		}
		value.Events = append(value.Events, event)
	}
	return value, nil
}

func replayCompatible(historyValue *history.History) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(CompatibleWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	return replayer.ReplayWorkflowHistory(nil, historyValue)
}

func replayIncompatible(historyValue *history.History) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(incompatibleWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	return replayer.ReplayWorkflowHistory(nil, historyValue)
}

func incompatibleWorkflow(ctx workflow.Context, input WorkflowInput) (WorkflowResult, error) {
	if err := workflow.NewTimer(ctx, time.Second).Get(ctx, nil); err != nil {
		return WorkflowResult{}, err
	}
	return CompatibleWorkflow(ctx, input)
}
