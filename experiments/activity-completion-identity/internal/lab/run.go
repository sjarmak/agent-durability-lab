package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	defaultRunTimeout = 25 * time.Second
	namespace         = "default"
)

type failureRecord struct {
	Time  time.Time `json:"time"`
	Error string    `json:"error"`
}

func Run(parent context.Context, options Options) (result Result, runErr error) {
	if err := validateOptions(options); err != nil {
		return Result{}, err
	}
	if options.Timeout <= 0 {
		options.Timeout = defaultRunTimeout
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()

	runDirectory := filepath.Join(options.OutputRoot, options.RunID)
	if err := os.MkdirAll(options.OutputRoot, 0o750); err != nil {
		return Result{}, fmt.Errorf("create evidence root: %w", err)
	}
	if err := os.Mkdir(runDirectory, 0o750); err != nil {
		return Result{}, fmt.Errorf("create append-only run directory: %w", err)
	}
	result.RunDirectory = runDirectory
	evidence := Evidence{Arm: options.Arm}
	defer func() {
		if runErr != nil {
			_ = writeJSON(filepath.Join(runDirectory, "observations.partial.json"), evidence)
			_ = writeJSON(filepath.Join(runDirectory, "failure.json"), failureRecord{
				Time: time.Now().UTC(), Error: runErr.Error(),
			})
		}
	}()

	startedAt := time.Now().UTC()
	serverLog, err := os.OpenFile(
		filepath.Join(runDirectory, "temporal-server.log"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600,
	)
	if err != nil {
		return result, fmt.Errorf("create Temporal server log: %w", err)
	}
	temporalServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath,
		DBFilename:   filepath.Join(runDirectory, "temporal.db"),
		LogLevel:     "warn",
		LogFormat:    "pretty",
		Stdout:       serverLog,
		Stderr:       serverLog,
	})
	if err != nil {
		_ = serverLog.Close()
		return result, fmt.Errorf("start Temporal dev server: %w", err)
	}
	defer func() {
		temporalServer.Client().Close()
		runErr = errors.Join(runErr, temporalServer.Stop(), serverLog.Close())
	}()

	fence, err := OpenAttemptFence(filepath.Join(runDirectory, "attempt-fence.db"))
	if err != nil {
		return result, err
	}
	defer func() { runErr = errors.Join(runErr, fence.Close()) }()

	attemptChannel := make(chan capturedAttempt, 2)
	taskQueue := "completion-identity-" + options.RunID
	temporalWorker := worker.New(temporalServer.Client(), taskQueue, worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(
		completionIdentityWorkflow,
		workflow.RegisterOptions{Name: workflowName},
	)
	temporalWorker.RegisterActivityWithOptions(
		attemptRecorder{fence: fence, attempts: attemptChannel}.Run,
		activity.RegisterOptions{Name: activityName},
	)
	if err := temporalWorker.Start(); err != nil {
		return result, fmt.Errorf("start experiment Worker: %w", err)
	}
	defer temporalWorker.Stop()

	workflowID := "activity-completion-identity/" + options.RunID
	workflowRun, err := temporalServer.Client().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue, WorkflowExecutionTimeout: options.Timeout,
	}, workflowName)
	if err != nil {
		return result, fmt.Errorf("start experiment Workflow: %w", err)
	}
	result.WorkflowID = workflowID
	result.WorkflowRunID = workflowRun.GetRunID()

	attempt1, attempt2, err := awaitAttempts(ctx, attemptChannel)
	if err != nil {
		return result, err
	}
	evidence.Attempts = []AttemptObservation{attempt1.observation, attempt2.observation}
	evidence.Completions = exerciseCompletionArm(
		ctx, options.Arm, temporalServer.Client(), fence, workflowID, workflowRun.GetRunID(), attempt1, attempt2,
	)
	if err := workflowRun.Get(ctx, &evidence.WorkflowOutcome); err != nil {
		return result, fmt.Errorf("wait for experiment Workflow: %w", err)
	}

	history, err := readHistory(ctx, temporalServer.Client(), workflowID, workflowRun.GetRunID())
	if err != nil {
		return result, err
	}
	evidence.History = summarizeHistory(history)
	result.Verdict = Verify(evidence)
	manifest := Manifest{
		SchemaVersion:   1,
		Experiment:      "activity-completion-identity",
		RunID:           options.RunID,
		Arm:             options.Arm,
		WorkflowID:      workflowID,
		WorkflowRunID:   workflowRun.GetRunID(),
		TaskQueue:       taskQueue,
		ActivityID:      activityID,
		StartedAt:       startedAt,
		CompletedAt:     time.Now().UTC(),
		TemporalCLI:     temporalCLIVersion(ctx, options.TemporalPath),
		TemporalServer:  temporalServerVersion(ctx, temporalServer.Client()),
		TemporalAPI:     moduleVersion("go.temporal.io/api"),
		TemporalSDK:     moduleVersion("go.temporal.io/sdk"),
		GoVersion:       runtime.Version(),
		FailureBoundary: "attempt 1 Start-to-Close timeout followed by attempt 2 start, before stale completion submission",
		Invariant:       "once attempt 2 owns the operation, attempt 1 cannot select the accepted Workflow result",
		Falsifier:       "stale task token succeeds, stale completion by ID is attempt-scoped, or the application fence authorizes attempt 1",
	}
	if err := preserveEvidence(runDirectory, evidence, result.Verdict, manifest, history); err != nil {
		return result, err
	}
	if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
		return result, fmt.Errorf(
			"experiment oracle failed: %s; completions: %+v",
			strings.Join(result.Verdict.Failures, "; "), evidence.Completions,
		)
	}
	return result, nil
}

func awaitAttempts(ctx context.Context, attempts <-chan capturedAttempt) (capturedAttempt, capturedAttempt, error) {
	observed := make(map[int32]capturedAttempt, 2)
	for len(observed) < 2 {
		select {
		case attempt := <-attempts:
			if attempt.observation.Attempt < 1 || attempt.observation.Attempt > 2 {
				return capturedAttempt{}, capturedAttempt{}, fmt.Errorf(
					"observed unexpected attempt %d", attempt.observation.Attempt,
				)
			}
			if _, exists := observed[attempt.observation.Attempt]; exists {
				return capturedAttempt{}, capturedAttempt{}, fmt.Errorf(
					"observed duplicate attempt %d", attempt.observation.Attempt,
				)
			}
			observed[attempt.observation.Attempt] = attempt
		case <-ctx.Done():
			return capturedAttempt{}, capturedAttempt{}, fmt.Errorf("wait for attempts 1 and 2: %w", ctx.Err())
		}
	}
	return observed[1], observed[2], nil
}

func exerciseCompletionArm(
	ctx context.Context,
	arm Arm,
	temporalClient client.Client,
	fence *AttemptFence,
	workflowID, runID string,
	attempt1, attempt2 capturedAttempt,
) []CompletionObservation {
	staleResult := "stale-attempt-1"
	currentResult := "current-attempt-2"
	switch arm {
	case ArmStaleTaskToken:
		return []CompletionObservation{
			observeCompletion(1, CompletionTaskToken, staleResult, func() error {
				return temporalClient.CompleteActivity(ctx, attempt1.taskToken, staleResult, nil)
			}),
			observeCompletion(2, CompletionTaskToken, currentResult, func() error {
				return temporalClient.CompleteActivity(ctx, attempt2.taskToken, currentResult, nil)
			}),
		}
	case ArmStaleByID:
		return []CompletionObservation{
			observeCompletion(1, CompletionByID, staleResult, func() error {
				return temporalClient.CompleteActivityByID(
					ctx, namespace, workflowID, runID, activityID, staleResult, nil,
				)
			}),
			observeCompletion(2, CompletionTaskToken, currentResult, func() error {
				return temporalClient.CompleteActivity(ctx, attempt2.taskToken, currentResult, nil)
			}),
		}
	case ArmFencedByID:
		observations := []CompletionObservation{
			observeApplicationFence(1, func() error {
				return fence.Authorize(ctx, attempt1.ownerToken)
			}),
		}
		if observations[0].Accepted {
			observations = append(observations, observeCompletion(1, CompletionByID, staleResult, func() error {
				return temporalClient.CompleteActivityByID(
					ctx, namespace, workflowID, runID, activityID, staleResult, nil,
				)
			}))
		}
		currentAuthorization := observeApplicationFence(2, func() error {
			return fence.Authorize(ctx, attempt2.ownerToken)
		})
		if !currentAuthorization.Accepted {
			observations = append(observations, currentAuthorization)
			return observations
		}
		return append(observations, observeCompletion(2, CompletionByID, currentResult, func() error {
			return temporalClient.CompleteActivityByID(
				ctx, namespace, workflowID, runID, activityID, currentResult, nil,
			)
		}))
	default:
		return nil
	}
}

func observeCompletion(
	callerAttempt int32,
	mechanism CompletionMechanism,
	result string,
	operation func() error,
) CompletionObservation {
	requestedAt := time.Now().UTC()
	err := operation()
	observation := CompletionObservation{
		CallerAttempt: callerAttempt,
		Mechanism:     mechanism,
		RequestedAt:   requestedAt,
		RespondedAt:   time.Now().UTC(),
		Accepted:      err == nil,
		Result:        result,
	}
	if err != nil {
		observation.ErrorCode = serviceerror.ToStatus(err).Code().String()
		observation.ErrorType = reflect.TypeOf(err).String()
		observation.Error = err.Error()
	}
	return observation
}

func observeApplicationFence(attempt int32, operation func() error) CompletionObservation {
	requestedAt := time.Now().UTC()
	err := operation()
	observation := CompletionObservation{
		CallerAttempt: attempt,
		Mechanism:     CompletionApplicationFence,
		RequestedAt:   requestedAt,
		RespondedAt:   time.Now().UTC(),
		Accepted:      err == nil,
	}
	if err != nil {
		observation.ErrorType = reflect.TypeOf(err).String()
		observation.Error = err.Error()
		if errors.Is(err, ErrStaleAttempt) {
			observation.ErrorCode = "stale_attempt"
		}
	}
	return observation
}

func preserveEvidence(
	runDirectory string,
	evidence Evidence,
	verdict Verdict,
	manifest Manifest,
	history *historypb.History,
) error {
	if err := writeJSON(filepath.Join(runDirectory, "observations.json"), evidence); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(runDirectory, "verdict.json"), verdict); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		return err
	}
	return writeHistory(filepath.Join(runDirectory, "temporal-history.json"), history)
}

func temporalCLIVersion(ctx context.Context, path string) string {
	output, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return "unknown: " + err.Error()
	}
	return strings.TrimSpace(string(output))
}

func validateOptions(options Options) error {
	if !options.Arm.Valid() || options.TemporalPath == "" || options.OutputRoot == "" || options.RunID == "" {
		return errors.New("run requires a valid arm, Temporal binary, output root, and run ID")
	}
	if !safePathComponent(options.RunID) {
		return errors.New("run ID must contain only ASCII letters, digits, dot, underscore, or hyphen")
	}
	info, err := os.Stat(options.TemporalPath)
	if err != nil {
		return fmt.Errorf("inspect Temporal binary %q: %w", options.TemporalPath, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("temporal binary %q is not executable", options.TemporalPath)
	}
	return nil
}

func safePathComponent(value string) bool {
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return value != "." && value != ".."
}
