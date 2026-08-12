package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

type experimentMetadata struct {
	CodexVersion       string `json:"codex_version"`
	CodexBinaryPath    string `json:"codex_binary_path"`
	CodexBinarySHA256  string `json:"codex_binary_sha256"`
	CodexWrapperPath   string `json:"codex_wrapper_path"`
	CodexWrapperSHA256 string `json:"codex_wrapper_sha256"`
	CodexHomePath      string `json:"codex_home_path"`
	Model              string `json:"model"`
	ReasoningEffort    string `json:"reasoning_effort"`
	Sandbox            string `json:"sandbox"`
	Hermetic           bool   `json:"hermetic"`
	Authentication     string `json:"authentication"`
	InvocationPath     string `json:"invocation_path"`
	WorkerSHA256       string `json:"worker_sha256"`
	EffectBinaryPath   string `json:"effect_binary_path"`
	EffectSHA256       string `json:"effect_sha256"`
	LauncherBinaryPath string `json:"launcher_binary_path"`
	LauncherSHA256     string `json:"launcher_sha256"`
	OutputSchemaPath   string `json:"output_schema_path"`
	SchemaSHA256       string `json:"schema_sha256"`
	HarnessSHA256      string `json:"harness_sha256"`
}

type suiteFailure struct {
	RecordedAt time.Time `json:"recorded_at"`
	Error      string    `json:"error"`
	Preserved  bool      `json:"preserved"`
}

type experimentHashes struct {
	Codex, Wrapper, Worker, Effect, Launcher, Schema, Harness string
}

func RunExperiment(parent context.Context, options ExperimentOptions) (result ExperimentResult, runErr error) {
	var err error
	options, err = normalizeExperimentOptions(options)
	if err != nil {
		return result, err
	}
	if err := validateExperimentOptions(options); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()
	if err := os.MkdirAll(filepath.Dir(options.EvidenceRoot), 0o750); err != nil {
		return result, err
	}
	if err := os.Mkdir(options.EvidenceRoot, 0o750); err != nil {
		return result, err
	}
	result.EvidenceRoot = options.EvidenceRoot
	defer func() {
		if runErr != nil {
			runErr = errors.Join(runErr, writeJSONExclusive(filepath.Join(options.EvidenceRoot, "failure.json"), suiteFailure{
				RecordedAt: time.Now().UTC(), Error: runErr.Error(), Preserved: true,
			}))
		}
	}()
	metadata, err := inspectExperimentInputs(ctx, options)
	if err != nil {
		return result, err
	}
	serverLog, err := os.OpenFile(filepath.Join(options.EvidenceRoot, "temporal-server.log"),
		os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return result, err
	}
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath, DBFilename: filepath.Join(options.EvidenceRoot, "temporal.db"),
		LogLevel: "warn", LogFormat: "pretty", Stdout: serverLog, Stderr: serverLog,
	})
	if err != nil {
		_ = serverLog.Close()
		return result, fmt.Errorf("start Temporal dev server: %w", err)
	}
	defer func() {
		server.Client().Close()
		runErr = errors.Join(runErr, server.Stop(), serverLog.Close())
	}()
	controller, err := client.Dial(client.Options{
		HostPort: server.FrontendHostPort(), Namespace: "default", Identity: "codex-experiment-controller",
	})
	if err != nil {
		return result, fmt.Errorf("connect Codex experiment controller: %w", err)
	}
	defer controller.Close()
	for trial := 1; trial <= options.Trials; trial++ {
		for _, boundary := range experimentSchedule(options.RecoveryMode) {
			directory, err := runCodexTrial(ctx, controller, server.FrontendHostPort(), options, metadata, boundary, trial)
			if err != nil {
				return result, fmt.Errorf("run %s trial %d: %w", boundary, trial, err)
			}
			result.RunDirectories = append(result.RunDirectories, directory)
		}
	}
	if err := verifyExperimentInputsUnchanged(options, metadata); err != nil {
		return result, err
	}
	if err := writeJSONExclusive(filepath.Join(options.EvidenceRoot, "suite-summary.json"), result); err != nil {
		return result, err
	}
	return result, nil
}

func experimentSchedule(mode RecoveryMode) []FaultBoundary {
	if mode.normalized() == RecoveryModeFenced {
		return append([]FaultBoundary{FaultNone}, fencedFaultSchedule()...)
	}
	return append([]FaultBoundary{FaultNone}, unsafeFaultSchedule()...)
}

func inspectExperimentInputs(ctx context.Context, options ExperimentOptions) (experimentMetadata, error) {
	versionOutput, err := exec.CommandContext(ctx, options.CodexBinary, "--version").Output()
	if err != nil {
		return experimentMetadata{}, fmt.Errorf("read Codex version: %w", err)
	}
	version := strings.TrimSpace(string(versionOutput))
	if version == "" {
		return experimentMetadata{}, errors.New("codex returned empty version")
	}
	authentication := "not-applicable-hermetic"
	if !options.Hermetic {
		wrapperStatus, err := exec.CommandContext(ctx, options.CodexWrapper, "login", "status").CombinedOutput()
		if err := requireChatGPTLogin("fixed codex-2 wrapper", wrapperStatus, err); err != nil {
			return experimentMetadata{}, err
		}
		binaryStatus := exec.CommandContext(ctx, options.CodexBinary, "login", "status")
		binaryStatus.Env = mergeEnvironment(os.Environ(), []string{"CODEX_HOME=" + options.CodexHome})
		statusOutput, err := binaryStatus.CombinedOutput()
		if err := requireChatGPTLogin("pinned Codex CLI/profile", statusOutput, err); err != nil {
			return experimentMetadata{}, err
		}
		authentication = "wrapper-and-pinned-cli-profile-logged-in-using-chatgpt"
	}
	hashes, err := hashExperimentInputs(options)
	if err != nil {
		return experimentMetadata{}, err
	}
	return experimentMetadata{
		CodexVersion: version, CodexBinaryPath: options.CodexBinary, CodexBinarySHA256: hashes.Codex,
		CodexWrapperPath: options.CodexWrapper, CodexWrapperSHA256: hashes.Wrapper,
		CodexHomePath: options.CodexHome, Model: options.Model, ReasoningEffort: options.ReasoningEffort,
		Sandbox: "workspace-write", Hermetic: options.Hermetic, Authentication: authentication,
		InvocationPath: "pinned-underlying-cli-with-codex-2-profile",
		WorkerSHA256:   hashes.Worker, EffectBinaryPath: options.EffectBinary, EffectSHA256: hashes.Effect,
		LauncherBinaryPath: options.LauncherBinary, LauncherSHA256: hashes.Launcher,
		OutputSchemaPath: options.OutputSchema, SchemaSHA256: hashes.Schema, HarnessSHA256: hashes.Harness,
	}, nil
}

func requireChatGPTLogin(label string, output []byte, commandErr error) error {
	if commandErr != nil {
		return fmt.Errorf("%s login status: %w: %s", label, commandErr, output)
	}
	if !strings.Contains(string(output), "Logged in using ChatGPT") {
		return fmt.Errorf("%s is not authenticated with ChatGPT: %s", label, output)
	}
	return nil
}

func hashExperimentInputs(options ExperimentOptions) (experimentHashes, error) {
	harness, err := os.Executable()
	if err != nil {
		return experimentHashes{}, err
	}
	paths := []string{
		options.CodexBinary, options.CodexWrapper, options.WorkerBinary,
		options.EffectBinary, options.LauncherBinary, options.OutputSchema, harness,
	}
	values := make([]string, len(paths))
	for index, path := range paths {
		values[index], err = protocol.FileSHA256(path)
		if err != nil {
			return experimentHashes{}, err
		}
	}
	return experimentHashes{
		Codex: values[0], Wrapper: values[1], Worker: values[2], Effect: values[3],
		Launcher: values[4], Schema: values[5], Harness: values[6],
	}, nil
}

func verifyExperimentInputsUnchanged(options ExperimentOptions, metadata experimentMetadata) error {
	actual, err := hashExperimentInputs(options)
	if err != nil {
		return err
	}
	want := experimentHashes{
		Codex: metadata.CodexBinarySHA256, Wrapper: metadata.CodexWrapperSHA256,
		Worker: metadata.WorkerSHA256, Effect: metadata.EffectSHA256,
		Launcher: metadata.LauncherSHA256, Schema: metadata.SchemaSHA256, Harness: metadata.HarnessSHA256,
	}
	if actual != want {
		return errors.New("pinned experiment input changed while the suite was running")
	}
	return nil
}
