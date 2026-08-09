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
	"go.temporal.io/sdk/testsuite"
)

type experimentMetadata struct {
	ClaudeVersion  string
	ClaudeSHA256   string
	WorkerSHA256   string
	EffectSHA256   string
	LauncherSHA256 string
}

type suiteFailure struct {
	RecordedAt time.Time `json:"recorded_at"`
	Error      string    `json:"error"`
	Preserved  bool      `json:"preserved"`
}

func RunExperiment(parent context.Context, options ExperimentOptions) (result ExperimentResult, runErr error) {
	if err := validateExperimentOptions(options); err != nil {
		return ExperimentResult{}, err
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()
	if err := prepareEvidenceRoot(options.EvidenceRoot); err != nil {
		return ExperimentResult{}, err
	}
	result.EvidenceRoot = options.EvidenceRoot
	defer func() {
		if runErr != nil {
			failureErr := writeJSONExclusive(filepath.Join(options.EvidenceRoot, "failure.json"), suiteFailure{
				RecordedAt: time.Now().UTC(), Error: runErr.Error(), Preserved: true,
			})
			if failureErr != nil {
				runErr = errors.Join(runErr, fmt.Errorf("preserve suite failure: %w", failureErr))
			}
		}
	}()

	metadata, err := inspectExperimentBinaries(ctx, options)
	if err != nil {
		return result, err
	}
	temporalServer, serverLog, err := startExperimentServer(ctx, options)
	if err != nil {
		return result, err
	}
	defer func() {
		temporalServer.Client().Close()
		runErr = errors.Join(runErr, temporalServer.Stop(), serverLog.Close())
	}()

	result.RunDirectories, err = runExperimentTrials(ctx, temporalServer, options, metadata)
	if err != nil {
		return result, err
	}
	if err := writeJSONExclusive(filepath.Join(options.EvidenceRoot, "suite-summary.json"), result); err != nil {
		return result, err
	}
	return result, nil
}

func prepareEvidenceRoot(root string) error {
	if err := os.MkdirAll(filepath.Dir(root), 0o750); err != nil {
		return fmt.Errorf("create evidence parent: %w", err)
	}
	if err := os.Mkdir(root, 0o750); err != nil {
		return fmt.Errorf("create append-only evidence root: %w", err)
	}
	return nil
}

func startExperimentServer(ctx context.Context, options ExperimentOptions) (*testsuite.DevServer, *os.File, error) {
	serverLog, err := os.OpenFile(filepath.Join(options.EvidenceRoot, "temporal-server.log"),
		os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("create Temporal server log: %w", err)
	}
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: options.TemporalPath, DBFilename: filepath.Join(options.EvidenceRoot, "temporal.db"),
		LogLevel: "warn", LogFormat: "pretty", Stdout: serverLog, Stderr: serverLog,
	})
	if err != nil {
		_ = serverLog.Close()
		return nil, nil, fmt.Errorf("start Temporal dev server: %w", err)
	}
	return server, serverLog, nil
}

func runExperimentTrials(ctx context.Context, server *testsuite.DevServer, options ExperimentOptions,
	metadata experimentMetadata,
) ([]string, error) {
	var directories []string
	for trial := 1; trial <= options.Trials; trial++ {
		directory, err := runClaudeTrial(ctx, server.Client(), server.FrontendHostPort(),
			options, metadata, protocol.ProbeUnfaulted, FaultNone, trial)
		if err != nil {
			return directories, fmt.Errorf("run unfaulted trial %d: %w", trial, err)
		}
		directories = append(directories, directory)
		for _, boundary := range unsafeFaultSchedule() {
			directory, err = runClaudeTrial(ctx, server.Client(), server.FrontendHostPort(),
				options, metadata, protocol.ProbeUnsafe, boundary, trial)
			if err != nil {
				return directories, fmt.Errorf("run %s trial %d: %w", boundary, trial, err)
			}
			directories = append(directories, directory)
		}
	}
	return directories, nil
}

func inspectExperimentBinaries(ctx context.Context, options ExperimentOptions) (experimentMetadata, error) {
	versionCommand := exec.CommandContext(ctx, options.ClaudeBinary, "--version")
	versionOutput, err := versionCommand.Output()
	if err != nil {
		return experimentMetadata{}, fmt.Errorf("read Claude CLI version: %w", err)
	}
	claudeHash, err := protocol.FileSHA256(options.ClaudeBinary)
	if err != nil {
		return experimentMetadata{}, err
	}
	workerHash, err := protocol.FileSHA256(options.WorkerBinary)
	if err != nil {
		return experimentMetadata{}, err
	}
	effectHash, err := protocol.FileSHA256(options.EffectBinary)
	if err != nil {
		return experimentMetadata{}, err
	}
	launcherHash, err := protocol.FileSHA256(options.LauncherBinary)
	if err != nil {
		return experimentMetadata{}, err
	}
	version := strings.TrimSpace(string(versionOutput))
	if version == "" {
		return experimentMetadata{}, errors.New("claude CLI returned an empty version")
	}
	return experimentMetadata{
		ClaudeVersion: version, ClaudeSHA256: claudeHash,
		WorkerSHA256: workerHash, EffectSHA256: effectHash, LauncherSHA256: launcherHash,
	}, nil
}
