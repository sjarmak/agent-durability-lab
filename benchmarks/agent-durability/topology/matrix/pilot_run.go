package matrix

import (
	"context"
	"debug/buildinfo"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/semantics"
)

type PilotConfig struct {
	Root                string
	PreregistrationPath string
	ContractPath        string
	TemporalPath        string
	WorkRoot            string
	AgentBinary         string
	SourceRoot          string
}

func RunPilot(ctx context.Context, config PilotConfig) (_ PilotReport, returnErr error) {
	if err := validatePilotConfig(ctx, config); err != nil {
		return PilotReport{}, err
	}
	registrationData, err := os.ReadFile(config.PreregistrationPath)
	if err != nil {
		return PilotReport{}, err
	}
	contractData, err := os.ReadFile(config.ContractPath)
	if err != nil {
		return PilotReport{}, err
	}
	registration, err := protocol.DecodePreregistration(registrationData)
	if err != nil {
		return PilotReport{}, err
	}
	contractSHA := sealedfs.HashBytes(contractData)
	if contractSHA != registration.Hashes.TopologyContractV1SHA256 {
		return PilotReport{}, fmt.Errorf("%w: topology contract hash", protocol.ErrInvalidEvidence)
	}
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePilot)
	if err != nil {
		return PilotReport{}, err
	}
	scheduleAudit, err := auditPilotSchedule(registration, schedule)
	if err != nil {
		return PilotReport{}, err
	}
	executable, err := os.Executable()
	if err != nil {
		return PilotReport{}, err
	}
	report := PilotReport{
		ProtocolVersion: protocol.PublicationProtocolVersion, Kind: PilotKind,
		CreatedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), TrackerBeadID: PilotTrackerBeadID,
		PublicationExcluded: true, ExclusionReason: "preregistered pilot episodes are excluded from publication analysis",
		Schedule: scheduleAudit, ContractSHA256: contractSHA,
	}
	report.HarnessBinarySHA256, err = sealedfs.HashRegularFile(executable)
	if err != nil {
		return PilotReport{}, err
	}
	report.AgentBinarySHA256, err = sealedfs.HashRegularFile(config.AgentBinary)
	if err != nil {
		return PilotReport{}, err
	}
	report.TemporalBinarySHA256, err = sealedfs.HashRegularFile(config.TemporalPath)
	if err != nil {
		return PilotReport{}, err
	}
	report.PreregistrationSHA256 = sealedfs.HashBytes(registrationData)
	sources, err := collectPilotSources(config.SourceRoot)
	if err != nil {
		return PilotReport{}, err
	}
	root, err := createRoot(config.Root)
	if err != nil {
		return PilotReport{}, err
	}
	if err := writeBytesExclusive(filepath.Join(root, PilotRegistrationFile), registrationData); err != nil {
		return PilotReport{}, err
	}
	if err := writeBytesExclusive(filepath.Join(root, PilotContractFile), contractData); err != nil {
		return PilotReport{}, err
	}
	if err := sealedfs.WriteJSONExclusive(filepath.Join(root, PilotScheduleFile), schedule); err != nil {
		return PilotReport{}, err
	}
	report.ScheduleSHA256, err = sealedfs.HashRegularFile(filepath.Join(root, PilotScheduleFile))
	if err != nil {
		return PilotReport{}, err
	}
	temporalExecutor, err := semantics.OpenTemporalExecutor(ctx, semantics.ExecutorConfig{
		TemporalPath: config.TemporalPath, WorkRoot: config.WorkRoot, AgentBinary: config.AgentBinary,
	})
	if err != nil {
		return PilotReport{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, temporalExecutor.Close()) }()
	versions, err := collectVersions(ctx, temporalExecutor, config.TemporalPath, executable)
	if err != nil {
		return PilotReport{}, err
	}
	rootRuns := filepath.Join(root, "runs")
	executors := liveArmExecutors(temporalExecutor, rootRuns)
	for _, block := range schedule.Blocks {
		execution, runErr := runner.RunPair(ctx, runner.Config{
			Root: filepath.Join(root, "pairs"), EvidenceRoot: rootRuns, Phase: protocol.PhasePilot,
			Registration: registration, Schedule: schedule, Executors: executors,
		}, block)
		if runErr != nil {
			return PilotReport{}, runErr
		}
		if err := accountPilotPair(&report, execution); err != nil {
			return PilotReport{}, err
		}
	}
	if err := temporalExecutor.Close(); err != nil {
		return PilotReport{}, err
	}
	finalizePilotQualification(&report)
	if report.Qualified {
		manifest, buildErr := buildPilotFreeze(config, root, report, registration, sources, versions)
		if buildErr != nil {
			return PilotReport{}, buildErr
		}
		if err := sealedfs.WriteJSONExclusive(filepath.Join(root, PilotFreezeFile), manifest); err != nil {
			return PilotReport{}, err
		}
		report.FreezeManifestSHA256, err = sealedfs.HashRegularFile(filepath.Join(root, PilotFreezeFile))
		if err != nil {
			return PilotReport{}, err
		}
	}
	if err := sealedfs.WriteJSONExclusive(filepath.Join(root, PilotReportFile), report); err != nil {
		return PilotReport{}, err
	}
	if err := writePilotInventory(root); err != nil {
		return PilotReport{}, err
	}
	return AuditPilot(root)
}

func validatePilotConfig(ctx context.Context, config PilotConfig) error {
	if ctx == nil || config.Root == "" || config.PreregistrationPath == "" || config.ContractPath == "" ||
		config.TemporalPath == "" || config.WorkRoot == "" || config.AgentBinary == "" || config.SourceRoot == "" {
		return fmt.Errorf("%w: pilot config", protocol.ErrInvalidEvidence)
	}
	if err := ValidateDisjointPaths(config.Root, config.WorkRoot); err != nil {
		return err
	}
	for _, path := range []string{config.PreregistrationPath, config.ContractPath, config.TemporalPath, config.AgentBinary} {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("%w: pilot input path", protocol.ErrInvalidEvidence)
		}
	}
	if info, err := os.Stat(config.SourceRoot); err != nil || !info.IsDir() {
		return fmt.Errorf("%w: pilot source root", protocol.ErrInvalidEvidence)
	}
	return nil
}

type pilotSources struct {
	runner, analyzer, agent, destination, barrier SourceSet
}

func collectPilotSources(root string) (pilotSources, error) {
	common := []string{
		"go.mod", "go.sum", "Makefile", "benchmarks/agent-durability/topology/agent",
		"benchmarks/agent-durability/topology/evidence", "benchmarks/agent-durability/topology/internal/sealedfs",
		"benchmarks/agent-durability/topology/matrix", "benchmarks/agent-durability/topology/oracle",
		"benchmarks/agent-durability/topology/protocol", "benchmarks/agent-durability/topology/runner",
		"benchmarks/agent-durability/topology/semantics", "benchmarks/agent-durability/topology/cmd/pilot",
		"internal/agentprocess", "internal/failureinject", "internal/workstore",
	}
	runnerSource, err := collectSourceSet(root, common)
	if err != nil {
		return pilotSources{}, err
	}
	analyzerSource, err := collectSourceSet(root, []string{
		"go.mod", "go.sum", "benchmarks/agent-durability/topology/evidence",
		"benchmarks/agent-durability/topology/internal/sealedfs", "benchmarks/agent-durability/topology/matrix",
		"benchmarks/agent-durability/topology/oracle", "benchmarks/agent-durability/topology/protocol",
		"benchmarks/agent-durability/topology/runner", "benchmarks/agent-durability/topology/semantics",
		"benchmarks/agent-durability/topology/cmd/pilot",
	})
	if err != nil {
		return pilotSources{}, err
	}
	agentSource, err := collectSourceSet(root, []string{"cmd/agent-simulator", "internal/agentsim", "internal/failureinject"})
	if err != nil {
		return pilotSources{}, err
	}
	destinationSource, err := collectSourceSet(root, []string{
		"benchmarks/agent-durability/topology/semantics/destination.go",
		"benchmarks/agent-durability/topology/semantics/activities.go",
		"benchmarks/agent-durability/topology/semantics/recovery_activities.go",
		"benchmarks/agent-durability/topology/semantics/runtime.go",
		"benchmarks/agent-durability/topology/semantics/recovery_runtime.go", "internal/workstore",
	})
	if err != nil {
		return pilotSources{}, err
	}
	barrierSource, err := collectSourceSet(root, []string{
		"benchmarks/agent-durability/topology/agent", "benchmarks/agent-durability/topology/semantics/runtime.go",
		"benchmarks/agent-durability/topology/semantics/recovery_runtime.go", "internal/failureinject",
	})
	if err != nil {
		return pilotSources{}, err
	}
	return pilotSources{runner: runnerSource, analyzer: analyzerSource, agent: agentSource, destination: destinationSource, barrier: barrierSource}, nil
}

func collectVersions(ctx context.Context, executor *semantics.TemporalExecutor, temporalPath, executable string) (VersionEnvelope, error) {
	server, err := executor.ServerVersion(ctx)
	if err != nil {
		return VersionEnvelope{}, err
	}
	cli, err := temporalCLIVersion(ctx, temporalPath)
	if err != nil {
		return VersionEnvelope{}, err
	}
	sdkVersion, err := dependencyVersion(executable, "go.temporal.io/sdk")
	if err != nil {
		return VersionEnvelope{}, err
	}
	return VersionEnvelope{
		TemporalServer: server, TemporalCLI: cli, TemporalGoSDK: sdkVersion, Go: runtime.Version(),
	}, nil
}

func temporalCLIVersion(ctx context.Context, temporalPath string) (string, error) {
	command := exec.CommandContext(ctx, temporalPath, "--version")
	output, err := command.CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) == "" {
		return "", fmt.Errorf("read Temporal CLI version: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func dependencyVersion(executable, module string) (string, error) {
	info, err := buildinfo.ReadFile(executable)
	if err == nil {
		for _, dependency := range info.Deps {
			if dependency.Path == module {
				return dependency.Version, nil
			}
		}
	}
	if current, ok := debug.ReadBuildInfo(); ok {
		for _, dependency := range current.Deps {
			if dependency.Path == module {
				return dependency.Version, nil
			}
		}
	}
	return "", fmt.Errorf("%w: dependency version %s", protocol.ErrInvalidEvidence, module)
}

func buildPilotFreeze(
	config PilotConfig,
	root string,
	report PilotReport,
	registration protocol.Preregistration,
	sources pilotSources,
	versions VersionEnvelope,
) (PilotFreezeManifest, error) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return PilotFreezeManifest{}, fmt.Errorf("read host identity: %w", err)
	}
	manifest := PilotFreezeManifest{
		ProtocolVersion: protocol.PublicationProtocolVersion, Kind: PilotFreezeKind,
		CreatedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), TrackerBeadID: PilotTrackerBeadID,
		ReviewDisposition: "qualified", PilotRoot: filepath.Base(root), PilotQualified: report.Qualified, PublicationExcluded: true,
		RunnerSource: sources.runner, AnalyzerSource: sources.analyzer, AgentProcessSource: sources.agent,
		DestinationSource: sources.destination, BarrierSource: sources.barrier,
		RunnerBinarySHA256: report.HarnessBinarySHA256, AnalyzerBinarySHA256: report.HarnessBinarySHA256,
		AgentBinarySHA256: report.AgentBinarySHA256, TemporalBinarySHA256: report.TemporalBinarySHA256,
		ContractSHA256: report.ContractSHA256, PreregistrationSHA256: report.PreregistrationSHA256, ScheduleSHA256: report.ScheduleSHA256,
		Versions: versions,
		Host:     HostEnvelope{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, HostnameSHA256: sealedfs.HashBytes([]byte(hostname)), CPUCount: runtime.NumCPU()},
		Worker:   semantics.FrozenWorkerConfiguration(), PublicationSeed: registration.Population.PublicationSeed,
		PilotSeed: registration.Population.PilotSeed,
		Pilot:     PilotFreezeSummary{Pairs: report.ValidPairs, Arms: report.ValidArms, HistoriesReplayed: report.HistoriesReplayed},
	}
	if err := validatePilotFreeze(manifest); err != nil {
		return PilotFreezeManifest{}, err
	}
	return manifest, nil
}

func writePilotInventory(root string) error {
	artifacts, err := inventoryArtifactsExcluding(root, PilotInventoryFile)
	if err != nil {
		return err
	}
	return sealedfs.WriteJSONExclusive(filepath.Join(root, PilotInventoryFile), Inventory{
		ProtocolVersion: protocol.PublicationProtocolVersion, Kind: PilotKind,
		ArtifactCount: len(artifacts), Artifacts: artifacts,
	})
}
