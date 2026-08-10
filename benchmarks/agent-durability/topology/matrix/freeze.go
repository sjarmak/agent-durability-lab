package matrix

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/semantics"
)

const (
	PilotFreezeKind    = "topology-publication-harness-freeze-v1"
	PilotTrackerBeadID = "temporal_projects-4ic.6"
)

type VersionEnvelope struct {
	TemporalServer string `json:"temporal_server"`
	TemporalCLI    string `json:"temporal_cli"`
	TemporalGoSDK  string `json:"temporal_go_sdk"`
	Go             string `json:"go"`
}

type HostEnvelope struct {
	GOOS           string `json:"goos"`
	GOARCH         string `json:"goarch"`
	HostnameSHA256 string `json:"hostname_sha256"`
	CPUCount       int    `json:"cpu_count"`
}

type PilotFreezeSummary struct {
	Pairs             int `json:"pairs"`
	Arms              int `json:"arms"`
	HistoriesReplayed int `json:"histories_replayed"`
}

type PilotFreezeManifest struct {
	ProtocolVersion       string                        `json:"protocol_version"`
	Kind                  string                        `json:"kind"`
	CreatedAtUTC          string                        `json:"created_at_utc"`
	TrackerBeadID         string                        `json:"tracker_bead_id"`
	ReviewDisposition     string                        `json:"review_disposition"`
	PilotRoot             string                        `json:"pilot_root"`
	PilotQualified        bool                          `json:"pilot_qualified"`
	PublicationExcluded   bool                          `json:"publication_excluded"`
	RunnerSource          SourceSet                     `json:"runner_source"`
	AnalyzerSource        SourceSet                     `json:"analyzer_source"`
	AgentProcessSource    SourceSet                     `json:"agent_process_source"`
	DestinationSource     SourceSet                     `json:"destination_source"`
	BarrierSource         SourceSet                     `json:"barrier_controller_source"`
	RunnerBinarySHA256    string                        `json:"runner_binary_sha256"`
	AnalyzerBinarySHA256  string                        `json:"analyzer_binary_sha256"`
	AgentBinarySHA256     string                        `json:"agent_binary_sha256"`
	TemporalBinarySHA256  string                        `json:"temporal_binary_sha256"`
	ContractSHA256        string                        `json:"contract_sha256"`
	PreregistrationSHA256 string                        `json:"preregistration_sha256"`
	ScheduleSHA256        string                        `json:"schedule_sha256"`
	Versions              VersionEnvelope               `json:"versions"`
	Host                  HostEnvelope                  `json:"host"`
	Worker                semantics.WorkerConfiguration `json:"worker_configuration"`
	PublicationSeed       uint64                        `json:"publication_seed"`
	PilotSeed             uint64                        `json:"pilot_seed"`
	Pilot                 PilotFreezeSummary            `json:"pilot"`
}

type PilotFreezeVerificationConfig struct {
	SourceRoot     string
	RunnerBinary   string
	AgentBinary    string
	TemporalBinary string
}

func validatePilotFreeze(manifest PilotFreezeManifest) error {
	created, err := time.Parse(time.RFC3339Nano, manifest.CreatedAtUTC)
	if err != nil || created.Location() != time.UTC || manifest.ProtocolVersion != protocol.PublicationProtocolVersion ||
		manifest.Kind != PilotFreezeKind || manifest.TrackerBeadID != PilotTrackerBeadID ||
		manifest.ReviewDisposition != "qualified" || !manifest.PilotQualified || !manifest.PublicationExcluded ||
		manifest.PilotRoot == "" || filepath.Base(manifest.PilotRoot) != manifest.PilotRoot || strings.Contains(manifest.PilotRoot, "..") {
		return fmt.Errorf("%w: pilot freeze identity", protocol.ErrInvalidEvidence)
	}
	for _, set := range []SourceSet{
		manifest.RunnerSource, manifest.AnalyzerSource, manifest.AgentProcessSource,
		manifest.DestinationSource, manifest.BarrierSource,
	} {
		if err := validateSourceSet(set); err != nil {
			return err
		}
	}
	for _, digest := range []string{
		manifest.RunnerBinarySHA256, manifest.AnalyzerBinarySHA256, manifest.AgentBinarySHA256,
		manifest.TemporalBinarySHA256, manifest.ContractSHA256, manifest.PreregistrationSHA256, manifest.ScheduleSHA256,
	} {
		if !validDigest(digest) {
			return fmt.Errorf("%w: pilot freeze digest", protocol.ErrInvalidEvidence)
		}
	}
	if manifest.RunnerBinarySHA256 != manifest.AnalyzerBinarySHA256 ||
		manifest.Versions.TemporalServer == "" || manifest.Versions.TemporalCLI == "" ||
		manifest.Versions.TemporalGoSDK == "" || manifest.Versions.Go == "" ||
		manifest.Host.GOOS == "" || manifest.Host.GOARCH == "" || !validDigest(manifest.Host.HostnameSHA256) || manifest.Host.CPUCount < 1 ||
		!reflect.DeepEqual(manifest.Worker, semantics.FrozenWorkerConfiguration()) ||
		manifest.PublicationSeed == 0 || manifest.PilotSeed == 0 || manifest.PublicationSeed == manifest.PilotSeed ||
		manifest.Pilot.Pairs < 1 || manifest.Pilot.Arms != manifest.Pilot.Pairs*len(protocol.Topologies()) ||
		manifest.Pilot.HistoriesReplayed != manifest.Pilot.Arms {
		return fmt.Errorf("%w: pilot freeze environment", protocol.ErrInvalidEvidence)
	}
	return nil
}

func VerifyPilotFreeze(ctx context.Context, root string, config PilotFreezeVerificationConfig) error {
	if ctx == nil || config.SourceRoot == "" || config.RunnerBinary == "" || config.AgentBinary == "" || config.TemporalBinary == "" {
		return fmt.Errorf("%w: pilot freeze verification config", protocol.ErrInvalidEvidence)
	}
	report, err := AuditPilot(root)
	if err != nil {
		return err
	}
	if !report.Qualified {
		return fmt.Errorf("%w: pilot did not qualify", protocol.ErrInvalidEvidence)
	}
	resolved, err := resolveRoot(root)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(resolved, PilotFreezeFile))
	if err != nil {
		return err
	}
	var manifest PilotFreezeManifest
	if err := sealedfs.DecodeJSON(PilotFreezeFile, data, &manifest); err != nil {
		return err
	}
	sources, err := collectPilotSources(config.SourceRoot)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(manifest.RunnerSource, sources.runner) || !reflect.DeepEqual(manifest.AnalyzerSource, sources.analyzer) ||
		!reflect.DeepEqual(manifest.AgentProcessSource, sources.agent) || !reflect.DeepEqual(manifest.DestinationSource, sources.destination) ||
		!reflect.DeepEqual(manifest.BarrierSource, sources.barrier) {
		return fmt.Errorf("%w: frozen source differs", protocol.ErrInvalidEvidence)
	}
	runnerHash, err := sealedfs.HashRegularFile(config.RunnerBinary)
	if err != nil {
		return err
	}
	agentHash, err := sealedfs.HashRegularFile(config.AgentBinary)
	if err != nil {
		return err
	}
	temporalHash, err := sealedfs.HashRegularFile(config.TemporalBinary)
	if err != nil {
		return err
	}
	if runnerHash != manifest.RunnerBinarySHA256 || agentHash != manifest.AgentBinarySHA256 || temporalHash != manifest.TemporalBinarySHA256 {
		return fmt.Errorf("%w: frozen binary differs", protocol.ErrInvalidEvidence)
	}
	cli, err := temporalCLIVersion(ctx, config.TemporalBinary)
	if err != nil {
		return err
	}
	sdk, err := dependencyVersion(config.RunnerBinary, "go.temporal.io/sdk")
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	currentHost := HostEnvelope{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, HostnameSHA256: sealedfs.HashBytes([]byte(hostname)), CPUCount: runtime.NumCPU()}
	if cli != manifest.Versions.TemporalCLI || sdk != manifest.Versions.TemporalGoSDK || runtime.Version() != manifest.Versions.Go ||
		!reflect.DeepEqual(currentHost, manifest.Host) || !reflect.DeepEqual(semantics.FrozenWorkerConfiguration(), manifest.Worker) {
		return fmt.Errorf("%w: frozen environment differs", protocol.ErrInvalidEvidence)
	}
	return nil
}
