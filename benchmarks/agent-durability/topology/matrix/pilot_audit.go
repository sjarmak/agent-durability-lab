package matrix

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
)

func AuditPilot(root string) (PilotReport, error) {
	return auditPilot(root, false)
}

func auditPilot(root string, allowFixtures bool) (PilotReport, error) {
	resolved, err := resolveRoot(root)
	if err != nil {
		return PilotReport{}, err
	}
	rootHandle, err := os.OpenRoot(resolved)
	if err != nil {
		return PilotReport{}, err
	}
	defer func() { _ = rootHandle.Close() }()
	inventoryData, err := sealedfs.ReadRegularFileOnce(rootHandle, PilotInventoryFile, 64<<20)
	if err != nil {
		return PilotReport{}, fmt.Errorf("%w: pilot inventory: %v", protocol.ErrInvalidEvidence, err)
	}
	var inventory Inventory
	if err := sealedfs.DecodeJSON(PilotInventoryFile, inventoryData, &inventory); err != nil {
		return PilotReport{}, err
	}
	if inventory.ProtocolVersion != protocol.PublicationProtocolVersion || inventory.Kind != PilotKind ||
		inventory.ArtifactCount == 0 || inventory.ArtifactCount != len(inventory.Artifacts) {
		return PilotReport{}, fmt.Errorf("%w: pilot inventory identity", protocol.ErrInvalidEvidence)
	}
	actual, err := inventoryArtifactsExcluding(resolved, PilotInventoryFile)
	if err != nil {
		return PilotReport{}, err
	}
	if !reflect.DeepEqual(actual, inventory.Artifacts) {
		return PilotReport{}, fmt.Errorf("%w: pilot artifact inventory differs", protocol.ErrInvalidEvidence)
	}
	reportData, err := sealedfs.ReadRegularFileOnce(rootHandle, PilotReportFile, 1<<20)
	if err != nil {
		return PilotReport{}, err
	}
	var report PilotReport
	if err := sealedfs.DecodeJSON(PilotReportFile, reportData, &report); err != nil {
		return PilotReport{}, err
	}
	created, timeErr := time.Parse(time.RFC3339Nano, report.CreatedAtUTC)
	if timeErr != nil || created.Location() != time.UTC || report.ProtocolVersion != protocol.PublicationProtocolVersion ||
		report.Kind != PilotKind || report.TrackerBeadID != PilotTrackerBeadID || !report.PublicationExcluded || report.ExclusionReason == "" ||
		!validDigest(report.HarnessBinarySHA256) || !validDigest(report.AgentBinarySHA256) || !validDigest(report.TemporalBinarySHA256) ||
		!validDigest(report.ContractSHA256) || !validDigest(report.PreregistrationSHA256) || !validDigest(report.ScheduleSHA256) {
		return PilotReport{}, fmt.Errorf("%w: pilot report identity", protocol.ErrInvalidEvidence)
	}
	registrationData, err := sealedfs.ReadRegularFileOnce(rootHandle, PilotRegistrationFile, 4<<20)
	if err != nil {
		return PilotReport{}, err
	}
	registration, err := protocol.DecodePreregistration(registrationData)
	if err != nil {
		return PilotReport{}, err
	}
	contractData, err := sealedfs.ReadRegularFileOnce(rootHandle, PilotContractFile, 4<<20)
	if err != nil {
		return PilotReport{}, err
	}
	scheduleData, err := sealedfs.ReadRegularFileOnce(rootHandle, PilotScheduleFile, 64<<20)
	if err != nil {
		return PilotReport{}, err
	}
	var schedule protocol.Schedule
	if err := sealedfs.DecodeJSON(PilotScheduleFile, scheduleData, &schedule); err != nil {
		return PilotReport{}, err
	}
	scheduleAudit, err := auditPilotSchedule(registration, schedule)
	if err != nil {
		return PilotReport{}, err
	}
	if report.Schedule != scheduleAudit || report.ContractSHA256 != sealedfs.HashBytes(contractData) ||
		report.ContractSHA256 != registration.Hashes.TopologyContractV1SHA256 ||
		report.PreregistrationSHA256 != sealedfs.HashBytes(registrationData) || report.ScheduleSHA256 != sealedfs.HashBytes(scheduleData) {
		return PilotReport{}, fmt.Errorf("%w: pilot frozen input mismatch", protocol.ErrInvalidEvidence)
	}
	want := report
	want.AttemptedPairs, want.ValidPairs, want.InvalidPairs = 0, 0, 0
	want.AttemptedArms, want.ValidArms = 0, 0
	want.UnsafeArms, want.UnsafeArmsDistinguished = 0, 0
	want.ProtectedOrUnfaultedArms, want.ProtectedOrUnfaultedArmsPassed = 0, 0
	want.HistoriesReplayed, want.Qualified = 0, false
	pairRoot := filepath.Join(resolved, "pairs")
	runRoot := filepath.Join(resolved, "runs")
	for _, block := range schedule.Blocks {
		execution, loadErr := runner.LoadPair(pairRoot, runner.PairDirectoryName(block.PairID))
		if loadErr != nil || execution.Phase != protocol.PhasePilot || !reflect.DeepEqual(execution.Block, block) || len(execution.Arms) != len(protocol.Topologies()) {
			return PilotReport{}, fmt.Errorf("%w: pilot pair %s: %v", protocol.ErrInvalidEvidence, block.PairID, loadErr)
		}
		bundles := make([]protocol.EvidenceBundle, 0, len(execution.Arms))
		for index, arm := range execution.Arms {
			if arm.Order != index+1 || arm.Topology != block.TopologyOrder[index] || arm.RunID != block.PairID+"/"+string(block.TopologyOrder[index]) {
				return PilotReport{}, fmt.Errorf("%w: pilot pair arm order", protocol.ErrInvalidEvidence)
			}
			if arm.EvidenceDirectory == "" {
				if execution.Admission != protocol.AdmissionInvalid || arm.Error == "" {
					return PilotReport{}, fmt.Errorf("%w: pilot missing arm evidence", protocol.ErrInvalidEvidence)
				}
				continue
			}
			bundle, verdict, verifyErr := oracle.VerifyRun(runRoot, evidence.RunDirectoryName(arm.RunID))
			if verifyErr != nil || !reflect.DeepEqual(verdict, arm.Verdict) {
				return PilotReport{}, fmt.Errorf("%w: pilot arm %s: %v", protocol.ErrInvalidEvidence, arm.RunID, verifyErr)
			}
			if err := auditPilotArm(block, arm, bundle, allowFixtures); err != nil {
				return PilotReport{}, err
			}
			if bundle.Manifest.TrackerBeadID != PilotTrackerBeadID || bundle.EffectiveInput.SourceSHA256 != report.HarnessBinarySHA256 ||
				bundle.NativeHistory.ReplayWorkerSHA256 != report.HarnessBinarySHA256 ||
				bundle.EffectiveInput.AgentBinarySHA256 != report.AgentBinarySHA256 {
				return PilotReport{}, fmt.Errorf("%w: pilot arm provenance", protocol.ErrInvalidEvidence)
			}
			bundles = append(bundles, bundle)
		}
		if len(bundles) == len(protocol.Topologies()) &&
			(!bundles[0].EffectiveInput.MatchedWith(bundles[1].EffectiveInput) ||
				bundles[0].Manifest.LogicalOperationID != bundles[1].Manifest.LogicalOperationID ||
				bundles[0].Manifest.TrackerBeadID != bundles[1].Manifest.TrackerBeadID) {
			return PilotReport{}, fmt.Errorf("%w: pilot paired input", protocol.ErrInvalidEvidence)
		}
		if err := accountPilotPair(&want, execution); err != nil {
			return PilotReport{}, err
		}
	}
	finalizePilotQualification(&want)
	if want.Qualified {
		if !validDigest(report.FreezeManifestSHA256) || report.FreezeManifestSHA256 != hashForPath(actual, PilotFreezeFile) {
			return PilotReport{}, fmt.Errorf("%w: pilot freeze hash", protocol.ErrInvalidEvidence)
		}
		freezeData, readErr := sealedfs.ReadRegularFileOnce(rootHandle, PilotFreezeFile, 16<<20)
		if readErr != nil {
			return PilotReport{}, readErr
		}
		var freeze PilotFreezeManifest
		if err := sealedfs.DecodeJSON(PilotFreezeFile, freezeData, &freeze); err != nil {
			return PilotReport{}, err
		}
		if err := validatePilotFreeze(freeze); err != nil {
			return PilotReport{}, err
		}
		if freeze.PilotRoot != filepath.Base(resolved) || freeze.RunnerBinarySHA256 != report.HarnessBinarySHA256 ||
			freeze.AnalyzerBinarySHA256 != report.HarnessBinarySHA256 || freeze.AgentBinarySHA256 != report.AgentBinarySHA256 ||
			freeze.TemporalBinarySHA256 != report.TemporalBinarySHA256 || freeze.ContractSHA256 != report.ContractSHA256 ||
			freeze.PreregistrationSHA256 != report.PreregistrationSHA256 || freeze.ScheduleSHA256 != report.ScheduleSHA256 ||
			freeze.PublicationSeed != registration.Population.PublicationSeed || freeze.PilotSeed != registration.Population.PilotSeed ||
			freeze.Pilot != (PilotFreezeSummary{Pairs: report.ValidPairs, Arms: report.ValidArms, HistoriesReplayed: report.HistoriesReplayed}) {
			return PilotReport{}, fmt.Errorf("%w: pilot freeze binding", protocol.ErrInvalidEvidence)
		}
	} else if report.FreezeManifestSHA256 != "" || hashForPath(actual, PilotFreezeFile) != "" {
		return PilotReport{}, fmt.Errorf("%w: unqualified pilot freeze", protocol.ErrInvalidEvidence)
	}
	if !reflect.DeepEqual(report, want) {
		return PilotReport{}, fmt.Errorf("%w: pilot report differs from raw evidence", protocol.ErrInvalidEvidence)
	}
	return report, nil
}
