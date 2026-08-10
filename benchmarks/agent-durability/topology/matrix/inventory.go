package matrix

import (
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
)

type Artifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type Inventory struct {
	ProtocolVersion string     `json:"protocol_version"`
	Kind            string     `json:"kind"`
	ArtifactCount   int        `json:"artifact_count"`
	Artifacts       []Artifact `json:"artifacts"`
}

func writeInventory(root, kind string) error {
	artifacts, err := inventoryArtifacts(root)
	if err != nil {
		return err
	}
	return sealedfs.WriteJSONExclusive(filepath.Join(root, InventoryFile), Inventory{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		Kind:            kind, ArtifactCount: len(artifacts), Artifacts: artifacts,
	})
}

func Audit(root string) (Report, error) {
	resolved, err := resolveRoot(root)
	if err != nil {
		return Report{}, err
	}
	rootHandle, err := os.OpenRoot(resolved)
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = rootHandle.Close() }()
	inventoryData, err := sealedfs.ReadRegularFileOnce(rootHandle, InventoryFile, 64<<20)
	if err != nil {
		return Report{}, fmt.Errorf("%w: matrix inventory: %v", protocol.ErrInvalidEvidence, err)
	}
	var inventory Inventory
	if err := sealedfs.DecodeJSON(InventoryFile, inventoryData, &inventory); err != nil {
		return Report{}, err
	}
	if inventory.ProtocolVersion != protocol.PublicationProtocolVersion || !validMatrixKind(inventory.Kind) ||
		inventory.ArtifactCount != len(inventory.Artifacts) || inventory.ArtifactCount == 0 {
		return Report{}, fmt.Errorf("%w: matrix inventory identity", protocol.ErrInvalidEvidence)
	}
	actual, err := inventoryArtifacts(resolved)
	if err != nil {
		return Report{}, err
	}
	if !reflect.DeepEqual(actual, inventory.Artifacts) {
		return Report{}, fmt.Errorf("%w: matrix artifact inventory differs", protocol.ErrInvalidEvidence)
	}
	reportData, err := sealedfs.ReadRegularFileOnce(rootHandle, ReportFile, 1<<20)
	if err != nil {
		return Report{}, err
	}
	var report Report
	if err := sealedfs.DecodeJSON(ReportFile, reportData, &report); err != nil {
		return Report{}, err
	}
	if report.ProtocolVersion != protocol.PublicationProtocolVersion || report.Kind != inventory.Kind || !validMatrixKind(report.Kind) ||
		!report.PublicationExcluded || report.TrackerBeadID != "temporal_projects-4ic.4" || report.ExclusionReason == "" ||
		!validDigest(report.HarnessBinarySHA256) || report.Kind == FixtureConformanceKind &&
		(report.AgentBinarySHA256 != "" || report.TemporalBinarySHA256 != "") || report.Kind == FullConformanceKind &&
		(!validDigest(report.AgentBinarySHA256) || !validDigest(report.TemporalBinarySHA256)) {
		return Report{}, fmt.Errorf("%w: matrix report identity", protocol.ErrInvalidEvidence)
	}
	registrationData, err := sealedfs.ReadRegularFileOnce(rootHandle, PreregistrationFile, 4<<20)
	if err != nil {
		return Report{}, err
	}
	registration, err := protocol.DecodePreregistration(registrationData)
	if err != nil {
		return Report{}, err
	}
	scheduleData, err := sealedfs.ReadRegularFileOnce(rootHandle, ScheduleFile, 64<<20)
	if err != nil {
		return Report{}, err
	}
	var schedule protocol.Schedule
	if err := sealedfs.DecodeJSON(ScheduleFile, scheduleData, &schedule); err != nil {
		return Report{}, err
	}
	scheduleAudit, selected, err := AuditSchedule(registration, schedule)
	if err != nil {
		return Report{}, err
	}
	if report.Schedule != scheduleAudit || report.SelectedStrata != len(selected) ||
		report.PreregistrationSHA256 != hashForPath(actual, PreregistrationFile) || report.ScheduleSHA256 != hashForPath(actual, ScheduleFile) {
		return Report{}, fmt.Errorf("%w: matrix schedule report mismatch", protocol.ErrInvalidEvidence)
	}
	want := report
	want.ValidPairs, want.ValidArms = 0, 0
	want.UnsafeArms, want.UnsafeArmsDistinguished = 0, 0
	want.ProtectedOrUnfaultedArms, want.ProtectedOrUnfaultedArmsPassed = 0, 0
	want.LiveSentinelPairs, want.LiveSentinelArms = 0, 0
	want.LiveUnsafeArms, want.LiveUnsafeArmsDistinguished = 0, 0
	want.LivePassingArms, want.LivePassingArmsPassed, want.LiveHistoriesReplayed = 0, 0, 0
	pairRoot := filepath.Join(resolved, "fixtures", "pairs")
	runRoot := filepath.Join(resolved, "fixtures", "runs")
	for _, block := range selected {
		execution, loadErr := runner.LoadPair(pairRoot, runner.PairDirectoryName(block.PairID))
		if loadErr != nil || !reflect.DeepEqual(execution.Block, block) {
			return Report{}, fmt.Errorf("%w: fixture pair %s: %v", protocol.ErrInvalidEvidence, block.PairID, loadErr)
		}
		bundles := make([]protocol.EvidenceBundle, 0, len(execution.Arms))
		for index := range execution.Arms {
			arm := &execution.Arms[index]
			bundle, verdict, verifyErr := oracle.VerifyRun(runRoot, evidence.RunDirectoryName(arm.RunID))
			if verifyErr != nil || bundle.Manifest.RunID != arm.RunID || !reflect.DeepEqual(verdict, arm.Verdict) {
				return Report{}, fmt.Errorf("%w: fixture arm %s: %v", protocol.ErrInvalidEvidence, arm.RunID, verifyErr)
			}
			bundles = append(bundles, bundle)
		}
		if err := auditPairBundles(block, execution, bundles, true); err != nil {
			return Report{}, err
		}
		if err := accountFixturePair(&want, execution); err != nil {
			return Report{}, err
		}
	}
	invalidSpecs, err := invalidControlSpecs(selected)
	if err != nil {
		return Report{}, err
	}
	want.InvalidControls, want.InvalidControlsRejected = len(invalidSpecs), 0
	for _, spec := range invalidSpecs {
		controlRoot := filepath.Join(resolved, "invalid-controls", spec.name)
		execution, loadErr := runner.LoadPair(filepath.Join(controlRoot, "pairs"), runner.PairDirectoryName(spec.block.PairID))
		if loadErr != nil || execution.Admission != protocol.AdmissionInvalid || len(execution.ReasonCodes) == 0 {
			return Report{}, fmt.Errorf("%w: invalid control %s: %v", protocol.ErrInvalidEvidence, spec.name, loadErr)
		}
		bundles := make([]protocol.EvidenceBundle, 0, len(execution.Arms))
		for _, arm := range execution.Arms {
			bundle, verdict, verifyErr := oracle.VerifyRun(filepath.Join(controlRoot, "runs"), evidence.RunDirectoryName(arm.RunID))
			if verifyErr != nil ||
				!reflect.DeepEqual(verdict, arm.Verdict) {
				return Report{}, fmt.Errorf("%w: invalid control arm %s: %v", protocol.ErrInvalidEvidence, arm.RunID, verifyErr)
			}
			if spec.expectInvalidArms && verdict.Admission != protocol.AdmissionInvalid ||
				!spec.expectInvalidArms && verdict.Admission != protocol.AdmissionValid {
				return Report{}, fmt.Errorf("%w: invalid control arm disposition %s", protocol.ErrInvalidEvidence, arm.RunID)
			}
			bundles = append(bundles, bundle)
		}
		if err := auditPairBundles(spec.block, execution, bundles, spec.expectInputMatch); err != nil {
			return Report{}, err
		}
		want.InvalidControlsRejected++
	}
	if report.Kind == FullConformanceKind {
		liveBlocks, selectErr := SelectLiveSentinels(schedule)
		if selectErr != nil {
			return Report{}, selectErr
		}
		livePairRoot := filepath.Join(resolved, "live-sentinels", "pairs")
		liveRunRoot := filepath.Join(resolved, "live-sentinels", "runs")
		for _, block := range liveBlocks {
			execution, loadErr := runner.LoadPair(livePairRoot, runner.PairDirectoryName(block.PairID))
			if loadErr != nil || !reflect.DeepEqual(execution.Block, block) {
				return Report{}, fmt.Errorf("%w: live sentinel pair %s: %v", protocol.ErrInvalidEvidence, block.PairID, loadErr)
			}
			bundles := make([]protocol.EvidenceBundle, 0, len(execution.Arms))
			for index := range execution.Arms {
				arm := &execution.Arms[index]
				bundle, verdict, verifyErr := oracle.VerifyRun(liveRunRoot, evidence.RunDirectoryName(arm.RunID))
				fixtureHistory, historyKindErr := protocol.NativeExportIsFixture(bundle.NativeHistory.Export)
				if verifyErr != nil || historyKindErr != nil || fixtureHistory || bundle.Manifest.RunID != arm.RunID || !bundle.NativeHistory.ReplayCompatible ||
					!bundle.Execution.ReplayVerified || bundle.EffectiveInput.SourceSHA256 != report.HarnessBinarySHA256 ||
					bundle.NativeHistory.ReplayWorkerSHA256 != report.HarnessBinarySHA256 ||
					bundle.EffectiveInput.AgentBinarySHA256 != report.AgentBinarySHA256 || !reflect.DeepEqual(verdict, arm.Verdict) {
					return Report{}, fmt.Errorf("%w: live sentinel arm %s: %v", protocol.ErrInvalidEvidence, arm.RunID, verifyErr)
				}
				bundles = append(bundles, bundle)
			}
			if err := auditPairBundles(block, execution, bundles, true); err != nil {
				return Report{}, err
			}
			if err := accountLivePair(&want, execution); err != nil {
				return Report{}, err
			}
		}
	}
	if !reflect.DeepEqual(report, want) {
		return Report{}, fmt.Errorf("%w: matrix report counts differ from raw evidence", protocol.ErrInvalidEvidence)
	}
	return report, nil
}

func validMatrixKind(kind string) bool {
	return kind == FixtureConformanceKind || kind == FullConformanceKind
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func auditPairBundles(
	block protocol.PairBlock,
	execution runner.PairExecution,
	bundles []protocol.EvidenceBundle,
	expectInputMatch bool,
) error {
	if execution.ProtocolVersion != protocol.PublicationProtocolVersion || execution.Phase != protocol.PhasePublication ||
		!reflect.DeepEqual(execution.Block, block) || len(execution.Arms) != len(protocol.Topologies()) || len(bundles) != len(execution.Arms) {
		return fmt.Errorf("%w: pair reconstruction shape", protocol.ErrInvalidEvidence)
	}
	for index, arm := range execution.Arms {
		wantTopology := block.TopologyOrder[index]
		wantRunID := block.PairID + "/" + string(wantTopology)
		manifest := bundles[index].Manifest
		if arm.Order != index+1 || arm.Topology != wantTopology || arm.RunID != wantRunID || manifest.RunID != wantRunID ||
			manifest.PairID != block.PairID || manifest.ScheduleBlockID != block.ScheduleBlockID || manifest.Topology != wantTopology ||
			manifest.Case != block.Stratum.Case || manifest.Boundary != block.Stratum.Boundary || manifest.Probe != block.Stratum.Probe ||
			manifest.Fanout != block.Stratum.Fanout {
			return fmt.Errorf("%w: pair arm identity reconstruction", protocol.ErrInvalidEvidence)
		}
	}
	matched := bundles[0].EffectiveInput.MatchedWith(bundles[1].EffectiveInput) &&
		bundles[0].Manifest.LogicalOperationID == bundles[1].Manifest.LogicalOperationID &&
		bundles[0].Manifest.TrackerBeadID == bundles[1].Manifest.TrackerBeadID
	if matched != expectInputMatch {
		return fmt.Errorf("%w: pair matched-input reconstruction", protocol.ErrInvalidEvidence)
	}
	return nil
}

func inventoryArtifacts(root string) ([]Artifact, error) {
	return inventoryArtifactsExcluding(root, InventoryFile)
}

func inventoryArtifactsExcluding(root, excludedFile string) ([]Artifact, error) {
	root, err := resolveRoot(root)
	if err != nil {
		return nil, err
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rootHandle.Close() }()
	artifacts := make([]Artifact, 0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink in matrix evidence", protocol.ErrInvalidEvidence)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("%w: non-regular matrix artifact", protocol.ErrInvalidEvidence)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%w: matrix artifact path", protocol.ErrInvalidEvidence)
		}
		if filepath.ToSlash(relative) == excludedFile {
			return nil
		}
		data, err := sealedfs.ReadRegularFileOnce(rootHandle, relative, 64<<20)
		if err != nil {
			return err
		}
		artifacts = append(artifacts, Artifact{Path: filepath.ToSlash(relative), SHA256: sealedfs.HashBytes(data), Bytes: int64(len(data))})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(artifacts, func(first, second Artifact) int { return strings.Compare(first.Path, second.Path) })
	return artifacts, nil
}

func resolveRoot(root string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("%w: matrix root", protocol.ErrInvalidEvidence)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("%w: matrix root: %v", protocol.ErrInvalidEvidence, err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("%w: matrix root", protocol.ErrInvalidEvidence)
	}
	return resolved, nil
}

func hashForPath(artifacts []Artifact, path string) string {
	for _, artifact := range artifacts {
		if artifact.Path == path {
			return artifact.SHA256
		}
	}
	return ""
}
