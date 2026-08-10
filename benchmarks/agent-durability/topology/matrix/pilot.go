package matrix

import (
	"fmt"
	"reflect"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
)

const (
	PilotKind             = "topology-pilot-v1"
	PilotReportFile       = "pilot-report.json"
	PilotScheduleFile     = "frozen-pilot-schedule.json"
	PilotInventoryFile    = "pilot-inventory.json"
	PilotFreezeFile       = "publication-harness-freeze.json"
	PilotContractFile     = "frozen-topology-contract.json"
	PilotRegistrationFile = "frozen-preregistration.json"
)

type PilotScheduleAudit struct {
	Strata          int `json:"strata"`
	Pairs           int `json:"pairs"`
	ArmExecutions   int `json:"arm_executions"`
	DirectFirst     int `json:"direct_first"`
	ChildFirst      int `json:"child_first"`
	PairsPerStratum int `json:"pairs_per_stratum"`
}

type PilotReport struct {
	ProtocolVersion                string             `json:"protocol_version"`
	Kind                           string             `json:"kind"`
	CreatedAtUTC                   string             `json:"created_at_utc"`
	TrackerBeadID                  string             `json:"tracker_bead_id"`
	PublicationExcluded            bool               `json:"publication_excluded"`
	ExclusionReason                string             `json:"exclusion_reason"`
	Schedule                       PilotScheduleAudit `json:"schedule_audit"`
	AttemptedPairs                 int                `json:"attempted_pairs"`
	ValidPairs                     int                `json:"valid_pairs"`
	InvalidPairs                   int                `json:"invalid_pairs"`
	AttemptedArms                  int                `json:"attempted_arms"`
	ValidArms                      int                `json:"valid_arms"`
	UnsafeArms                     int                `json:"unsafe_arms"`
	UnsafeArmsDistinguished        int                `json:"unsafe_arms_distinguished"`
	ProtectedOrUnfaultedArms       int                `json:"protected_or_unfaulted_arms"`
	ProtectedOrUnfaultedArmsPassed int                `json:"protected_or_unfaulted_arms_passed"`
	HistoriesReplayed              int                `json:"histories_replayed"`
	Qualified                      bool               `json:"qualified"`
	HarnessBinarySHA256            string             `json:"harness_binary_sha256"`
	AgentBinarySHA256              string             `json:"agent_binary_sha256"`
	TemporalBinarySHA256           string             `json:"temporal_binary_sha256"`
	ContractSHA256                 string             `json:"contract_sha256"`
	PreregistrationSHA256          string             `json:"preregistration_sha256"`
	ScheduleSHA256                 string             `json:"schedule_sha256"`
	FreezeManifestSHA256           string             `json:"freeze_manifest_sha256,omitempty"`
}

func auditPilotSchedule(registration protocol.Preregistration, schedule protocol.Schedule) (PilotScheduleAudit, error) {
	if err := protocol.ValidateSchedule(registration, protocol.PhasePilot, schedule); err != nil {
		return PilotScheduleAudit{}, err
	}
	strata, err := protocol.BuildStrata(registration)
	if err != nil {
		return PilotScheduleAudit{}, err
	}
	type counts struct {
		pairs, directFirst, childFirst int
		slots                          map[int]bool
	}
	byStratum := make(map[string]*counts, len(strata))
	audit := PilotScheduleAudit{
		Strata: len(strata), Pairs: len(schedule.Blocks),
		ArmExecutions:   len(schedule.Blocks) * len(protocol.Topologies()),
		PairsPerStratum: registration.Population.PilotPairsPerStratum,
	}
	for index, block := range schedule.Blocks {
		if block.Index != index+1 || block.Reserve {
			return PilotScheduleAudit{}, fmt.Errorf("%w: pilot schedule block", protocol.ErrInvalidEvidence)
		}
		entry := byStratum[block.Stratum.ID]
		if entry == nil {
			entry = &counts{slots: make(map[int]bool)}
			byStratum[block.Stratum.ID] = entry
		}
		if entry.slots[block.Slot] || block.Slot > registration.Population.PilotPairsPerStratum {
			return PilotScheduleAudit{}, fmt.Errorf("%w: pilot slot", protocol.ErrInvalidEvidence)
		}
		entry.slots[block.Slot] = true
		entry.pairs++
		if block.TopologyOrder[0] == protocol.TopologyDirectActivity {
			entry.directFirst++
			audit.DirectFirst++
		} else {
			entry.childFirst++
			audit.ChildFirst++
		}
	}
	for _, stratum := range strata {
		entry := byStratum[stratum.ID]
		if entry == nil || entry.pairs != registration.Population.PilotPairsPerStratum ||
			len(entry.slots) != registration.Population.PilotPairsPerStratum ||
			entry.directFirst < 1 || entry.childFirst < 1 || absInt(entry.directFirst-entry.childFirst) > 1 {
			return PilotScheduleAudit{}, fmt.Errorf("%w: per-stratum pilot balance", protocol.ErrInvalidEvidence)
		}
	}
	if audit.Strata != registration.Population.ExpectedStrata ||
		audit.Pairs != registration.Population.ExpectedStrata*registration.Population.PilotPairsPerStratum ||
		audit.DirectFirst != audit.ChildFirst {
		return PilotScheduleAudit{}, fmt.Errorf("%w: global pilot balance", protocol.ErrInvalidEvidence)
	}
	return audit, nil
}

func accountPilotPair(report *PilotReport, execution runner.PairExecution) error {
	if report == nil {
		return fmt.Errorf("%w: nil pilot report", protocol.ErrInvalidEvidence)
	}
	report.AttemptedPairs++
	report.AttemptedArms += len(execution.Arms)
	if execution.Admission == protocol.AdmissionInvalid {
		report.InvalidPairs++
		return nil
	}
	if execution.Admission != protocol.AdmissionValid || len(execution.Arms) != len(protocol.Topologies()) {
		return fmt.Errorf("%w: pilot pair admission shape", protocol.ErrInvalidEvidence)
	}
	report.ValidPairs++
	for _, arm := range execution.Arms {
		verdict := arm.Verdict
		if verdict.Admission != protocol.AdmissionValid || verdict.Liveness != protocol.OutcomePass || verdict.Diagnosability != protocol.OutcomePass {
			return fmt.Errorf("%w: valid pilot arm disposition", protocol.ErrInvalidEvidence)
		}
		report.ValidArms++
		report.HistoriesReplayed++
		if execution.Block.Stratum.Probe == protocol.ProbeUnsafe {
			report.UnsafeArms++
			if verdict.Safety != protocol.OutcomeFail || verdict.EfficiencyEligible {
				return fmt.Errorf("%w: unsafe pilot arm did not distinguish", protocol.ErrInvalidEvidence)
			}
			report.UnsafeArmsDistinguished++
			continue
		}
		report.ProtectedOrUnfaultedArms++
		if verdict.Correctness == protocol.OutcomePass && verdict.Safety == protocol.OutcomePass && verdict.EfficiencyEligible {
			report.ProtectedOrUnfaultedArmsPassed++
		}
	}
	return nil
}

func finalizePilotQualification(report *PilotReport) {
	if report == nil {
		return
	}
	report.Qualified = report.Schedule.Pairs > 0 && report.Schedule.ArmExecutions == report.Schedule.Pairs*len(protocol.Topologies()) &&
		report.AttemptedPairs == report.Schedule.Pairs && report.ValidPairs == report.Schedule.Pairs && report.InvalidPairs == 0 &&
		report.AttemptedArms == report.Schedule.ArmExecutions && report.ValidArms == report.Schedule.ArmExecutions &&
		report.UnsafeArms+report.ProtectedOrUnfaultedArms == report.ValidArms &&
		report.UnsafeArmsDistinguished == report.UnsafeArms &&
		report.ProtectedOrUnfaultedArmsPassed == report.ProtectedOrUnfaultedArms &&
		report.HistoriesReplayed == report.ValidArms
}

func auditPilotArm(block protocol.PairBlock, arm runner.ArmRun, bundle protocol.EvidenceBundle, allowFixture bool) error {
	wantRunID := block.PairID + "/" + string(arm.Topology)
	manifest := bundle.Manifest
	if arm.RunID != wantRunID || manifest.RunID != wantRunID || manifest.PairID != block.PairID ||
		manifest.ScheduleBlockID != block.ScheduleBlockID || manifest.Topology != arm.Topology ||
		manifest.Case != block.Stratum.Case || manifest.Boundary != block.Stratum.Boundary ||
		manifest.Probe != block.Stratum.Probe || manifest.Fanout != block.Stratum.Fanout ||
		!reflect.DeepEqual(bundle.Verdict, arm.Verdict) {
		return fmt.Errorf("%w: pilot arm identity", protocol.ErrInvalidEvidence)
	}
	fixture, err := protocol.NativeExportIsFixture(bundle.NativeHistory.Export)
	if err != nil || fixture && !allowFixture || !bundle.NativeHistory.ReplayCompatible || !bundle.Execution.ReplayVerified {
		return fmt.Errorf("%w: pilot native history", protocol.ErrInvalidEvidence)
	}
	return nil
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
