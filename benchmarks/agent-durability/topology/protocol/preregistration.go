package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	ContractFile                  = "topology-contract-v1.json"
	FrozenPreregistrationV1SHA256 = "6881a8687c42833a1e24cdc239fcda67f971596669759b699ee76a2a7330ab6b"
)

var requiredEvidenceFiles = []string{
	"manifest.json",
	"causal-events.jsonl",
	"lineage.json",
	"authority-state.json",
	"destination-state.json",
	"dependency-state.json",
	"workload-state.json",
	"fault-boundary.json",
	"native-history-or-journal-export.json",
	"process-observations.json",
	"effective-input.json",
	"verdict.json",
	"publication-timing.jsonl",
	"publication-execution.json",
	"publication-inventory.json",
}

type PopulationPolicy struct {
	MinimumValidPairsPerStratum     int    `json:"minimum_valid_pairs_per_stratum"`
	MaximumAttemptedPairsPerStratum int    `json:"maximum_attempted_pairs_per_stratum"`
	PilotPairsPerStratum            int    `json:"pilot_pairs_per_stratum"`
	PublicationSeed                 uint64 `json:"publication_seed"`
	PilotSeed                       uint64 `json:"pilot_seed"`
	ScheduleAlgorithm               string `json:"schedule_algorithm"`
	Pairing                         string `json:"pairing"`
	StatisticalUnit                 string `json:"statistical_unit"`
	ArmExecution                    string `json:"arm_execution"`
	ArmOrder                        string `json:"arm_order"`
	StratumOrder                    string `json:"stratum_order"`
	ReservePolicy                   string `json:"reserve_policy"`
	StopRule                        string `json:"stop_rule"`
	ExpectedStrata                  int    `json:"expected_strata"`
	ExpectedPrimaryValidPairs       int    `json:"expected_primary_valid_pairs"`
	ExpectedPrimaryArmExecutions    int    `json:"expected_primary_arm_executions"`
}

type ScalePolicy struct {
	FanoutSizes                    []int  `json:"fanout_sizes"`
	CanonicalFanout                int    `json:"canonical_fanout"`
	UnsafeAtCanonicalOnly          bool   `json:"unsafe_at_canonical_only"`
	SecondaryBoundariesAtCanonical bool   `json:"secondary_boundaries_at_canonical_only"`
	SelectionRule                  string `json:"selection_rule"`
	Rationale                      string `json:"rationale"`
	PostPilotSelection             string `json:"post_pilot_selection"`
}

type AdmissionPolicy struct {
	RequiredOutcomeDimensions []string `json:"required_outcome_dimensions"`
	EfficiencyGate            string   `json:"efficiency_gate"`
	UnsafeGate                string   `json:"unsafe_gate"`
	UnfaultedGate             string   `json:"unfaulted_gate"`
	ProtectedGate             string   `json:"protected_gate"`
	InvalidPolicy             string   `json:"invalid_policy"`
	InvalidReasons            []string `json:"invalid_reasons"`
	OutcomeBasedExclusion     string   `json:"outcome_based_exclusion"`
}

type DiagnosabilityPolicy struct {
	RequiredChain                  string `json:"required_chain"`
	LineageGapCountRequired        int    `json:"lineage_gap_count_required"`
	AdapterLogsCanDetermineVerdict bool   `json:"adapter_logs_can_determine_verdict"`
	NativeHistoryRequired          bool   `json:"native_history_required"`
	ReplayRequired                 bool   `json:"replay_required"`
}

type HashPins struct {
	TopologyContractV1SHA256 string `json:"topology_contract_v1_sha256"`
}

// Preregistration includes the fields that control scheduling and admission.
// Other frozen sections are retained as raw JSON so unknown top-level fields
// still fail closed without duplicating case-specific analysis policy here.
type Preregistration struct {
	ProtocolVersion       string               `json:"protocol_version"`
	ContractVersion       string               `json:"contract_version"`
	FrozenAtUTC           string               `json:"frozen_at_utc"`
	Question              string               `json:"question"`
	Arms                  []Topology           `json:"arms"`
	Cases                 []CaseID             `json:"cases"`
	Probes                []Probe              `json:"probes"`
	TrackByProbe          map[Probe]string     `json:"track_by_probe"`
	PrimaryBoundaryByCase map[CaseID]string    `json:"primary_boundary_by_case"`
	SecondaryBoundaries   map[CaseID][]string  `json:"secondary_boundaries_by_case"`
	Population            PopulationPolicy     `json:"population"`
	ScalePolicy           ScalePolicy          `json:"scale_policy"`
	Workload              json.RawMessage      `json:"workload"`
	RecoveryBounds        json.RawMessage      `json:"recovery_bounds"`
	Timing                json.RawMessage      `json:"timing"`
	Admission             AdmissionPolicy      `json:"admission"`
	Analysis              json.RawMessage      `json:"analysis"`
	Diagnosability        DiagnosabilityPolicy `json:"diagnosability"`
	PilotPolicy           json.RawMessage      `json:"pilot_policy"`
	HostControls          []string             `json:"host_controls"`
	RequiredEvidence      []string             `json:"required_evidence"`
	Hashes                HashPins             `json:"hashes"`
	ChangePolicy          string               `json:"change_policy"`
	ReviewAuthorization   string               `json:"review_authorization"`
	Falsifiers            []string             `json:"falsifiers"`
}

func LoadPreregistration(path string) (Preregistration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Preregistration{}, err
	}
	return DecodePreregistration(data)
}

func DecodePreregistration(data []byte) (Preregistration, error) {
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != FrozenPreregistrationV1SHA256 {
		return Preregistration{}, invalid("frozen preregistration hash")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var registration Preregistration
	if err := decoder.Decode(&registration); err != nil {
		return Preregistration{}, fmt.Errorf("decode topology preregistration: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Preregistration{}, invalid("preregistration trailing JSON")
	}
	if err := registration.Validate(); err != nil {
		return Preregistration{}, err
	}
	return registration, nil
}

func (p Preregistration) Validate() error {
	if p.ProtocolVersion != PublicationProtocolVersion || p.ContractVersion != ContractVersion || p.Question == "" {
		return invalid("preregistration version or question")
	}
	frozen, err := time.Parse(time.RFC3339Nano, p.FrozenAtUTC)
	if err != nil || frozen.Location() != time.UTC {
		return invalid("preregistration UTC freeze timestamp")
	}
	if !slices.Equal(p.Arms, Topologies()) || !slices.Equal(p.Cases, Cases()) || !slices.Equal(p.Probes, Probes()) {
		return invalid("arms, cases, or probes")
	}
	if len(p.TrackByProbe) != len(Probes()) || p.TrackByProbe[ProbeUnfaulted] != "matched-policy-no-injected-fault" ||
		p.TrackByProbe[ProbeUnsafe] != "common-negative-control" || p.TrackByProbe[ProbeProtected] != "matched-application-and-destination-safety" {
		return invalid("probe tracks")
	}
	if err := p.validateBoundaries(); err != nil {
		return err
	}
	population := p.Population
	if population.MinimumValidPairsPerStratum != 30 || population.MaximumAttemptedPairsPerStratum != 40 ||
		population.PilotPairsPerStratum != 3 || population.PublicationSeed == 0 || population.PilotSeed == 0 ||
		population.PublicationSeed == population.PilotSeed || population.ScheduleAlgorithm != ScheduleAlgorithm ||
		population.Pairing != "matched-case-boundary-probe-fanout-slot" || population.StatisticalUnit != "independent-paired-episode" ||
		population.ArmExecution != "sequential-within-pair-after-condition-based-idle-readiness" ||
		population.ArmOrder != "randomized-and-balanced-within-stratum" || population.StratumOrder != "randomized-with-recorded-seed" ||
		population.ReservePolicy != "use-next-predetermined-slot-only-after-invalid-pair" ||
		population.StopRule != "stop-at-thirty-valid-pairs-or-after-forty-attempted-pairs" || population.ExpectedStrata != 88 ||
		population.ExpectedPrimaryValidPairs != 2640 || population.ExpectedPrimaryArmExecutions != 5280 {
		return invalid("population policy")
	}
	scale := p.ScalePolicy
	if !slices.Equal(scale.FanoutSizes, []int{8, 32, 128}) || scale.CanonicalFanout != 32 || !scale.UnsafeAtCanonicalOnly ||
		!scale.SecondaryBoundariesAtCanonical || scale.SelectionRule == "" || scale.Rationale == "" || scale.PostPilotSelection != "forbidden" {
		return invalid("scale policy")
	}
	if !slices.Equal(p.Admission.RequiredOutcomeDimensions, []string{"correctness", "safety", "liveness", "diagnosability"}) ||
		p.Admission.EfficiencyGate == "" || p.Admission.UnsafeGate == "" || p.Admission.UnfaultedGate == "" ||
		p.Admission.ProtectedGate == "" || p.Admission.InvalidPolicy == "" || len(p.Admission.InvalidReasons) != 7 ||
		p.Admission.OutcomeBasedExclusion != "forbidden" {
		return invalid("admission policy")
	}
	if p.Diagnosability.RequiredChain == "" || p.Diagnosability.LineageGapCountRequired != 0 ||
		p.Diagnosability.AdapterLogsCanDetermineVerdict || !p.Diagnosability.NativeHistoryRequired || !p.Diagnosability.ReplayRequired {
		return invalid("diagnosability policy")
	}
	if !slices.Equal(p.RequiredEvidence, requiredEvidenceFiles) || !validSHA256(p.Hashes.TopologyContractV1SHA256) ||
		len(p.HostControls) == 0 || len(p.Falsifiers) == 0 || p.ChangePolicy == "" || p.ReviewAuthorization == "" ||
		len(p.Workload) == 0 || len(p.RecoveryBounds) == 0 || len(p.Timing) == 0 || len(p.Analysis) == 0 || len(p.PilotPolicy) == 0 {
		return invalid("evidence, hash, host, change, or review policy")
	}
	strata, err := BuildStrata(p)
	if err != nil || len(strata) != population.ExpectedStrata {
		return invalid("derived strata")
	}
	return nil
}

func (p Preregistration) validateBoundaries() error {
	if len(p.PrimaryBoundaryByCase) != len(Cases()) || len(p.SecondaryBoundaries) != len(Cases()) {
		return invalid("boundary case coverage")
	}
	seen := make(map[string]bool)
	for _, benchmarkCase := range Cases() {
		primary := p.PrimaryBoundaryByCase[benchmarkCase]
		if primary == "" || seen[primary] {
			return invalid("primary boundary identity")
		}
		seen[primary] = true
		secondary, ok := p.SecondaryBoundaries[benchmarkCase]
		if !ok {
			return invalid("secondary boundary case coverage")
		}
		for _, boundary := range secondary {
			if boundary == "" || boundary == primary || seen[boundary] {
				return invalid("secondary boundary identity")
			}
			seen[boundary] = true
		}
	}
	return nil
}

func (p Preregistration) Clone() Preregistration {
	clone := p
	clone.Arms = slices.Clone(p.Arms)
	clone.Cases = slices.Clone(p.Cases)
	clone.Probes = slices.Clone(p.Probes)
	clone.TrackByProbe = cloneMap(p.TrackByProbe)
	clone.PrimaryBoundaryByCase = cloneMap(p.PrimaryBoundaryByCase)
	clone.SecondaryBoundaries = make(map[CaseID][]string, len(p.SecondaryBoundaries))
	for key, values := range p.SecondaryBoundaries {
		clone.SecondaryBoundaries[key] = slices.Clone(values)
	}
	clone.ScalePolicy.FanoutSizes = slices.Clone(p.ScalePolicy.FanoutSizes)
	clone.Admission.RequiredOutcomeDimensions = slices.Clone(p.Admission.RequiredOutcomeDimensions)
	clone.Admission.InvalidReasons = slices.Clone(p.Admission.InvalidReasons)
	clone.Workload = slices.Clone(p.Workload)
	clone.RecoveryBounds = slices.Clone(p.RecoveryBounds)
	clone.Timing = slices.Clone(p.Timing)
	clone.Analysis = slices.Clone(p.Analysis)
	clone.PilotPolicy = slices.Clone(p.PilotPolicy)
	clone.HostControls = slices.Clone(p.HostControls)
	clone.RequiredEvidence = slices.Clone(p.RequiredEvidence)
	clone.Falsifiers = slices.Clone(p.Falsifiers)
	return clone
}

func RequiredEvidenceFiles() []string { return slices.Clone(requiredEvidenceFiles) }

func VerifyContractHash(registration Preregistration, root string) error {
	if err := registration.Validate(); err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(root, ContractFile))
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != registration.Hashes.TopologyContractV1SHA256 {
		return invalid("topology contract hash")
	}
	return nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == hex.EncodeToString(decoded)
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
