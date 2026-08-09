// Package publication defines the frozen population and analysis protocol for
// the system-timed Agent Durability Lab v2 comparison.
package publication

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

const (
	ProtocolVersion          = "adl.publication.v2"
	PreservedProtocolVersion = "adl.publication.v1"
)

const (
	SystemTemporal   = "temporal"
	SystemPostgreSQL = "postgresql-queue"
)

const (
	scheduleAlgorithm   = "balanced-splitmix64-fisher-yates-v2-namespaced-ids"
	scheduleAlgorithmV1 = "balanced-splitmix64-fisher-yates-v1"
)

type PopulationPolicy struct {
	MinimumValidPairsPerStratum     int    `json:"minimum_valid_pairs_per_stratum"`
	MaximumAttemptedPairsPerStratum int    `json:"maximum_attempted_pairs_per_stratum"`
	PilotPairsPerStratum            int    `json:"pilot_pairs_per_stratum"`
	PublicationSeed                 uint64 `json:"publication_seed"`
	PilotSeed                       uint64 `json:"pilot_seed"`
	PilotNamespace                  string `json:"pilot_namespace,omitempty"`
	ScheduleAlgorithm               string `json:"schedule_algorithm"`
	Pairing                         string `json:"pairing"`
	StatisticalUnit                 string `json:"statistical_unit"`
	SystemExecution                 string `json:"system_execution"`
	ReservePolicy                   string `json:"reserve_policy"`
}

type TimingPolicy struct {
	Clock                     string `json:"clock"`
	UTCAnchors                bool   `json:"utc_anchors"`
	ExactBarriers             bool   `json:"exact_barriers"`
	SetupExcluded             bool   `json:"setup_excluded"`
	TeardownExcluded          bool   `json:"teardown_excluded"`
	LivenessDeadlinesOnly     bool   `json:"liveness_deadlines_only"`
	PeakQPSWindowMilliseconds int64  `json:"peak_qps_window_ms"`
}

type AdmissionPolicy struct {
	RequiredOutcomeDimensions []string `json:"required_outcome_dimensions"`
	InvalidPolicy             string   `json:"invalid_policy"`
	ExclusionPolicy           string   `json:"exclusion_policy"`
	StopRule                  string   `json:"stop_rule"`
}

type AnalysisPolicy struct {
	ConfidenceLevel    float64 `json:"confidence_level"`
	BootstrapResamples int     `json:"bootstrap_resamples"`
	BootstrapMethod    string  `json:"bootstrap_method"`
	AnalysisSeed       uint64  `json:"analysis_seed"`
	Paired             bool    `json:"paired"`
	ScalarWinner       bool    `json:"scalar_winner"`
	MultiplicityPolicy string  `json:"multiplicity_policy"`
	EfficiencyGate     string  `json:"efficiency_gate"`
	BinaryInterval     string  `json:"binary_interval"`
	RatioRule          string  `json:"ratio_rule"`
}

type LivenessPolicy struct {
	HealthyTaskLatencyBoundMilliseconds int64  `json:"healthy_task_latency_bound_ms"`
	BoundRole                           string `json:"bound_role"`
}

type HashPins struct {
	ContractV2SHA256       string `json:"contract_v2_sha256"`
	AdapterBaselineSHA256  string `json:"adapter_baseline_sha256"`
	PopulationConfigSHA256 string `json:"population_config_sha256"`
}

type Preregistration struct {
	ProtocolVersion  string                       `json:"protocol_version"`
	ContractVersion  string                       `json:"contract_version"`
	FrozenAtUTC      string                       `json:"frozen_at_utc"`
	RequiredSystems  []string                     `json:"required_systems"`
	Cases            []protocol.CaseID            `json:"cases"`
	Probes           []protocol.Probe             `json:"probes"`
	TrackByProbe     map[protocol.Probe]string    `json:"track_by_probe"`
	Population       PopulationPolicy             `json:"population"`
	Timing           TimingPolicy                 `json:"timing"`
	Admission        AdmissionPolicy              `json:"admission"`
	Analysis         AnalysisPolicy               `json:"analysis"`
	Liveness         LivenessPolicy               `json:"liveness"`
	PrimaryEstimands map[protocol.CaseID][]string `json:"primary_estimands"`
	RequiredEvidence []string                     `json:"required_evidence"`
	HostControls     []string                     `json:"host_controls"`
	Hashes           HashPins                     `json:"hashes"`
	ChangePolicy     string                       `json:"change_policy"`
	Supersedes       string                       `json:"supersedes,omitempty"`
	PilotLineage     string                       `json:"pilot_lineage,omitempty"`
	Falsifiers       []string                     `json:"falsifiers"`
}

type PairBlock struct {
	Index       int             `json:"index"`
	PairID      string          `json:"pair_id"`
	Case        protocol.CaseID `json:"case"`
	Probe       protocol.Probe  `json:"probe"`
	Slot        int             `json:"slot"`
	Reserve     bool            `json:"reserve"`
	SystemOrder []string        `json:"system_order"`
}

type Schedule struct {
	ProtocolVersion string      `json:"protocol_version"`
	Seed            uint64      `json:"seed"`
	Algorithm       string      `json:"algorithm"`
	Blocks          []PairBlock `json:"blocks"`
}

func LoadPreregistration(path string) (Preregistration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Preregistration{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var registration Preregistration
	if err := decoder.Decode(&registration); err != nil {
		return Preregistration{}, fmt.Errorf("decode publication preregistration: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Preregistration{}, fmt.Errorf("%w: preregistration has trailing JSON", protocol.ErrInvalidEvidence)
	}
	if err := registration.Validate(); err != nil {
		return Preregistration{}, err
	}
	return registration, nil
}

func (p Preregistration) Validate() error {
	if (p.ProtocolVersion != ProtocolVersion && p.ProtocolVersion != PreservedProtocolVersion) || p.ContractVersion != protocol.ContractVersion {
		return invalid("publication protocol version")
	}
	frozen, err := time.Parse(time.RFC3339Nano, p.FrozenAtUTC)
	if err != nil || frozen.Location() != time.UTC {
		return invalid("UTC freeze timestamp")
	}
	if !equalStrings(p.RequiredSystems, []string{SystemTemporal, SystemPostgreSQL}) ||
		!equalCases(p.Cases, protocol.Cases()) ||
		!equalProbes(p.Probes, []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected}) {
		return invalid("required systems, cases, or probes")
	}
	if p.TrackByProbe[protocol.ProbeUnfaulted] != "native-minimum-baseline" ||
		p.TrackByProbe[protocol.ProbeUnsafe] != "native-minimum-negative-control" ||
		p.TrackByProbe[protocol.ProbeProtected] != "portable-safety" || len(p.TrackByProbe) != 3 {
		return invalid("probe-to-track mapping")
	}
	population := p.Population
	wantScheduleAlgorithm := scheduleAlgorithm
	if p.ProtocolVersion == PreservedProtocolVersion {
		wantScheduleAlgorithm = scheduleAlgorithmV1
	}
	reserve := population.MaximumAttemptedPairsPerStratum - population.MinimumValidPairsPerStratum
	if population.MinimumValidPairsPerStratum < 30 || population.MaximumAttemptedPairsPerStratum < population.MinimumValidPairsPerStratum ||
		population.MinimumValidPairsPerStratum%2 != 0 || reserve%2 != 0 || population.PilotPairsPerStratum < 3 ||
		population.PublicationSeed == 0 || population.PilotSeed == 0 || population.PublicationSeed == population.PilotSeed ||
		population.ScheduleAlgorithm != wantScheduleAlgorithm || population.Pairing != "matched-case-probe-slot" ||
		population.StatisticalUnit != "independent-paired-episode" || population.SystemExecution != "sequential-within-pair" ||
		population.ReservePolicy != "use-next-predetermined-slot-only-after-invalid-pair" {
		return invalid("population policy")
	}
	if p.ProtocolVersion == ProtocolVersion && population.PilotNamespace == "" ||
		p.ProtocolVersion == PreservedProtocolVersion && population.PilotNamespace != "" {
		return invalid("pilot namespace")
	}
	if p.Timing.Clock != "go-monotonic-duration-with-utc-anchors" || !p.Timing.UTCAnchors || !p.Timing.ExactBarriers ||
		!p.Timing.SetupExcluded || !p.Timing.TeardownExcluded || !p.Timing.LivenessDeadlinesOnly || p.Timing.PeakQPSWindowMilliseconds < 1 {
		return invalid("timing policy")
	}
	if !equalStrings(p.Admission.RequiredOutcomeDimensions, []string{"correctness", "safety", "liveness", "diagnosability"}) ||
		p.Admission.InvalidPolicy != "retain-and-use-next-predetermined-reserve-slot" ||
		p.Admission.ExclusionPolicy != "preregistered-infrastructure-only-never-outcome-based" ||
		p.Admission.StopRule != "stop-at-thirty-valid-pairs-or-after-forty-attempted-pairs" {
		return invalid("admission policy")
	}
	if p.Analysis.ConfidenceLevel != 0.95 || p.Analysis.BootstrapResamples < 10_000 ||
		p.Analysis.BootstrapMethod != "paired-percentile-bootstrap-median-difference" || p.Analysis.AnalysisSeed == 0 ||
		!p.Analysis.Paired || p.Analysis.ScalarWinner || p.Analysis.MultiplicityPolicy != "descriptive-families-no-null-hypothesis-winner-claims" ||
		p.Analysis.EfficiencyGate != "all-thirty-matched-pairs-valid-and-both-arms-pass-four-outcomes" ||
		p.Analysis.BinaryInterval != "wilson-95-percent" || p.Analysis.RatioRule != "report-ratio-only-when-both-paired-values-are-positive" {
		return invalid("analysis policy")
	}
	if p.ProtocolVersion == ProtocolVersion {
		if p.Liveness.HealthyTaskLatencyBoundMilliseconds != 1000 ||
			p.Liveness.BoundRole != "infrastructure-liveness-gate-not-efficiency-estimand" ||
			p.Supersedes != PreservedProtocolVersion || p.PilotLineage == "" {
			return invalid("system-timed liveness policy or supersession lineage")
		}
	} else if p.Liveness != (LivenessPolicy{}) || p.Supersedes != "" || p.PilotLineage != "" {
		return invalid("preserved publication v1 lineage")
	}
	if len(p.PrimaryEstimands) != len(protocol.Cases()) {
		return invalid("primary estimand case count")
	}
	for _, benchmarkCase := range protocol.Cases() {
		if len(p.PrimaryEstimands[benchmarkCase]) == 0 {
			return invalid("primary estimand")
		}
	}
	if len(p.RequiredEvidence) < len(protocol.RawEvidenceFiles())+3 || len(p.HostControls) == 0 ||
		!validSHA256(p.Hashes.ContractV2SHA256) || !validSHA256(p.Hashes.AdapterBaselineSHA256) || !validSHA256(p.Hashes.PopulationConfigSHA256) ||
		p.ChangePolicy != "no-post-publication-result-changes" || len(p.Falsifiers) == 0 {
		return invalid("evidence, hash, host, change, or falsifier policy")
	}
	return nil
}

func (p Preregistration) Clone() Preregistration {
	clone := p
	clone.RequiredSystems = slices.Clone(p.RequiredSystems)
	clone.Cases = slices.Clone(p.Cases)
	clone.Probes = slices.Clone(p.Probes)
	clone.TrackByProbe = make(map[protocol.Probe]string, len(p.TrackByProbe))
	for key, value := range p.TrackByProbe {
		clone.TrackByProbe[key] = value
	}
	clone.Admission.RequiredOutcomeDimensions = slices.Clone(p.Admission.RequiredOutcomeDimensions)
	clone.PrimaryEstimands = make(map[protocol.CaseID][]string, len(p.PrimaryEstimands))
	for key, values := range p.PrimaryEstimands {
		clone.PrimaryEstimands[key] = slices.Clone(values)
	}
	clone.RequiredEvidence = slices.Clone(p.RequiredEvidence)
	clone.HostControls = slices.Clone(p.HostControls)
	clone.Falsifiers = slices.Clone(p.Falsifiers)
	return clone
}

func BuildSchedule(registration Preregistration) (Schedule, error) {
	if err := registration.Validate(); err != nil {
		return Schedule{}, err
	}
	random := splitMix64{state: registration.Population.PublicationSeed}
	blocks := make([]PairBlock, 0, len(registration.Cases)*len(registration.Probes)*registration.Population.MaximumAttemptedPairsPerStratum)
	for _, benchmarkCase := range registration.Cases {
		for _, probe := range registration.Probes {
			primary := balancedOrders(registration.Population.MinimumValidPairsPerStratum)
			shuffle(primary, &random)
			reserveCount := registration.Population.MaximumAttemptedPairsPerStratum - registration.Population.MinimumValidPairsPerStratum
			reserve := balancedOrders(reserveCount)
			shuffle(reserve, &random)
			orders := append(primary, reserve...)
			for slot, firstSystem := range orders {
				secondSystem := SystemTemporal
				if firstSystem == SystemTemporal {
					secondSystem = SystemPostgreSQL
				}
				blocks = append(blocks, PairBlock{
					PairID: fmt.Sprintf("publication-v2-pair/%s/%s/slot-%02d", benchmarkCase, probe, slot+1),
					Case:   benchmarkCase, Probe: probe, Slot: slot + 1,
					Reserve:     slot >= registration.Population.MinimumValidPairsPerStratum,
					SystemOrder: []string{firstSystem, secondSystem},
				})
			}
		}
	}
	shuffle(blocks, &random)
	for index := range blocks {
		blocks[index].Index = index + 1
	}
	return Schedule{ProtocolVersion: registration.ProtocolVersion, Seed: registration.Population.PublicationSeed, Algorithm: scheduleAlgorithm, Blocks: blocks}, nil
}

func BuildPilotSchedule(registration Preregistration) (Schedule, error) {
	if err := registration.Validate(); err != nil {
		return Schedule{}, err
	}
	random := splitMix64{state: registration.Population.PilotSeed}
	blocks := make([]PairBlock, 0, len(registration.Cases)*len(registration.Probes)*registration.Population.PilotPairsPerStratum)
	for _, benchmarkCase := range registration.Cases {
		for _, probe := range registration.Probes {
			first := SystemTemporal
			if random.next()%2 == 1 {
				first = SystemPostgreSQL
			}
			for slot := 1; slot <= registration.Population.PilotPairsPerStratum; slot++ {
				firstSystem := first
				if slot%2 == 0 {
					firstSystem = otherSystem(first)
				}
				blocks = append(blocks, PairBlock{
					PairID: fmt.Sprintf("%s/%s/%s/slot-%02d", registration.Population.PilotNamespace, benchmarkCase, probe, slot),
					Case:   benchmarkCase, Probe: probe, Slot: slot,
					SystemOrder: []string{firstSystem, otherSystem(firstSystem)},
				})
			}
		}
	}
	shuffle(blocks, &random)
	for index := range blocks {
		blocks[index].Index = index + 1
	}
	return Schedule{ProtocolVersion: registration.ProtocolVersion, Seed: registration.Population.PilotSeed, Algorithm: scheduleAlgorithm, Blocks: blocks}, nil
}

func otherSystem(systemID string) string {
	if systemID == SystemTemporal {
		return SystemPostgreSQL
	}
	return SystemTemporal
}

func balancedOrders(count int) []string {
	orders := make([]string, count)
	for index := range orders {
		if index < count/2 {
			orders[index] = SystemTemporal
		} else {
			orders[index] = SystemPostgreSQL
		}
	}
	return orders
}

type splitMix64 struct{ state uint64 }

func (s *splitMix64) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	value := s.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func shuffle[T any](values []T, random *splitMix64) {
	for index := len(values) - 1; index > 0; index-- {
		other := int(random.next() % uint64(index+1))
		values[index], values[other] = values[other], values[index]
	}
}

func invalid(field string) error {
	return fmt.Errorf("%w: invalid publication %s", protocol.ErrInvalidEvidence, field)
}

func equalStrings(got, want []string) bool { return slices.Equal(got, want) }

func equalCases(got, want []protocol.CaseID) bool { return slices.Equal(got, want) }

func equalProbes(got, want []protocol.Probe) bool { return slices.Equal(got, want) }

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}
