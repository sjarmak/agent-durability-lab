package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestContractV1HasUniqueRequiredCasesSystemsAndEvidence(t *testing.T) {
	data, err := os.ReadFile("contract-v1.json")
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	var contract struct {
		Version string `json:"contract_version"`
		Tracks  []struct {
			ID string `json:"id"`
		} `json:"tracks"`
		Cases []struct {
			ID              string `json:"id"`
			Invariant       string `json:"invariant"`
			FailureBoundary string `json:"failure_boundary"`
			Falsifier       string `json:"falsifier"`
		} `json:"cases"`
		Systems []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"systems"`
		RequiredEvidence  []string `json:"required_evidence"`
		PrimaryMetrics    []string `json:"primary_metrics"`
		ParityGateMetrics []string `json:"parity_gated_metrics"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("decode contract: %v", err)
	}
	if contract.Version != "adl.cross-system.v1" {
		t.Fatalf("contract version = %q", contract.Version)
	}
	assertUniqueNonempty(t, "track", len(contract.Tracks), func(index int) string { return contract.Tracks[index].ID })
	assertUniqueNonempty(t, "case", len(contract.Cases), func(index int) string { return contract.Cases[index].ID })
	for _, benchmarkCase := range contract.Cases {
		if benchmarkCase.Invariant == "" || benchmarkCase.FailureBoundary == "" || benchmarkCase.Falsifier == "" {
			t.Errorf("case %q lacks invariant, boundary, or falsifier", benchmarkCase.ID)
		}
	}
	assertUniqueNonempty(t, "system", len(contract.Systems), func(index int) string { return contract.Systems[index].ID })
	for _, system := range contract.Systems {
		if system.Status == "" {
			t.Errorf("system %q lacks implementation status", system.ID)
		}
	}
	if len(contract.RequiredEvidence) < 8 || len(contract.PrimaryMetrics) == 0 || len(contract.ParityGateMetrics) == 0 {
		t.Fatalf("contract evidence/metrics are incomplete: %+v", contract)
	}
	for _, name := range append(protocol.RawEvidenceFiles(), protocol.VerdictFile) {
		if !hasValue(contract.RequiredEvidence, name) {
			t.Errorf("contract required_evidence lacks %q", name)
		}
	}
}

func TestContractV2DefinesAuthorityAndRecoveryDynamicsWithoutReplacingV1(t *testing.T) {
	data, err := os.ReadFile("contract-v2.json")
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	var contract struct {
		Version          string `json:"contract_version"`
		PreservesVersion string `json:"preserves_contract"`
		Suites           []struct {
			ID string `json:"id"`
		} `json:"suites"`
		Cases []struct {
			ID                 string            `json:"id"`
			Suite              string            `json:"suite"`
			Decision           string            `json:"decision"`
			Invariant          string            `json:"invariant"`
			FailureBoundary    string            `json:"failure_boundary"`
			Oracle             []string          `json:"oracle"`
			NegativeControl    string            `json:"negative_control"`
			ProtectedMechanism string            `json:"protected_mechanism"`
			Responsibility     map[string]string `json:"responsibility"`
			Falsifier          string            `json:"falsifier"`
			StatisticalUnit    string            `json:"statistical_unit"`
		} `json:"cases"`
		Systems []struct {
			ID       string `json:"id"`
			Required bool   `json:"required"`
			Status   string `json:"status"`
		} `json:"systems"`
		RequiredEvidence  []string `json:"required_evidence"`
		OutcomeDimensions []string `json:"outcome_dimensions"`
		LatencyMetrics    []string `json:"latency_metrics"`
		LoadMetrics       []string `json:"load_metrics"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("decode contract: %v", err)
	}
	if contract.Version != "adl.cross-system.v2" || contract.PreservesVersion != "adl.cross-system.v1" {
		t.Fatalf("version boundary = %q preserving %q", contract.Version, contract.PreservesVersion)
	}
	assertUniqueNonempty(t, "suite", len(contract.Suites), func(index int) string { return contract.Suites[index].ID })
	assertUniqueNonempty(t, "case", len(contract.Cases), func(index int) string { return contract.Cases[index].ID })
	wantCases := []string{
		"aba-reacquisition",
		"layered-retry-amplification",
		"outage-backlog-recovery",
		"backpressure-overload",
		"poison-work-isolation",
		"silent-progress",
	}
	caseIDs := make(map[string]bool, len(contract.Cases))
	for _, benchmarkCase := range contract.Cases {
		caseIDs[benchmarkCase.ID] = true
	}
	for _, want := range wantCases {
		if !caseIDs[want] {
			t.Errorf("contract cases lack %q", want)
		}
	}
	for _, benchmarkCase := range contract.Cases {
		if benchmarkCase.Suite == "" || benchmarkCase.Decision == "" || benchmarkCase.Invariant == "" ||
			benchmarkCase.FailureBoundary == "" || len(benchmarkCase.Oracle) == 0 ||
			benchmarkCase.NegativeControl == "" || benchmarkCase.ProtectedMechanism == "" ||
			len(benchmarkCase.Responsibility) == 0 || benchmarkCase.Falsifier == "" || benchmarkCase.StatisticalUnit == "" {
			t.Errorf("case %q lacks a required experiment contract field", benchmarkCase.ID)
		}
	}
	assertUniqueNonempty(t, "system", len(contract.Systems), func(index int) string { return contract.Systems[index].ID })
	requiredSystems := make(map[string]bool, len(contract.Systems))
	for _, system := range contract.Systems {
		if system.Status == "" {
			t.Errorf("system %q lacks status", system.ID)
		}
		requiredSystems[system.ID] = system.Required
	}
	for _, systemID := range []string{"temporal", "postgresql-queue"} {
		if !requiredSystems[systemID] {
			t.Errorf("system %q is not required", systemID)
		}
	}
	for _, dimension := range []string{"correctness", "safety", "liveness", "diagnosability"} {
		if !hasValue(contract.OutcomeDimensions, dimension) {
			t.Errorf("outcome_dimensions lacks %q", dimension)
		}
	}
	for _, metric := range []string{"queue_latency_ms", "execution_latency_ms", "recovery_latency_ms", "end_to_end_latency_ms"} {
		if !hasValue(contract.LatencyMetrics, metric) {
			t.Errorf("latency_metrics lacks %q", metric)
		}
	}
	for _, metric := range []string{"amplification_factor", "peak_qps", "peak_retry_concurrency", "backlog_integral_ms", "healthy_task_latency_ms"} {
		if !hasValue(contract.LoadMetrics, metric) {
			t.Errorf("load_metrics lacks %q", metric)
		}
	}
	for _, name := range []string{
		"manifest.json", "causal-events.jsonl", "authority-state.json", "dependency-state.json",
		"workload-state.json", "fault-boundary.json", "native-history-or-journal-export.json",
		"process-observations.json", "effective-input.json", "verdict.json",
	} {
		if !hasValue(contract.RequiredEvidence, name) {
			t.Errorf("required_evidence lacks %q", name)
		}
	}
}

type topologyContractFile struct {
	Version    string `json:"contract_version"`
	Topologies []struct {
		ID                string `json:"id"`
		ParentSchedules   string `json:"parent_schedules"`
		WorkActivity      string `json:"work_activity"`
		AdditionalRetries bool   `json:"additional_retries"`
	} `json:"topologies"`
	Workload struct {
		FixedFanout     bool  `json:"fixed_fanout"`
		DynamicFanout   bool  `json:"dynamic_fanout"`
		FanoutSizes     []int `json:"fanout_sizes"`
		CanonicalFanout int   `json:"canonical_fanout"`
	} `json:"workload"`
	Cases                []topologyCaseContract `json:"cases"`
	FullRecoveryDynamics []string               `json:"full_recovery_dynamics"`
	OutcomeDimensions    []string               `json:"outcome_dimensions"`
	RequiredEvidence     []string               `json:"required_evidence"`
}

type topologyCaseContract struct {
	ID                 string            `json:"id"`
	Decision           string            `json:"decision"`
	Invariant          string            `json:"invariant"`
	FailureBoundaries  []string          `json:"failure_boundaries"`
	Oracle             []string          `json:"oracle"`
	NegativeControl    string            `json:"negative_control"`
	ProtectedMechanism string            `json:"protected_mechanism"`
	Metrics            []string          `json:"metrics"`
	Responsibility     map[string]string `json:"responsibility"`
	Falsifier          string            `json:"falsifier"`
	StatisticalUnit    string            `json:"statistical_unit"`
}

type topologyPreregistrationFile struct {
	ProtocolVersion           string              `json:"protocol_version"`
	ContractVersion           string              `json:"contract_version"`
	Arms                      []string            `json:"arms"`
	Cases                     []string            `json:"cases"`
	Probes                    []string            `json:"probes"`
	PrimaryBoundaryByCase     map[string]string   `json:"primary_boundary_by_case"`
	SecondaryBoundariesByCase map[string][]string `json:"secondary_boundaries_by_case"`
	Population                topologyPopulation  `json:"population"`
	ScalePolicy               topologyScalePolicy `json:"scale_policy"`
	Timing                    struct {
		ExactBarriers         bool `json:"exact_barriers"`
		LivenessDeadlinesOnly bool `json:"liveness_deadlines_only"`
	} `json:"timing"`
	Admission struct {
		RequiredOutcomeDimensions []string `json:"required_outcome_dimensions"`
		InvalidPolicy             string   `json:"invalid_policy"`
		EfficiencyGate            string   `json:"efficiency_gate"`
	} `json:"admission"`
	Analysis struct {
		Paired             bool    `json:"paired"`
		ConfidenceLevel    float64 `json:"confidence_level"`
		BootstrapResamples int     `json:"bootstrap_resamples"`
		ScalarWinner       bool    `json:"scalar_winner"`
		MultiplicityPolicy string  `json:"multiplicity_policy"`
	} `json:"analysis"`
	PilotPolicy struct {
		PublicationExcluded      bool `json:"publication_excluded"`
		MaySelectFavorableScales bool `json:"may_select_favorable_scales"`
	} `json:"pilot_policy"`
	Hashes struct {
		Contract string `json:"topology_contract_v1_sha256"`
	} `json:"hashes"`
	ChangePolicy string   `json:"change_policy"`
	Falsifiers   []string `json:"falsifiers"`
}

type topologyPopulation struct {
	MinimumValidPairsPerStratum     int    `json:"minimum_valid_pairs_per_stratum"`
	MaximumAttemptedPairsPerStratum int    `json:"maximum_attempted_pairs_per_stratum"`
	PilotPairsPerStratum            int    `json:"pilot_pairs_per_stratum"`
	Pairing                         string `json:"pairing"`
	StatisticalUnit                 string `json:"statistical_unit"`
	ExpectedStrata                  int    `json:"expected_strata"`
	ExpectedPrimaryValidPairs       int    `json:"expected_primary_valid_pairs"`
	ExpectedPrimaryArmExecutions    int    `json:"expected_primary_arm_executions"`
}

type topologyScalePolicy struct {
	FanoutSizes                        []int  `json:"fanout_sizes"`
	CanonicalFanout                    int    `json:"canonical_fanout"`
	UnsafeAtCanonicalOnly              bool   `json:"unsafe_at_canonical_only"`
	SecondaryBoundariesAtCanonicalOnly bool   `json:"secondary_boundaries_at_canonical_only"`
	SelectionRule                      string `json:"selection_rule"`
}

var topologyCaseIDs = []string{
	"join-barrier", "incremental-partial-reduction", "queued-executing-supersession", "destructive-transition",
	"crash-recovery-boundaries", "layered-retry-amplification", "outage-backlog-herd-recovery",
	"backpressure-overload", "poison-work-isolation", "silent-progress",
}

func TestTopologyContractV1DefinesMatchedTemporalArmsAndScale(t *testing.T) {
	contract := readJSONFile[topologyContractFile](t, "topology-contract-v1.json")
	if contract.Version != "adl.temporal-topology.v1" || len(contract.Topologies) != 2 {
		t.Fatalf("contract identity/topologies = %q/%d", contract.Version, len(contract.Topologies))
	}
	assertUniqueNonempty(t, "topology", len(contract.Topologies), func(index int) string { return contract.Topologies[index].ID })
	for _, topology := range contract.Topologies {
		if topology.ParentSchedules == "" || topology.WorkActivity != "hermetic-agent-work" || topology.AdditionalRetries {
			t.Errorf("topology %q does not preserve the matched work-Activity boundary: %+v", topology.ID, topology)
		}
	}
	if !contract.Workload.FixedFanout || contract.Workload.DynamicFanout || contract.Workload.CanonicalFanout != 32 ||
		len(contract.Workload.FanoutSizes) != 3 || contract.Workload.FanoutSizes[0] != 8 ||
		contract.Workload.FanoutSizes[1] != 32 || contract.Workload.FanoutSizes[2] != 128 {
		t.Fatalf("workload scale policy = %+v", contract.Workload)
	}
}

func TestTopologyContractV1DefinesEveryCaseAndRecoveryFamily(t *testing.T) {
	contract := readJSONFile[topologyContractFile](t, "topology-contract-v1.json")
	assertUniqueNonempty(t, "case", len(contract.Cases), func(index int) string { return contract.Cases[index].ID })
	caseIDs := make(map[string]bool, len(contract.Cases))
	for _, benchmarkCase := range contract.Cases {
		caseIDs[benchmarkCase.ID] = true
		if benchmarkCase.Decision == "" || benchmarkCase.Invariant == "" || len(benchmarkCase.FailureBoundaries) == 0 ||
			len(benchmarkCase.Oracle) == 0 || benchmarkCase.NegativeControl == "" || benchmarkCase.ProtectedMechanism == "" ||
			len(benchmarkCase.Metrics) == 0 || len(benchmarkCase.Responsibility) == 0 || benchmarkCase.Falsifier == "" ||
			benchmarkCase.StatisticalUnit != "one independent matched topology episode" {
			t.Errorf("case %q lacks a required experiment contract field", benchmarkCase.ID)
		}
	}
	for _, want := range topologyCaseIDs {
		if !caseIDs[want] {
			t.Errorf("topology contract cases lack %q", want)
		}
	}
	for _, want := range topologyCaseIDs[4:] {
		if !hasValue(contract.FullRecoveryDynamics, want) {
			t.Errorf("full_recovery_dynamics lacks %q", want)
		}
	}
}

func TestTopologyContractV1DefinesAdmissionDimensionsAndEvidence(t *testing.T) {
	contract := readJSONFile[topologyContractFile](t, "topology-contract-v1.json")
	for _, dimension := range []string{"correctness", "safety", "liveness", "diagnosability", "efficiency"} {
		if !hasValue(contract.OutcomeDimensions, dimension) {
			t.Errorf("outcome_dimensions lacks %q", dimension)
		}
	}
	for _, name := range []string{
		"manifest.json", "causal-events.jsonl", "lineage.json", "workload-state.json", "fault-boundary.json",
		"destination-state.json", "process-observations.json", "native-history-or-journal-export.json",
		"effective-input.json", "verdict.json",
	} {
		if !hasValue(contract.RequiredEvidence, name) {
			t.Errorf("required_evidence lacks %q", name)
		}
	}
}

func TestTopologyPreregistrationV1FreezesPopulationAndScale(t *testing.T) {
	registration := readJSONFile[topologyPreregistrationFile](t, "topology-preregistration-v1.json")
	if registration.ProtocolVersion != "adl.temporal-topology.publication.v1" ||
		registration.ContractVersion != "adl.temporal-topology.v1" || len(registration.Arms) != 2 || len(registration.Probes) != 3 {
		t.Fatalf("protocol/contract/arms/probes = %q/%q/%v/%v", registration.ProtocolVersion, registration.ContractVersion, registration.Arms, registration.Probes)
	}
	if len(registration.Cases) != 10 || len(registration.PrimaryBoundaryByCase) != len(registration.Cases) ||
		len(registration.SecondaryBoundariesByCase) != len(registration.Cases) {
		t.Fatalf("case/boundary coverage = %d/%d/%d", len(registration.Cases), len(registration.PrimaryBoundaryByCase), len(registration.SecondaryBoundariesByCase))
	}
	if registration.Population.MinimumValidPairsPerStratum != 30 || registration.Population.MaximumAttemptedPairsPerStratum != 40 ||
		registration.Population.PilotPairsPerStratum != 3 || registration.Population.Pairing != "matched-case-boundary-probe-fanout-slot" ||
		registration.Population.StatisticalUnit != "independent-paired-episode" {
		t.Fatalf("population policy = %+v", registration.Population)
	}
	if len(registration.ScalePolicy.FanoutSizes) != 3 || registration.ScalePolicy.FanoutSizes[0] != 8 ||
		registration.ScalePolicy.FanoutSizes[1] != 32 || registration.ScalePolicy.FanoutSizes[2] != 128 ||
		registration.ScalePolicy.CanonicalFanout != 32 || !registration.ScalePolicy.UnsafeAtCanonicalOnly ||
		!registration.ScalePolicy.SecondaryBoundariesAtCanonicalOnly || registration.ScalePolicy.SelectionRule == "" {
		t.Fatalf("scale policy = %+v", registration.ScalePolicy)
	}
}

func TestTopologyPreregistrationV1PopulationArithmetic(t *testing.T) {
	registration := readJSONFile[topologyPreregistrationFile](t, "topology-preregistration-v1.json")
	secondaryBoundaries := 0
	for _, caseID := range registration.Cases {
		if registration.PrimaryBoundaryByCase[caseID] == "" {
			t.Errorf("case %q lacks a primary boundary", caseID)
		}
		secondaryBoundaries += len(registration.SecondaryBoundariesByCase[caseID])
	}
	wantStrata := len(registration.Cases)*len(registration.ScalePolicy.FanoutSizes)*2 +
		len(registration.Cases) + secondaryBoundaries*2
	if secondaryBoundaries != 9 || registration.Population.ExpectedStrata != wantStrata ||
		registration.Population.ExpectedPrimaryValidPairs != wantStrata*registration.Population.MinimumValidPairsPerStratum ||
		registration.Population.ExpectedPrimaryArmExecutions != registration.Population.ExpectedPrimaryValidPairs*len(registration.Arms) {
		t.Fatalf("frozen population arithmetic = secondary %d, population %+v", secondaryBoundaries, registration.Population)
	}
}

func TestTopologyPreregistrationV1FreezesAdmissionAndAnalysis(t *testing.T) {
	registration := readJSONFile[topologyPreregistrationFile](t, "topology-preregistration-v1.json")
	if !registration.Timing.ExactBarriers || !registration.Timing.LivenessDeadlinesOnly {
		t.Fatalf("timing policy = %+v", registration.Timing)
	}
	for _, dimension := range []string{"correctness", "safety", "liveness", "diagnosability"} {
		if !hasValue(registration.Admission.RequiredOutcomeDimensions, dimension) {
			t.Errorf("admission dimensions lack %q", dimension)
		}
	}
	if registration.Admission.InvalidPolicy == "" || registration.Admission.EfficiencyGate == "" ||
		!registration.Analysis.Paired || registration.Analysis.ConfidenceLevel != 0.95 ||
		registration.Analysis.BootstrapResamples != 20000 || registration.Analysis.ScalarWinner ||
		registration.Analysis.MultiplicityPolicy == "" || !registration.PilotPolicy.PublicationExcluded ||
		registration.PilotPolicy.MaySelectFavorableScales || registration.ChangePolicy == "" || len(registration.Falsifiers) == 0 {
		t.Fatalf("preregistration policy is incomplete: %+v", registration)
	}
}

func TestTopologyPreregistrationV1MatchesContractHashAndBoundaries(t *testing.T) {
	registration := readJSONFile[topologyPreregistrationFile](t, "topology-preregistration-v1.json")
	contractData, err := os.ReadFile("topology-contract-v1.json")
	if err != nil {
		t.Fatalf("read hashed topology contract: %v", err)
	}
	contractDigest := sha256.Sum256(contractData)
	if got := hex.EncodeToString(contractDigest[:]); got != registration.Hashes.Contract {
		t.Fatalf("contract hash = %q, want %q", registration.Hashes.Contract, got)
	}
	contract := readJSONFile[topologyContractFile](t, "topology-contract-v1.json")
	contractBoundaries := make(map[string][]string, len(contract.Cases))
	for _, benchmarkCase := range contract.Cases {
		contractBoundaries[benchmarkCase.ID] = benchmarkCase.FailureBoundaries
	}
	for _, caseID := range registration.Cases {
		boundaries := append([]string{registration.PrimaryBoundaryByCase[caseID]}, registration.SecondaryBoundariesByCase[caseID]...)
		for _, boundary := range boundaries {
			if !hasValue(contractBoundaries[caseID], boundary) {
				t.Errorf("preregistered boundary %q/%q is absent from the contract", caseID, boundary)
			}
		}
	}
}

func readJSONFile[T any](t *testing.T, path string) T {
	t.Helper()
	var value T
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return value
}

func assertUniqueNonempty(t *testing.T, kind string, count int, value func(int) string) {
	t.Helper()
	if count == 0 {
		t.Fatalf("contract has no %ss", kind)
	}
	seen := make(map[string]bool, count)
	for index := range count {
		item := value(index)
		if item == "" || seen[item] {
			t.Errorf("%s %d has empty or duplicate ID %q", kind, index, item)
		}
		seen[item] = true
	}
}

func hasValue(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
