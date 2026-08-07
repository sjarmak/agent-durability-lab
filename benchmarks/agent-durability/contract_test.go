package benchmark

import (
	"encoding/json"
	"os"
	"testing"
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
