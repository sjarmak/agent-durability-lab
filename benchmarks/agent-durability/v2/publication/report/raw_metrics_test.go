package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/publication"
)

func TestPilotV4ReconstructsEveryPreregisteredPrimaryEstimand(t *testing.T) {
	root := filepath.Join("..", "..", "..", "evidence", "publication-v2-pilot-20260809-v4")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		t.Skip("preserved pilot v4 evidence is not present")
	}
	registration, err := publication.LoadPreregistration(filepath.Join("..", "..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "pairs", "*", publication.PublicationExecutionFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 54 {
		t.Fatalf("pilot pair evidence = %d, want 54", len(paths))
	}
	backlogRuns := 0
	abaFaultedRuns := 0
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var pair publication.PairExecution
		if err := json.Unmarshal(data, &pair); err != nil {
			t.Fatal(err)
		}
		for _, system := range pair.Systems {
			metrics, err := ReconstructPrimaryMetrics(system.EvidenceDir, registration.PrimaryEstimands[pair.Case])
			if err != nil {
				t.Fatalf("%s %s/%s: %v", system.SystemID, pair.Case, pair.Probe, err)
			}
			if pair.Case == "outage-backlog-recovery" && pair.Probe != "unfaulted" {
				backlogRuns++
				if metrics["backlog_integral_ms"] <= 0 || metrics["backlog_drain_p90_ms"] <= 0 {
					t.Fatalf("%s %s/%s lacks reconstructed backlog: %+v", system.SystemID, pair.Case, pair.Probe, metrics)
				}
			}
			if pair.Case == "aba-reacquisition" && pair.Probe != "unfaulted" {
				abaFaultedRuns++
				if metrics["recovery_latency_ms"] <= 0 || metrics["end_to_end_latency_ms"] <= 0 {
					t.Fatalf("%s %s/%s lacks generation-9 completion latency: %+v", system.SystemID, pair.Case, pair.Probe, metrics)
				}
			}
		}
	}
	if backlogRuns != 12 {
		t.Fatalf("faulted outage system runs = %d, want 12", backlogRuns)
	}
	if abaFaultedRuns != 12 {
		t.Fatalf("faulted ABA system runs = %d, want 12", abaFaultedRuns)
	}
}
