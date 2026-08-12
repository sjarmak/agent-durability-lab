package publication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

var frozenHarnessSources = []string{
	"benchmarks/agent-durability/v2/cmd/publication/main.go",
	"benchmarks/agent-durability/v2/publication/episode_plan.go",
	"benchmarks/agent-durability/v2/publication/episode_runtime.go",
	"benchmarks/agent-durability/v2/publication/pins.go",
	"benchmarks/agent-durability/v2/publication/population.go",
	"benchmarks/agent-durability/v2/publication/postgres_executor.go",
	"benchmarks/agent-durability/v2/publication/preregistration.go",
	"benchmarks/agent-durability/v2/publication/runner.go",
	"benchmarks/agent-durability/v2/publication/temporal_executor.go",
	"benchmarks/agent-durability/v2/publication/temporal_workflow.go",
	"benchmarks/agent-durability/v2/publication/writer.go",
}

type HarnessFreeze struct {
	ProtocolVersion       string   `json:"protocol_version"`
	FrozenAtUTC           string   `json:"frozen_at_utc"`
	SourceFiles           []string `json:"source_files"`
	HarnessSourceSHA256   string   `json:"harness_source_sha256"`
	RunnerBinarySHA256    string   `json:"runner_binary_sha256"`
	PreregistrationSHA256 string   `json:"preregistration_sha256"`
	PilotInventorySHA256  string   `json:"pilot_inventory_sha256"`
	PilotEvidenceRoot     string   `json:"pilot_evidence_root"`
}

func LoadHarnessFreeze(path string) (HarnessFreeze, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return HarnessFreeze{}, err
	}
	var freeze HarnessFreeze
	if err := json.Unmarshal(data, &freeze); err != nil {
		return HarnessFreeze{}, err
	}
	if freeze.ProtocolVersion != ProtocolVersion || freeze.FrozenAtUTC == "" ||
		!slices.Equal(freeze.SourceFiles, frozenHarnessSources) || !validDigest(freeze.HarnessSourceSHA256) ||
		!validDigest(freeze.RunnerBinarySHA256) || !validDigest(freeze.PreregistrationSHA256) ||
		!validDigest(freeze.PilotInventorySHA256) || freeze.PilotEvidenceRoot == "" {
		return HarnessFreeze{}, invalid("harness freeze")
	}
	return freeze, nil
}

func VerifyHarnessFreeze(freeze HarnessFreeze, repositoryRoot, runnerBinary, preregistrationPath string) error {
	if err := VerifyHarnessFreezeTrackedInputs(freeze, repositoryRoot, preregistrationPath); err != nil {
		return err
	}
	binaryHash, err := protocol.FileSHA256(runnerBinary)
	if err != nil {
		return err
	}
	return verifyFrozenHash("runner binary", binaryHash, freeze.RunnerBinarySHA256)
}

// VerifyHarnessFreezeTrackedInputs checks the publication inputs retained in
// the repository. The historical runner binary is checked separately by
// VerifyHarnessFreeze when a publication run is launched.
func VerifyHarnessFreezeTrackedInputs(freeze HarnessFreeze, repositoryRoot, preregistrationPath string) error {
	sourceHash, err := HarnessSourceSHA256(repositoryRoot)
	if err != nil {
		return err
	}
	preregistrationHash, err := protocol.FileSHA256(preregistrationPath)
	if err != nil {
		return err
	}
	pilotInventoryHash, err := protocol.FileSHA256(filepath.Join(repositoryRoot, freeze.PilotEvidenceRoot, PublicationPopulationInventoryFile))
	if err != nil {
		return err
	}
	checks := []struct{ name, got, want string }{
		{"harness source", sourceHash, freeze.HarnessSourceSHA256},
		{"preregistration", preregistrationHash, freeze.PreregistrationSHA256},
		{"pilot inventory", pilotInventoryHash, freeze.PilotInventorySHA256},
	}
	for _, check := range checks {
		if err := verifyFrozenHash(check.name, check.got, check.want); err != nil {
			return err
		}
	}
	return nil
}

func verifyFrozenHash(name, got, want string) error {
	if got != want {
		return fmt.Errorf("%w: %s hash = %s, want %s", protocol.ErrInvalidEvidence, name, got, want)
	}
	return nil
}

func HarnessSourceSHA256(repositoryRoot string) (string, error) {
	hash := sha256.New()
	for _, relative := range frozenHarnessSources {
		data, err := os.ReadFile(filepath.Join(repositoryRoot, filepath.FromSlash(relative)))
		if err != nil {
			return "", err
		}
		_, _ = hash.Write([]byte(relative))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
