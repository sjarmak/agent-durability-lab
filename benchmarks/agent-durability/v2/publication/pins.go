package publication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func VerifyHashPins(registration Preregistration, benchmarkRoot string) error {
	contractHash, err := fileHash(filepath.Join(benchmarkRoot, "contract-v2.json"))
	if err != nil {
		return err
	}
	if contractHash != registration.Hashes.ContractV2SHA256 {
		return fmt.Errorf("%w: contract hash = %s, want %s", protocol.ErrInvalidEvidence, contractHash, registration.Hashes.ContractV2SHA256)
	}
	sourceHash, err := AdapterBaselineSHA256(filepath.Join(benchmarkRoot, "v2"))
	if err != nil {
		return err
	}
	if sourceHash != registration.Hashes.AdapterBaselineSHA256 {
		return fmt.Errorf("%w: adapter baseline hash = %s, want %s", protocol.ErrInvalidEvidence, sourceHash, registration.Hashes.AdapterBaselineSHA256)
	}
	populationHash, err := PopulationConfigSHA256(registration)
	if err != nil {
		return err
	}
	if populationHash != registration.Hashes.PopulationConfigSHA256 {
		return fmt.Errorf("%w: population config hash = %s, want %s", protocol.ErrInvalidEvidence, populationHash, registration.Hashes.PopulationConfigSHA256)
	}
	return nil
}

// AdapterBaselineSHA256 hashes the path and contents of every Go source file
// in v2 except the publication package and its CLI entry point. Publication
// runner code is frozen separately after the pilot; this pin proves which
// already-conformant adapters it wraps.
func AdapterBaselineSHA256(v2Root string) (string, error) {
	var paths []string
	err := filepath.WalkDir(v2Root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(v2Root, path)
		if err != nil {
			return err
		}
		publicationCLI := filepath.Join("cmd", "publication")
		if entry.IsDir() && (relative == "publication" || strings.HasPrefix(relative, "publication"+string(filepath.Separator)) ||
			relative == publicationCLI || strings.HasPrefix(relative, publicationCLI+string(filepath.Separator))) {
			return filepath.SkipDir
		}
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" {
			paths = append(paths, relative)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	slices.Sort(paths)
	if len(paths) == 0 {
		return "", fmt.Errorf("%w: adapter baseline has no Go source", protocol.ErrInvalidEvidence)
	}
	hash := sha256.New()
	for _, relative := range paths {
		data, err := os.ReadFile(filepath.Join(v2Root, relative))
		if err != nil {
			return "", err
		}
		_, _ = hash.Write([]byte(filepath.ToSlash(relative)))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func PopulationConfigSHA256(registration Preregistration) (string, error) {
	populationConfig := struct {
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
	}{
		RequiredSystems: registration.RequiredSystems, Cases: registration.Cases, Probes: registration.Probes,
		TrackByProbe: registration.TrackByProbe, Population: registration.Population, Timing: registration.Timing,
		Admission: registration.Admission, Analysis: registration.Analysis, Liveness: registration.Liveness, PrimaryEstimands: registration.PrimaryEstimands,
		RequiredEvidence: registration.RequiredEvidence, HostControls: registration.HostControls,
	}
	data, err := json.Marshal(populationConfig)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func fileHash(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}
