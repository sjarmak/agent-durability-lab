package oracle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
	legacyoracle "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func Evaluate(ctx context.Context, root string, pins evidence.Pins) (evidence.Report, error) {
	report := evidence.Report{
		ContractVersion: evidence.ContractVersion,
		ProfileKind:     evidence.CalibrationProfileKind,
		Status:          evidence.StatusConformant,
		ClaimBoundary:   evidence.CalibrationClaimBoundary,
		Pins:            pins,
	}
	var problems []error
	if err := ctx.Err(); err != nil {
		problems = append(problems, err)
	}
	if root == "" {
		problems = append(problems, errors.New("conformance root is required"))
	}
	if err := evidence.ValidatePins(pins); err != nil {
		problems = append(problems, err)
	}
	if len(problems) != 0 {
		report.Status = evidence.StatusNonconformant
		return report, errors.Join(problems...)
	}
	problems = append(problems, inventoryProblems(root)...)
	if len(problems) != 0 {
		report.Status = evidence.StatusNonconformant
		return report, errors.Join(problems...)
	}
	if root != "" {
		if err := evidence.VerifyPreservedExecutable(root, pins.Executable); err != nil {
			problems = append(problems, err)
		}
	}
	if len(problems) != 0 {
		report.Status = evidence.StatusNonconformant
		return report, errors.Join(problems...)
	}
	for _, benchmarkCase := range legacyprotocol.Cases() {
		runSpecs := []struct {
			probe  legacyprotocol.Probe
			trials int
		}{
			{probe: legacyprotocol.ProbeUnfaulted, trials: 1},
			{probe: legacyprotocol.ProbeUnsafe, trials: evidence.DevelopmentTrials},
			{probe: legacyprotocol.ProbeProtected, trials: evidence.DevelopmentTrials},
		}
		for _, spec := range runSpecs {
			for trial := 1; trial <= spec.trials; trial++ {
				runID := fmt.Sprintf("%s-%s-trial-%d", benchmarkCase, spec.probe, trial)
				relativePath := filepath.ToSlash(filepath.Join("runs", runID))
				runDir := filepath.Join(root, filepath.FromSlash(relativePath))
				recomputed := legacyoracle.Evaluate(ctx, runDir)
				stored, err := readStoredVerdict(filepath.Join(runDir, legacyprotocol.VerdictFile))
				if err != nil {
					problems = append(problems, fmt.Errorf("read stored verdict for %s: %w", runID, err))
				} else if !reflect.DeepEqual(stored, recomputed) {
					problems = append(problems, fmt.Errorf("stored verdict for %s differs from independent recomputation", runID))
				}
				want := legacyprotocol.VerdictValidPass
				if spec.probe == legacyprotocol.ProbeUnsafe {
					want = legacyprotocol.VerdictValidFail
				}
				if recomputed.RunID != runID || recomputed.Case != benchmarkCase || recomputed.Probe != spec.probe || recomputed.Trial != trial || recomputed.Class != want {
					problems = append(problems, fmt.Errorf("%s recomputed as %s, want %s with exact identity", runID, recomputed.Class, want))
				}
				replay := evidence.ReplayDisposition{Status: evidence.ReplayNotApplicable, Explanation: evidence.CalibrationReplayExplanation}
				if err := ValidateReplay(replay); err != nil {
					problems = append(problems, fmt.Errorf("%s replay disposition: %w", runID, err))
				}
				report.Episodes = append(report.Episodes, evidence.EpisodeReference{
					RunID: runID, Path: relativePath, Case: benchmarkCase, Probe: spec.probe, Trial: trial,
					Verdict: recomputed.Class, ReasonCodes: recomputed.ReasonCodes, Replay: replay,
				})
			}
		}
	}
	for _, spec := range evidence.InvalidControlSpecs() {
		relativePath := filepath.ToSlash(filepath.Join("invalid-controls", spec.ID))
		controlDir := filepath.Join(root, filepath.FromSlash(relativePath))
		recomputed := legacyoracle.Evaluate(ctx, controlDir)
		stored, err := readStoredVerdict(filepath.Join(controlDir, legacyprotocol.VerdictFile))
		if err != nil {
			problems = append(problems, fmt.Errorf("read stored verdict for control %s: %w", spec.ID, err))
		} else if !reflect.DeepEqual(stored, recomputed) {
			problems = append(problems, fmt.Errorf("stored verdict for control %s differs from independent recomputation", spec.ID))
		}
		if recomputed.RunID != "invalid-control-"+spec.ID || recomputed.Class != legacyprotocol.VerdictInvalid || !contains(recomputed.ReasonCodes, spec.ExpectedReason) {
			problems = append(problems, fmt.Errorf("control %s was not rejected for %s", spec.ID, spec.ExpectedReason))
		}
		report.InvalidControls = append(report.InvalidControls, evidence.InvalidControlReference{
			ID: spec.ID, Path: relativePath, ExpectedReason: spec.ExpectedReason,
			Verdict: recomputed.Class, ReasonCodes: recomputed.ReasonCodes,
		})
	}
	if len(problems) != 0 {
		report.Status = evidence.StatusNonconformant
	}
	return report, errors.Join(problems...)
}

func inventoryProblems(root string) []error {
	runIDs := make([]string, 0, len(legacyprotocol.Cases())*(1+2*evidence.DevelopmentTrials))
	for _, benchmarkCase := range legacyprotocol.Cases() {
		runIDs = append(runIDs, fmt.Sprintf("%s-%s-trial-1", benchmarkCase, legacyprotocol.ProbeUnfaulted))
		for _, probe := range []legacyprotocol.Probe{legacyprotocol.ProbeUnsafe, legacyprotocol.ProbeProtected} {
			for trial := 1; trial <= evidence.DevelopmentTrials; trial++ {
				runIDs = append(runIDs, fmt.Sprintf("%s-%s-trial-%d", benchmarkCase, probe, trial))
			}
		}
	}
	controlIDs := make([]string, 0, len(evidence.InvalidControlSpecs()))
	for _, spec := range evidence.InvalidControlSpecs() {
		controlIDs = append(controlIDs, spec.ID)
	}
	var problems []error
	problems = append(problems, exactEntries(root, []string{"runs", "invalid-controls", "inputs"}, []string{evidence.ReportFile}, true)...)
	problems = append(problems, exactEntries(filepath.Join(root, "inputs"), []string{"executable"}, nil, true)...)
	problems = append(problems, exactEntries(filepath.Join(root, "inputs", "executable"), []string{filepath.Base(evidence.ExecutableArtifactPath)}, nil, false)...)
	problems = append(problems, exactEntries(filepath.Join(root, "runs"), runIDs, nil, true)...)
	problems = append(problems, exactEntries(filepath.Join(root, "invalid-controls"), controlIDs, nil, true)...)
	runFiles := append(legacyprotocol.RawEvidenceFiles(), legacyprotocol.VerdictFile)
	for _, runID := range runIDs {
		problems = append(problems, exactEntries(filepath.Join(root, "runs", runID), runFiles, nil, false)...)
	}
	for _, controlID := range controlIDs {
		problems = append(problems, exactEntries(filepath.Join(root, "invalid-controls", controlID), runFiles, nil, false)...)
	}
	return problems
}

func exactEntries(path string, required, optional []string, wantDirectories bool) []error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return []error{fmt.Errorf("read inventory %s: %w", path, err)}
	}
	want := make(map[string]bool, len(required)+len(optional))
	for _, name := range required {
		want[name] = true
	}
	for _, name := range optional {
		want[name] = false
	}
	seen := make(map[string]bool, len(entries))
	var problems []error
	for _, entry := range entries {
		requiredEntry, allowed := want[entry.Name()]
		if !allowed {
			problems = append(problems, fmt.Errorf("unexpected inventory entry %s", filepath.Join(path, entry.Name())))
			continue
		}
		seen[entry.Name()] = true
		entryShouldBeDirectory := wantDirectories
		if !requiredEntry && entry.Name() == evidence.ReportFile {
			entryShouldBeDirectory = false
		}
		info, err := entry.Info()
		if err != nil {
			problems = append(problems, fmt.Errorf("inspect inventory entry %s: %w", filepath.Join(path, entry.Name()), err))
			continue
		}
		validType := info.IsDir()
		if !entryShouldBeDirectory {
			validType = info.Mode().IsRegular()
		}
		if entry.Type()&os.ModeSymlink != 0 || !validType {
			problems = append(problems, fmt.Errorf("inventory entry has wrong type %s", filepath.Join(path, entry.Name())))
		}
	}
	for _, name := range required {
		if !seen[name] {
			problems = append(problems, fmt.Errorf("required inventory entry is missing %s", filepath.Join(path, name)))
		}
	}
	return problems
}

func readStoredVerdict(path string) (legacyprotocol.Verdict, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return legacyprotocol.Verdict{}, err
	}
	var verdict legacyprotocol.Verdict
	if err := evidence.DecodeJSONStrict(data, &verdict); err != nil {
		return legacyprotocol.Verdict{}, err
	}
	return verdict, nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
