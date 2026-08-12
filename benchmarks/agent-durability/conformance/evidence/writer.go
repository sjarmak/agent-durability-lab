package evidence

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func WriteReport(ctx context.Context, root string, report Report) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if root == "" || report.ContractVersion != ContractVersion || report.ProfileKind != CalibrationProfileKind ||
		(report.Status != StatusConformant && report.Status != StatusNonconformant) || report.ClaimBoundary != CalibrationClaimBoundary {
		return "", fmt.Errorf("%w: incomplete conformance report", legacyprotocol.ErrInvalidEvidence)
	}
	if err := ValidatePins(report.Pins); err != nil {
		return "", fmt.Errorf("%w: %v", legacyprotocol.ErrInvalidEvidence, err)
	}
	if report.Status == StatusConformant {
		if err := validateConformantReport(report); err != nil {
			return "", fmt.Errorf("%w: %v", legacyprotocol.ErrInvalidEvidence, err)
		}
		if err := VerifyPreservedExecutable(root, report.Pins.Executable); err != nil {
			return "", err
		}
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode conformance report: %w", err)
	}
	path := filepath.Join(root, ReportFile)
	if err := writeExclusiveFile(ctx, path, append(data, '\n')); err != nil {
		return path, err
	}
	if err := syncDirectory(root); err != nil {
		return path, err
	}
	return path, nil
}

func validateConformantReport(report Report) error {
	type identity struct {
		benchmarkCase legacyprotocol.CaseID
		probe         legacyprotocol.Probe
		trial         int
		verdict       legacyprotocol.VerdictClass
	}
	wantEpisodes := make(map[string]identity, len(legacyprotocol.Cases())*(1+2*DevelopmentTrials))
	for _, benchmarkCase := range legacyprotocol.Cases() {
		unfaultedID := fmt.Sprintf("%s-%s-trial-1", benchmarkCase, legacyprotocol.ProbeUnfaulted)
		wantEpisodes[unfaultedID] = identity{benchmarkCase: benchmarkCase, probe: legacyprotocol.ProbeUnfaulted, trial: 1, verdict: legacyprotocol.VerdictValidPass}
		for _, probe := range []legacyprotocol.Probe{legacyprotocol.ProbeUnsafe, legacyprotocol.ProbeProtected} {
			verdict := legacyprotocol.VerdictValidPass
			if probe == legacyprotocol.ProbeUnsafe {
				verdict = legacyprotocol.VerdictValidFail
			}
			for trial := 1; trial <= DevelopmentTrials; trial++ {
				runID := fmt.Sprintf("%s-%s-trial-%d", benchmarkCase, probe, trial)
				wantEpisodes[runID] = identity{benchmarkCase: benchmarkCase, probe: probe, trial: trial, verdict: verdict}
			}
		}
	}
	if len(report.Episodes) != len(wantEpisodes) {
		return fmt.Errorf("episode inventory has %d entries, want %d", len(report.Episodes), len(wantEpisodes))
	}
	for _, episode := range report.Episodes {
		want, found := wantEpisodes[episode.RunID]
		if !found || episode.Path != filepath.ToSlash(filepath.Join("runs", episode.RunID)) || episode.Case != want.benchmarkCase ||
			episode.Probe != want.probe || episode.Trial != want.trial || episode.Verdict != want.verdict || episode.Replay.Captured ||
			episode.Replay.Status != ReplayNotApplicable || episode.Replay.Explanation != CalibrationReplayExplanation {
			return fmt.Errorf("episode %q has an unexpected identity, verdict, path, or replay disposition", episode.RunID)
		}
		delete(wantEpisodes, episode.RunID)
	}
	if len(wantEpisodes) != 0 {
		return fmt.Errorf("episode inventory is incomplete")
	}
	wantControls := make(map[string]InvalidControlSpec, len(invalidControlSpecs))
	for _, spec := range invalidControlSpecs {
		wantControls[spec.ID] = spec
	}
	if len(report.InvalidControls) != len(wantControls) {
		return fmt.Errorf("invalid-control inventory has %d entries, want %d", len(report.InvalidControls), len(wantControls))
	}
	for _, control := range report.InvalidControls {
		want, found := wantControls[control.ID]
		if !found || control.Path != filepath.ToSlash(filepath.Join("invalid-controls", control.ID)) ||
			control.ExpectedReason != want.ExpectedReason || control.Verdict != legacyprotocol.VerdictInvalid || !reasonPresent(control.ReasonCodes, want.ExpectedReason) {
			return fmt.Errorf("invalid control %q has an unexpected verdict, reason, or path", control.ID)
		}
		delete(wantControls, control.ID)
	}
	if len(wantControls) != 0 {
		return fmt.Errorf("invalid-control inventory is incomplete")
	}
	return nil
}

func reasonPresent(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
