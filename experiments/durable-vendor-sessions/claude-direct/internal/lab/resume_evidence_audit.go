package lab

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const resumeEvidenceAuditVersion = "claude-direct-resume-disk-audit-v1"

type ResumeEvidenceAudit struct {
	Version                 string            `json:"version"`
	EvidenceRoot            string            `json:"evidence_root"`
	Runs                    int               `json:"runs"`
	UnfaultedRuns           int               `json:"unfaulted_runs"`
	UnsafeRuns              int               `json:"unsafe_runs"`
	RunsByBoundary          map[string]int    `json:"runs_by_boundary"`
	ValidPassVerdicts       int               `json:"valid_pass_verdicts"`
	ValidFailVerdicts       int               `json:"valid_fail_verdicts"`
	DuplicateEffectRuns     int               `json:"duplicate_effect_runs"`
	VerdictsRecomputed      int               `json:"verdicts_recomputed"`
	HistoriesReplayed       int               `json:"histories_replayed"`
	RawInventoriesVerified  int               `json:"raw_inventories_verified"`
	RawArtifactsVerified    int               `json:"raw_artifacts_verified"`
	ProcessesObserved       int               `json:"processes_observed"`
	PhysicalEffects         int               `json:"physical_effects"`
	WorkspaceEffects        int               `json:"workspace_effects"`
	AcceptedOutcomes        int               `json:"accepted_outcomes"`
	SelectedSessions        int               `json:"selected_sessions"`
	SourceSHA256            map[string]string `json:"source_sha256"`
	ClaudeVersion           string            `json:"claude_version"`
	AllRequirementsVerified bool              `json:"all_requirements_verified"`
}

type auditedResumeAttempt struct {
	number  int32
	request ControlledEffectInput
	process ProcessRecord
	stream  ClaudeStreamResult
}

func AuditResumeEvidence(ctx context.Context, root string) (ResumeEvidenceAudit, error) {
	population, err := inspectAuditedPopulation(ctx, root, 12)
	if err != nil {
		return ResumeEvidenceAudit{}, err
	}
	report := ResumeEvidenceAudit{
		Version: resumeEvidenceAuditVersion, EvidenceRoot: population.root,
		RunsByBoundary: make(map[string]int), SourceSHA256: make(map[string]string),
	}
	seenDirectories := make(map[string]bool, len(population.suite.RunDirectories))
	seenSessions := make(map[string]bool, len(population.suite.RunDirectories))
	var claudeVersion string
	var harnessBound bool
	for _, runDirectory := range population.suite.RunDirectories {
		run, err := readPopulationRun(ctx, population, seenDirectories, runDirectory)
		if err != nil {
			return ResumeEvidenceAudit{}, err
		}
		attempts, err := readResumeAttempts(run.rawRoot, run.summary.SelectedVendorSessionID)
		if err != nil {
			return ResumeEvidenceAudit{}, fmt.Errorf("audit attempts for %s: %w", run.manifest.RunID, err)
		}
		if err := validateResumeAuditTrial(run.manifest, run.input, run.verdict, run.summary, run.processes, attempts); err != nil {
			return ResumeEvidenceAudit{}, fmt.Errorf("audit control facts for %s: %w", run.manifest.RunID, err)
		}
		if seenSessions[run.summary.SelectedVendorSessionID] {
			return ResumeEvidenceAudit{}, errors.New("resume-only population reused a selected session across logical runs")
		}
		seenSessions[run.summary.SelectedVendorSessionID] = true
		if report.Runs == 0 {
			harnessBound = run.input.Settings["harness_binary_sha256"] != ""
		}
		if err := collectCompatibleSourceIdentity(report.SourceSHA256, run.input, harnessBound); err != nil {
			return ResumeEvidenceAudit{}, fmt.Errorf("source identity for %s: %w", run.manifest.RunID, err)
		}
		version := run.input.Settings["claude_version"]
		if version == "" {
			return ResumeEvidenceAudit{}, errors.New("claude version is absent")
		}
		if claudeVersion == "" {
			claudeVersion = version
		} else if claudeVersion != version {
			return ResumeEvidenceAudit{}, errors.New("claude version differs across admitted runs")
		}

		report.Runs++
		report.VerdictsRecomputed++
		report.HistoriesReplayed++
		report.RawInventoriesVerified++
		report.RawArtifactsVerified += len(run.inventory.Files)
		report.ProcessesObserved += len(attempts)
		report.PhysicalEffects += len(run.summary.Destination.Attempts)
		report.WorkspaceEffects += len(run.summary.WorkspaceEffects)
		report.AcceptedOutcomes++
		report.RunsByBoundary[string(run.summary.FaultBoundary)]++
		if run.summary.Probe == protocol.ProbeUnfaulted {
			report.UnfaultedRuns++
			report.ValidPassVerdicts++
		} else {
			report.UnsafeRuns++
			report.ValidFailVerdicts++
			report.DuplicateEffectRuns++
		}
	}
	report.SelectedSessions = len(seenSessions)
	if err := validateResumeAuditPopulation(report, seenDirectories, population.entries); err != nil {
		return ResumeEvidenceAudit{}, err
	}
	report.ClaudeVersion = claudeVersion
	report.AllRequirementsVerified = true
	return report, nil
}

func readResumeAttempts(rawRoot, selectedSession string) ([]auditedResumeAttempt, error) {
	attemptRoot := filepath.Join(rawRoot, "attempts")
	entries, err := os.ReadDir(attemptRoot)
	if err != nil {
		return nil, fmt.Errorf("read attempts: %w", err)
	}
	attempts := make([]auditedResumeAttempt, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil, errors.New("attempt root must contain only real attempt directories")
		}
		name := entry.Name()
		number, err := parseAttemptNumber(name)
		if err != nil {
			return nil, err
		}
		directory := filepath.Join(attemptRoot, name)
		request, err := readAuditedEffectRequest(filepath.Join(directory, effectRequestFile))
		if err != nil {
			return nil, err
		}
		process, err := readStrictJSON[ProcessRecord](filepath.Join(directory, name+".process-started.json"))
		if err != nil {
			return nil, err
		}
		stream, err := readAuditedClaudeStream(filepath.Join(directory, name+".stdout.jsonl"))
		if err != nil {
			return nil, err
		}
		if process.AttemptID != name || process.ActorID != request.ActorID || process.Identity == "" ||
			process.State != "running" || request.PhysicalAttemptID != name || stream.SessionID != selectedSession ||
			stream.Result != "EFFECT_COMPLETE" || stream.IsError {
			return nil, fmt.Errorf("attempt %s process, request, and stream identities differ", name)
		}
		if err := validateRecordedInvocation(process, RecoveryModeResumeOnly, selectedSession, number); err != nil {
			return nil, fmt.Errorf("validate attempt %s invocation: %w", name, err)
		}
		attempts = append(attempts, auditedResumeAttempt{number: number, request: request, process: process, stream: stream})
	}
	sort.Slice(attempts, func(left, right int) bool { return attempts[left].number < attempts[right].number })
	for index := range attempts {
		if attempts[index].number != int32(index+1) {
			return nil, errors.New("resume attempt ordinals are not contiguous from one")
		}
	}
	return attempts, nil
}

func readAuditedClaudeStream(path string) (ClaudeStreamResult, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return ClaudeStreamResult{}, fmt.Errorf("inspect Claude stream: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<20 {
		return ClaudeStreamResult{}, errors.New("claude stream is not a bounded regular artifact")
	}
	file, err := os.Open(path)
	if err != nil {
		return ClaudeStreamResult{}, fmt.Errorf("open Claude stream: %w", err)
	}
	result, parseErr := ParseClaudeStream(io.LimitReader(file, 16<<20))
	return result, errors.Join(parseErr, file.Close())
}

func validateResumeAuditTrial(manifest protocol.Manifest, input protocol.EffectiveInput, verdict protocol.Verdict,
	summary trialSummary, processes []protocol.ProcessObservation, attempts []auditedResumeAttempt,
) error {
	processIdentities, err := processIdentitiesByActor(processes)
	if err != nil {
		return fmt.Errorf("validate process observations: %w", err)
	}
	if manifest.Case != protocol.CaseAmbiguousEffect || manifest.SessionID == "" ||
		manifest.Probe != summary.Probe || manifest.Trial != summary.Trial ||
		verdict.RunID != manifest.RunID || verdict.Case != manifest.Case || verdict.Probe != manifest.Probe ||
		verdict.Trial != manifest.Trial {
		return errors.New("manifest, summary, and verdict identities differ")
	}
	if summary.RecoveryMode != RecoveryModeResumeOnly ||
		input.Settings["recovery_mode"] != string(RecoveryModeResumeOnly) || summary.Authority != nil ||
		!summary.ReplayVerified || input.Settings["workflow_history_replay_verified"] != "true" ||
		!validVendorSessionID(summary.SelectedVendorSessionID) ||
		input.Settings["selected_vendor_session_id"] != summary.SelectedVendorSessionID ||
		summary.WorkflowResult.VendorSessionID != summary.SelectedVendorSessionID ||
		summary.WorkflowResult.Result != "EFFECT_COMPLETE" ||
		input.Settings["session_identity"] != "caller-selected-before-workflow-start" ||
		input.Settings["resume_control"] != "first-delivery-session-id-later-deliveries-resume" {
		return errors.New("resume-only session, replay, or result evidence is incomplete")
	}
	wantEffects := 1
	wantAttempt := int32(1)
	if manifest.Probe == protocol.ProbeUnsafe {
		wantEffects = 2
		wantAttempt = 2
		if summary.FaultBoundary == FaultNone || input.Settings["fault_boundary"] != string(summary.FaultBoundary) {
			return errors.New("unsafe resume trial lacks its exact fault boundary")
		}
		if verdict.Class != protocol.VerdictValidFail ||
			!slices.Equal(verdict.ReasonCodes, []string{protocol.ReasonDuplicateEffect}) {
			return errors.New("faulted resume trial does not independently expose duplicate effects")
		}
	} else if manifest.Probe == protocol.ProbeUnfaulted && summary.FaultBoundary == FaultNone {
		if verdict.Class != protocol.VerdictValidPass || len(verdict.ReasonCodes) != 0 {
			return errors.New("unfaulted resume trial is not a clean valid pass")
		}
	} else {
		return errors.New("run is neither unsafe nor unfaulted")
	}
	metrics := verdict.Metrics
	if metrics.AcceptedOutcomeCount != 1 || metrics.PhysicalEffectCount != wantEffects ||
		metrics.PhysicalAttemptCount != wantEffects || metrics.ConcurrentOwnerCount < 1 ||
		metrics.ConcurrentOwnerCount > wantEffects || metrics.StaleActionAcceptCount != 0 ||
		metrics.PostCancelAcceptCount != 0 {
		return errors.New("recomputed verdict does not match the resume-only effect population")
	}
	if summary.WorkspaceBeforeHash == "" || summary.WorkspaceAfterHash == "" ||
		summary.WorkspaceBeforeHash == summary.WorkspaceAfterHash ||
		input.Settings["workspace_before_sha256"] != summary.WorkspaceBeforeHash ||
		input.Settings["workspace_after_sha256"] != summary.WorkspaceAfterHash ||
		input.Settings["workspace_effect_count"] != strconv.Itoa(wantEffects) ||
		len(summary.WorkspaceEffects) != wantEffects || len(summary.Destination.Attempts) != wantEffects ||
		len(attempts) != wantEffects {
		return errors.New("independent workspace, destination, or process evidence is incomplete")
	}
	destinationByID := make(map[string]EffectAttempt, wantEffects)
	for _, effect := range summary.Destination.Attempts {
		if _, exists := destinationByID[effect.PhysicalAttemptID]; exists {
			return errors.New("destination contains a duplicate physical attempt identity")
		}
		destinationByID[effect.PhysicalAttemptID] = effect
	}
	workspaceByID := make(map[string]WorkspaceEffect, wantEffects)
	for _, effect := range summary.WorkspaceEffects {
		if _, exists := workspaceByID[effect.PhysicalAttemptID]; exists {
			return errors.New("workspace contains a duplicate physical attempt identity")
		}
		workspaceByID[effect.PhysicalAttemptID] = effect
	}
	for index, attempt := range attempts {
		request := attempt.request
		if attempt.number != int32(index+1) || request.LogicalSessionID != manifest.SessionID ||
			request.LogicalTurnID != "turn-1" || request.LogicalEffectID != "effect-1" ||
			request.Payload != "controlled-edit" || request.BarrierPoint != committedEffectBarrier ||
			request.SupervisorURL != "" || request.OwnershipGeneration != 0 || request.OwnerCapability != "" ||
			processIdentities[request.ActorID] != attempt.process.Identity {
			return errors.New("resume-only request identity or absence of fencing differs")
		}
		destination, destinationFound := destinationByID[request.PhysicalAttemptID]
		workspace, workspaceFound := workspaceByID[request.PhysicalAttemptID]
		if !destinationFound || !workspaceFound || !destination.Applied ||
			destination.LogicalSessionID != request.LogicalSessionID ||
			destination.LogicalTurnID != request.LogicalTurnID ||
			destination.LogicalEffectID != request.LogicalEffectID || destination.ActorID != request.ActorID ||
			destination.ProcessIdentity == "" || workspace.LogicalEffectID != request.LogicalEffectID ||
			workspace.Payload != request.Payload || workspace.ActorID != request.ActorID ||
			workspace.ProcessIdentity != destination.ProcessIdentity || !workspace.AppliedAt.Equal(destination.AppliedAt) {
			return errors.New("request, destination, and workspace effect identities differ")
		}
	}
	accepted := attempts[len(attempts)-1]
	if summary.WorkflowResult.TemporalAttempt != wantAttempt ||
		summary.WorkflowResult.PhysicalAttemptID != accepted.request.PhysicalAttemptID ||
		summary.WorkflowResult.ProcessIdentity != accepted.process.Identity {
		return errors.New("accepted Workflow result does not come from the final delivery attempt")
	}
	return nil
}

func validateResumeAuditPopulation(report ResumeEvidenceAudit, seen map[string]bool, entries []os.DirEntry) error {
	want := map[string]int{
		string(FaultNone): 3, string(FaultBeforeVendorRegistration): 3,
		string(FaultAfterToolEffect): 3, string(FaultAfterFinalOutput): 3,
	}
	if !mapsEqual(report.RunsByBoundary, want) || report.Runs != 12 || report.UnfaultedRuns != 3 ||
		report.UnsafeRuns != 9 || report.ValidPassVerdicts != 3 || report.ValidFailVerdicts != 9 ||
		report.DuplicateEffectRuns != 9 || report.VerdictsRecomputed != 12 || report.HistoriesReplayed != 12 ||
		report.RawInventoriesVerified != 12 || report.ProcessesObserved != 21 || report.PhysicalEffects != 21 ||
		report.WorkspaceEffects != 21 || report.AcceptedOutcomes != 12 || report.SelectedSessions != 12 {
		return errors.New("disk reconstruction does not match the exact resume-only control population")
	}
	return validateListedRunDirectories(report.EvidenceRoot, seen, entries)
}

func mapsEqual(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
