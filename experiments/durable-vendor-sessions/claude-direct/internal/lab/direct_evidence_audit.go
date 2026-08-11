package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const directEvidenceAuditVersion = "claude-direct-disk-audit-v1"

type DirectEvidenceAudit struct {
	Version                 string            `json:"version"`
	EvidenceRoot            string            `json:"evidence_root"`
	Runs                    int               `json:"runs"`
	UnfaultedRuns           int               `json:"unfaulted_runs"`
	UnsafeRuns              int               `json:"unsafe_runs"`
	RunsByBoundary          map[string]int    `json:"runs_by_boundary"`
	ValidPassVerdicts       int               `json:"valid_pass_verdicts"`
	ValidFailVerdicts       int               `json:"valid_fail_verdicts"`
	VerdictsRecomputed      int               `json:"verdicts_recomputed"`
	HistoriesReplayed       int               `json:"histories_replayed"`
	RawInventoriesVerified  int               `json:"raw_inventories_verified"`
	RawArtifactsVerified    int               `json:"raw_artifacts_verified"`
	ProcessesObserved       int               `json:"processes_observed"`
	PhysicalEffects         int               `json:"physical_effects"`
	WorkspaceEffects        int               `json:"workspace_effects"`
	AcceptedOutcomes        int               `json:"accepted_outcomes"`
	ProviderSessions        int               `json:"provider_sessions"`
	SourceSHA256            map[string]string `json:"source_sha256"`
	ClaudeVersion           string            `json:"claude_version"`
	AllRequirementsVerified bool              `json:"all_requirements_verified"`
}

type auditedDirectAttempt struct {
	number  int32
	request ControlledEffectInput
	process ProcessRecord
	stream  ClaudeStreamResult
}

func AuditDirectEvidence(ctx context.Context, root string) (DirectEvidenceAudit, error) {
	population, err := inspectAuditedPopulation(ctx, root, 12)
	if err != nil {
		return DirectEvidenceAudit{}, err
	}
	report := DirectEvidenceAudit{
		Version: directEvidenceAuditVersion, EvidenceRoot: population.root,
		RunsByBoundary: make(map[string]int), SourceSHA256: make(map[string]string),
	}
	seen := make(map[string]bool, len(population.suite.RunDirectories))
	providerSessions := make(map[string]bool, 21)
	var claudeVersion string
	var harnessBound bool
	for _, runDirectory := range population.suite.RunDirectories {
		run, err := readPopulationRun(ctx, population, seen, runDirectory)
		if err != nil {
			return DirectEvidenceAudit{}, err
		}
		attempts, err := readDirectAttempts(run.rawRoot)
		if err != nil {
			return DirectEvidenceAudit{}, fmt.Errorf("audit direct attempts for %s: %w", run.manifest.RunID, err)
		}
		if err := validateDirectAuditTrial(run, attempts); err != nil {
			return DirectEvidenceAudit{}, fmt.Errorf("audit direct facts for %s: %w", run.manifest.RunID, err)
		}
		for _, attempt := range attempts {
			if providerSessions[attempt.stream.SessionID] {
				return DirectEvidenceAudit{}, errors.New("direct population reused a provider session across logical runs")
			}
			providerSessions[attempt.stream.SessionID] = true
		}
		if report.Runs == 0 {
			harnessBound = run.input.Settings["harness_binary_sha256"] != ""
		}
		if err := collectCompatibleSourceIdentity(report.SourceSHA256, run.input, harnessBound); err != nil {
			return DirectEvidenceAudit{}, fmt.Errorf("source identity for %s: %w", run.manifest.RunID, err)
		}
		version := run.input.Settings["claude_version"]
		if version == "" {
			return DirectEvidenceAudit{}, errors.New("claude version is absent")
		}
		if claudeVersion == "" {
			claudeVersion = version
		} else if claudeVersion != version {
			return DirectEvidenceAudit{}, errors.New("claude version differs across direct runs")
		}
		report.Runs++
		report.VerdictsRecomputed++
		report.HistoriesReplayed++
		report.RawInventoriesVerified++
		report.RawArtifactsVerified += len(run.inventory.Files)
		report.ProcessesObserved += len(attempts)
		report.PhysicalEffects += len(run.summary.Destination.Attempts)
		report.WorkspaceEffects += len(run.summary.WorkspaceEffects)
		report.AcceptedOutcomes += run.verdict.Metrics.AcceptedOutcomeCount
		report.RunsByBoundary[string(run.summary.FaultBoundary)]++
		if run.summary.Probe == protocol.ProbeUnfaulted {
			report.UnfaultedRuns++
			report.ValidPassVerdicts++
		} else {
			report.UnsafeRuns++
			report.ValidFailVerdicts++
		}
	}
	report.ProviderSessions = len(providerSessions)
	report.ClaudeVersion = claudeVersion
	if err := validateDirectAuditPopulation(report, seen, population.entries); err != nil {
		return DirectEvidenceAudit{}, err
	}
	report.AllRequirementsVerified = true
	return report, nil
}

func readDirectAttempts(rawRoot string) ([]auditedDirectAttempt, error) {
	root := filepath.Join(rawRoot, "attempts")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read attempt root: %w", err)
	}
	attempts := make([]auditedDirectAttempt, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("attempt entry %q is not a real directory", entry.Name())
		}
		number, err := parseAttemptNumber(entry.Name())
		if err != nil {
			return nil, err
		}
		directory := filepath.Join(root, entry.Name())
		request, err := readAuditedEffectRequest(filepath.Join(directory, effectRequestFile))
		if err != nil {
			return nil, err
		}
		process, err := readStrictJSON[ProcessRecord](filepath.Join(directory, entry.Name()+".process-started.json"))
		if err != nil {
			return nil, err
		}
		stream, err := readAuditedClaudeStream(filepath.Join(directory, entry.Name()+".stdout.jsonl"))
		if err != nil {
			return nil, err
		}
		if process.AttemptID != entry.Name() || process.ActorID != request.ActorID || process.Identity == "" ||
			process.PID < 1 || process.State != "running" || request.PhysicalAttemptID != entry.Name() ||
			!validVendorSessionID(stream.SessionID) || stream.Result != "EFFECT_COMPLETE" || stream.IsError {
			return nil, fmt.Errorf("attempt %q request, process, or Claude stream identity is incomplete", entry.Name())
		}
		attempts = append(attempts, auditedDirectAttempt{number: number, request: request, process: process, stream: stream})
	}
	sort.Slice(attempts, func(left, right int) bool { return attempts[left].number < attempts[right].number })
	for index := range attempts {
		if attempts[index].number != int32(index+1) {
			return nil, errors.New("direct attempt ordinals are not contiguous from one")
		}
	}
	return attempts, nil
}

func validateDirectAuditTrial(run auditedRun, attempts []auditedDirectAttempt) error {
	manifest, verdict, input, summary := run.manifest, run.verdict, run.input, run.summary
	processIdentities, err := processIdentitiesByActor(run.processes)
	if err != nil {
		return fmt.Errorf("validate process observations: %w", err)
	}
	if manifest.Case != protocol.CaseAmbiguousEffect || manifest.Probe != summary.Probe || manifest.Trial != summary.Trial ||
		manifest.SessionID == "" || verdict.RunID != manifest.RunID || verdict.Case != manifest.Case ||
		verdict.Probe != manifest.Probe || verdict.Trial != manifest.Trial ||
		summary.WorkflowID == "" || summary.WorkflowRunID == "" || summary.WorkflowResult.ProcessIdentity == "" ||
		input.Settings["fault_boundary"] != string(summary.FaultBoundary) {
		return errors.New("manifest, input, and raw summary identities differ")
	}
	if summary.RecoveryMode.normalized() != RecoveryModeUnsafeFresh || summary.SelectedVendorSessionID != "" ||
		input.Settings["session_identity"] != "vendor-assigned-after-start" || input.Settings["resume_control"] != "none" ||
		!validVendorSessionID(summary.WorkflowResult.VendorSessionID) || summary.WorkflowResult.Result != "EFFECT_COMPLETE" {
		return errors.New("direct session strategy or accepted result is incomplete")
	}
	wantEffects := 2
	if summary.Probe == protocol.ProbeUnfaulted {
		wantEffects = 1
		if summary.FaultBoundary != FaultNone || verdict.Class != protocol.VerdictValidPass || len(verdict.ReasonCodes) != 0 {
			return errors.New("unfaulted trial names a fault boundary")
		}
	} else if summary.Probe != protocol.ProbeUnsafe || summary.FaultBoundary == FaultNone || summary.FaultAt.IsZero() {
		return errors.New("unsafe trial lacks an observed fault boundary")
	} else if verdict.Class != protocol.VerdictValidFail ||
		!slices.Equal(verdict.ReasonCodes, []string{protocol.ReasonDuplicateEffect}) {
		return errors.New("faulted direct trial does not independently expose duplicate effects")
	}
	metrics := verdict.Metrics
	if metrics.AcceptedOutcomeCount != 1 || metrics.PhysicalEffectCount != wantEffects ||
		metrics.PhysicalAttemptCount != wantEffects || metrics.StaleActionAcceptCount != 0 ||
		metrics.PostCancelAcceptCount != 0 || metrics.ConcurrentOwnerCount < 1 || metrics.ConcurrentOwnerCount > wantEffects {
		return errors.New("recomputed verdict does not match the direct effect population")
	}
	if summary.WorkspaceBeforeHash == "" || summary.WorkspaceAfterHash == "" ||
		summary.WorkspaceBeforeHash == summary.WorkspaceAfterHash || input.Settings["workspace_before_sha256"] != summary.WorkspaceBeforeHash ||
		input.Settings["workspace_after_sha256"] != summary.WorkspaceAfterHash ||
		input.Settings["workspace_effect_count"] != strconv.Itoa(wantEffects) || len(summary.Destination.Attempts) != wantEffects ||
		len(summary.WorkspaceEffects) != wantEffects || len(attempts) != wantEffects {
		return errors.New("raw destination/workspace facts do not distinguish the stored verdict")
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
	sessions := make(map[string]bool, wantEffects)
	for _, attempt := range attempts {
		request := attempt.request
		if request.LogicalSessionID != manifest.SessionID || request.LogicalTurnID != "turn-1" ||
			request.LogicalEffectID != "effect-1" || request.Payload != "controlled-edit" ||
			request.BarrierPoint != committedEffectBarrier || request.SupervisorURL != "" ||
			request.OwnershipGeneration != 0 || request.OwnerCapability != "" || sessions[attempt.stream.SessionID] ||
			processIdentities[request.ActorID] != attempt.process.Identity {
			return errors.New("direct request, fence absence, or provider session identity differs")
		}
		sessions[attempt.stream.SessionID] = true
		destination, destinationFound := destinationByID[request.PhysicalAttemptID]
		workspace, workspaceFound := workspaceByID[request.PhysicalAttemptID]
		if !destinationFound || !workspaceFound || !destination.Applied ||
			destination.LogicalSessionID != request.LogicalSessionID || destination.LogicalTurnID != request.LogicalTurnID ||
			destination.LogicalEffectID != request.LogicalEffectID || destination.ActorID != request.ActorID ||
			destination.ProcessIdentity == "" || workspace.LogicalEffectID != request.LogicalEffectID ||
			workspace.Payload != request.Payload || workspace.ActorID != request.ActorID ||
			workspace.ProcessIdentity != destination.ProcessIdentity || !workspace.AppliedAt.Equal(destination.AppliedAt) {
			return errors.New("request, destination, and workspace effect identities differ")
		}
	}
	accepted := attempts[len(attempts)-1]
	if summary.WorkflowResult.TemporalAttempt != int32(wantEffects) ||
		summary.WorkflowResult.PhysicalAttemptID != accepted.request.PhysicalAttemptID ||
		summary.WorkflowResult.ProcessIdentity != accepted.process.Identity ||
		summary.WorkflowResult.VendorSessionID != accepted.stream.SessionID {
		return errors.New("accepted Workflow result does not bind the final process and Claude stream")
	}
	return nil
}

func validateDirectAuditPopulation(report DirectEvidenceAudit, seen map[string]bool, entries []os.DirEntry) error {
	wantBoundaries := map[string]int{
		string(FaultNone): 3, string(FaultBeforeVendorRegistration): 3,
		string(FaultAfterToolEffect): 3, string(FaultAfterFinalOutput): 3,
	}
	if !reflect.DeepEqual(report.RunsByBoundary, wantBoundaries) || report.Runs != 12 ||
		report.UnfaultedRuns != 3 || report.UnsafeRuns != 9 || report.ValidPassVerdicts != 3 ||
		report.ValidFailVerdicts != 9 || report.VerdictsRecomputed != 12 || report.HistoriesReplayed != 12 ||
		report.RawInventoriesVerified != 12 || report.ProcessesObserved != 21 || report.PhysicalEffects != 21 || report.WorkspaceEffects != 21 ||
		report.AcceptedOutcomes != 12 || report.ProviderSessions != 21 {
		return errors.New("disk reconstruction does not match the exact direct population")
	}
	return validateListedRunDirectories(report.EvidenceRoot, seen, entries)
}

func WriteDirectEvidenceAudit(path string, report DirectEvidenceAudit) error {
	return writeEvidenceAudit(path, report.EvidenceRoot, report.AllRequirementsVerified, report)
}
