package lab

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const evidenceAuditVersion = "codex-direct-disk-audit-v1"

type EvidenceAudit struct {
	Version                 string            `json:"version"`
	EvidenceRoot            string            `json:"evidence_root"`
	Mode                    RecoveryMode      `json:"mode"`
	Trials                  int               `json:"trials"`
	Runs                    int               `json:"runs"`
	RunsByBoundary          map[string]int    `json:"runs_by_boundary"`
	ValidPassRuns           int               `json:"valid_pass_runs"`
	DistinguishingFailRuns  int               `json:"distinguishing_fail_runs"`
	HistoriesReplayed       int               `json:"histories_replayed"`
	RawInventoriesVerified  int               `json:"raw_inventories_verified"`
	RawArtifactsVerified    int               `json:"raw_artifacts_verified"`
	ProcessesObserved       int               `json:"processes_observed"`
	ThreadsObserved         int               `json:"threads_observed"`
	PhysicalEffects         int               `json:"physical_effects"`
	AttachmentsObserved     int               `json:"attachments_observed"`
	ReplacementsObserved    int               `json:"replacements_observed"`
	CancellationsObserved   int               `json:"cancellations_observed"`
	CapabilityLeaks         int               `json:"capability_leaks"`
	SourceSHA256            map[string]string `json:"source_sha256"`
	CodexVersion            string            `json:"codex_version"`
	Model                   string            `json:"model"`
	ReasoningEffort         string            `json:"reasoning_effort"`
	AllRequirementsVerified bool              `json:"all_requirements_verified"`
}

type auditedTrial struct {
	summary      trialSummary
	inventory    rawInventory
	effectCount  int
	attachments  int
	replacement  bool
	cancellation bool
}

func AuditEvidence(ctx context.Context, root string) (EvidenceAudit, error) {
	absolute, entries, suite, err := inspectEvidenceRoot(ctx, root)
	if err != nil {
		return EvidenceAudit{}, err
	}
	if len(suite.RunDirectories) == 0 {
		return EvidenceAudit{}, errors.New("suite summary contains no runs")
	}
	report := EvidenceAudit{
		Version: evidenceAuditVersion, EvidenceRoot: absolute,
		RunsByBoundary: make(map[string]int), SourceSHA256: make(map[string]string),
	}
	seen := make(map[string]bool, len(suite.RunDirectories))
	seenSchedule := make(map[string]bool, len(suite.RunDirectories))
	var metadata experimentMetadata
	for _, recorded := range suite.RunDirectories {
		if err := ctx.Err(); err != nil {
			return EvidenceAudit{}, err
		}
		name := filepath.Base(filepath.Clean(recorded))
		directory := filepath.Join(absolute, name)
		if name == "." || name == "" || seen[name] {
			return EvidenceAudit{}, fmt.Errorf("suite contains invalid or duplicate run %q", recorded)
		}
		seen[name] = true
		trial, err := auditTrial(ctx, directory)
		if err != nil {
			return EvidenceAudit{}, fmt.Errorf("audit %s: %w", name, err)
		}
		if report.Runs == 0 {
			report.Mode = trial.summary.Mode
			metadata = trial.summary.Metadata
		} else if trial.summary.Mode != report.Mode || !reflect.DeepEqual(metadata, trial.summary.Metadata) {
			return EvidenceAudit{}, errors.New("mode or pinned input identity differs across runs")
		}
		key := fmt.Sprintf("%d/%s", trial.summary.Trial, trial.summary.FaultBoundary)
		if seenSchedule[key] {
			return EvidenceAudit{}, fmt.Errorf("duplicate trial schedule entry %s", key)
		}
		seenSchedule[key] = true
		report.Runs++
		report.RunsByBoundary[string(trial.summary.FaultBoundary)]++
		report.HistoriesReplayed++
		report.RawInventoriesVerified++
		report.RawArtifactsVerified += len(trial.inventory.Files)
		report.ProcessesObserved += len(trial.summary.Attempts)
		for _, attempt := range trial.summary.Attempts {
			if attempt.Thread != nil {
				report.ThreadsObserved++
			}
		}
		report.PhysicalEffects += trial.effectCount
		report.AttachmentsObserved += trial.attachments
		if trial.replacement {
			report.ReplacementsObserved++
		}
		if trial.cancellation {
			report.CancellationsObserved++
		}
		if trial.summary.Verdict.NegativeControlTriggered {
			report.DistinguishingFailRuns++
		} else {
			report.ValidPassRuns++
		}
	}
	if err := validateEvidencePopulation(report, suite, entries, seen, seenSchedule); err != nil {
		return EvidenceAudit{}, err
	}
	if err := collectAuditSources(report.SourceSHA256, metadata); err != nil {
		return EvidenceAudit{}, err
	}
	report.Trials = len(suite.RunDirectories) / len(experimentSchedule(report.Mode))
	report.CodexVersion = metadata.CodexVersion
	report.Model = metadata.Model
	report.ReasoningEffort = metadata.ReasoningEffort
	report.AllRequirementsVerified = true
	return report, nil
}

func inspectEvidenceRoot(ctx context.Context, root string) (string, []os.DirEntry, ExperimentResult, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, ExperimentResult{}, err
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", nil, ExperimentResult{}, err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, ExperimentResult{}, errors.New("evidence root is not a real directory")
	}
	if _, err := os.Lstat(filepath.Join(absolute, "failure.json")); err == nil {
		return "", nil, ExperimentResult{}, errors.New("evidence root contains a suite failure")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", nil, ExperimentResult{}, err
	}
	entries, err := os.ReadDir(absolute)
	if err != nil {
		return "", nil, ExperimentResult{}, err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".staging-") || entry.Type()&os.ModeSymlink != 0 {
			return "", nil, ExperimentResult{}, fmt.Errorf("unfinished or symlinked root entry %q", entry.Name())
		}
	}
	suite, err := readStrictJSON[ExperimentResult](filepath.Join(absolute, "suite-summary.json"))
	return absolute, entries, suite, err
}

func auditTrial(ctx context.Context, directory string) (auditedTrial, error) {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return auditedTrial{}, errors.New("run is not a real directory")
	}
	summaryPath := filepath.Join(directory, "trial-summary.json")
	summary, err := readStrictJSON[trialSummary](summaryPath)
	if err != nil {
		return auditedTrial{}, err
	}
	if err := validateSummaryIdentity(directory, summary); err != nil {
		return auditedTrial{}, err
	}
	inventoryHash, err := protocol.FileSHA256(filepath.Join(directory, rawInventoryFile))
	if err != nil {
		return auditedTrial{}, err
	}
	inventory, err := verifyRawInventory(directory, inventoryHash)
	if err != nil {
		return auditedTrial{}, err
	}
	history, err := readBoundedFile(filepath.Join(directory, "workflow-history.json"), maxEvidenceJSONBytes)
	if err != nil {
		return auditedTrial{}, err
	}
	decodedHistory := &historypb.History{}
	if err := protojson.Unmarshal(history, decodedHistory); err != nil {
		return auditedTrial{}, fmt.Errorf("decode Temporal history for identity audit: %w", err)
	}
	if err := validatePreservedCodexHistory(decodedHistory, summary); err != nil {
		return auditedTrial{}, err
	}
	if err := replayWorkflowHistory(history); err != nil {
		return auditedTrial{}, err
	}
	attempts, err := collectTrialAttempts(filepath.Join(directory, "attempts"), summary.FaultBoundary,
		summary.Metadata.EffectBinaryPath)
	if err != nil {
		return auditedTrial{}, err
	}
	if !reflect.DeepEqual(attempts, summary.Attempts) {
		return auditedTrial{}, errors.New("attempt summary differs from raw process/thread/stream evidence")
	}
	workspace, err := ReadWorkspaceEffects(filepath.Join(directory, "fixture", "effects.jsonl"))
	if err != nil && !(summary.FaultBoundary == FaultCancellationWhileExecuting && errors.Is(err, os.ErrNotExist)) {
		return auditedTrial{}, err
	}
	after, err := hashWorkspace(filepath.Join(directory, "fixture"))
	if err != nil {
		return auditedTrial{}, err
	}
	if !reflect.DeepEqual(workspace, summary.WorkspaceEffects) || after != summary.WorkspaceAfterHash {
		return auditedTrial{}, errors.New("workspace summary differs from independent journal or hash")
	}
	result := auditedTrial{summary: summary, inventory: inventory}
	if summary.Mode == RecoveryModeFenced {
		authority, err := workstore.ReadSnapshot(ctx, filepath.Join(directory, "authority.db"), summary.LogicalSessionID)
		if err != nil {
			return auditedTrial{}, err
		}
		if summary.Authority == nil || !reflect.DeepEqual(authority, *summary.Authority) {
			return auditedTrial{}, errors.New("authority summary differs from the durable authority store")
		}
		result.effectCount = len(authority.Effects)
		for _, event := range authority.Events {
			switch event.Kind {
			case "activity_reattached":
				result.attachments++
			case "owner_replaced", "pending_launch_replaced":
				result.replacement = true
			case "cancellation_committed":
				result.cancellation = true
			}
		}
	} else {
		destination, err := ReadDestination(ctx, filepath.Join(directory, "destination.db"))
		if err != nil {
			return auditedTrial{}, err
		}
		if summary.Authority != nil || !reflect.DeepEqual(destination, summary.Destination) {
			return auditedTrial{}, errors.New("destination summary differs from the durable destination store")
		}
		result.effectCount = len(destination.Attempts)
	}
	if err := validateAuditedTrial(directory, result); err != nil {
		return auditedTrial{}, err
	}
	return result, nil
}

func validateSummaryIdentity(directory string, summary trialSummary) error {
	name := filepath.Base(directory)
	if summary.SchemaVersion != "codex-direct-trial-v1" || summary.LogicalSessionID != name ||
		summary.LogicalTurnID != "turn-1" || summary.LogicalEffectID != "effect-1" ||
		summary.WorkflowID != "codex-direct/"+name || summary.WorkflowRunID == "" ||
		summary.Trial < 1 || !summary.Mode.valid() || !summary.FaultBoundary.valid() ||
		!summary.ReplayVerified || summary.StartedAt.IsZero() || !summary.StartedAt.Before(summary.CompletedAt) ||
		summary.WorkspaceBeforeHash == "" || summary.WorkspaceAfterHash == "" {
		return errors.New("trial summary has incomplete or inconsistent stable identity")
	}
	wantName := fmt.Sprintf("codex-direct-%s-%s-trial-%d", summary.Mode, summary.FaultBoundary, summary.Trial)
	if name != wantName {
		return fmt.Errorf("run directory %q does not match summary identity %q", name, wantName)
	}
	if summary.FaultBoundary == FaultNone {
		if !summary.FaultAt.IsZero() {
			return errors.New("unfaulted trial contains an injected-fault timestamp")
		}
	} else {
		if summary.FaultAt.IsZero() || len(summary.BarrierArrivals) == 0 ||
			summary.FaultAt.Before(summary.StartedAt) || summary.FaultAt.After(summary.CompletedAt) {
			return errors.New("faulted trial lacks an exact bracketed barrier")
		}
		bracketed := false
		wantPoint := faultBarrierPoint(summary.FaultBoundary)
		for _, arrival := range summary.BarrierArrivals {
			if arrival.SessionID != summary.LogicalSessionID || arrival.PID <= 0 || arrival.ProcessStart == "" {
				return errors.New("barrier arrival lacks stable session/process identity")
			}
			if arrival.Point == wantPoint && !arrival.Time.After(summary.FaultAt) {
				bracketed = true
			}
		}
		if !bracketed {
			return errors.New("fault time is not preceded by an exact barrier arrival")
		}
	}
	return validateExperimentMetadata(summary.Metadata)
}

func faultBarrierPoint(boundary FaultBoundary) string {
	switch boundary {
	case FaultAfterClaimBeforeExec:
		return claimBeforeExecBarrier
	case FaultBeforeThreadObservation, FaultProcessFailureReplacement:
		return preThreadBarrier
	case FaultAfterThreadBeforeRegistration, FaultCancellationWhileExecuting:
		return threadRegistrationBarrier
	case FaultAfterToolEffect, FaultConcurrentRecovery:
		return committedEffectBarrier
	case FaultAfterFinalOutput:
		return finalOutputBarrier
	default:
		return ""
	}
}

func validateExperimentMetadata(metadata experimentMetadata) error {
	values := []string{
		metadata.CodexBinarySHA256, metadata.CodexWrapperSHA256, metadata.WorkerSHA256,
		metadata.EffectSHA256, metadata.LauncherSHA256, metadata.SchemaSHA256, metadata.HarnessSHA256,
	}
	for _, value := range values {
		if !validSHA256(value) {
			return errors.New("trial metadata contains an invalid source SHA-256")
		}
	}
	if metadata.CodexVersion == "" || metadata.CodexBinaryPath == "" || metadata.CodexWrapperPath == "" ||
		metadata.CodexHomePath == "" || metadata.Model == "" || metadata.ReasoningEffort == "" ||
		!safeCommandPath(metadata.EffectBinaryPath) || !safeCommandPath(metadata.LauncherBinaryPath) ||
		!safeCommandPath(metadata.OutputSchemaPath) ||
		metadata.Sandbox != "workspace-write" || metadata.InvocationPath != "pinned-underlying-cli-with-codex-2-profile" ||
		metadata.Authentication == "" || metadata.Hermetic && metadata.Authentication != "not-applicable-hermetic" ||
		!metadata.Hermetic && metadata.Authentication != "wrapper-and-pinned-cli-profile-logged-in-using-chatgpt" {
		return errors.New("trial metadata lacks pinned Codex CLI/model/profile identity")
	}
	return nil
}

func validateAuditedTrial(directory string, trial auditedTrial) error {
	summary := trial.summary
	if !summary.Verdict.Admitted || len(summary.Attempts) == 0 {
		return errors.New("trial is not admitted or contains no process attempts")
	}
	if summary.FaultBoundary != FaultCancellationWhileExecuting &&
		(summary.WorkflowResult.Result != "EFFECT_COMPLETE" || !validThreadID(summary.WorkflowResult.ThreadID)) {
		return errors.New("successful trial lacks a complete accepted Workflow result")
	}
	if err := validateAttemptIdentities(directory, summary); err != nil {
		return err
	}
	if summary.Mode == RecoveryModeFenced {
		return validateFencedTrial(trial)
	}
	return validateControlTrial(trial)
}

func validateControlTrial(trial auditedTrial) error {
	summary := trial.summary
	wantEffects := 1
	wantNegative := false
	if summary.FaultBoundary == FaultAfterToolEffect || summary.FaultBoundary == FaultAfterFinalOutput {
		wantEffects, wantNegative = trial.effectCount, true
		if wantEffects < 1 || wantEffects > 2 {
			return errors.New("post-execution control has an invalid physical-effect count")
		}
	}
	if trial.effectCount != wantEffects || len(summary.WorkspaceEffects) != wantEffects ||
		summary.Verdict.NegativeControlTriggered != wantNegative || summary.Verdict.SafetyPassed == wantNegative {
		return errors.New("control verdict does not match independently observed physical effects")
	}
	wantAttempts, wantThreads, wantCompleteStreams := 1, 1, 1
	if summary.FaultBoundary != FaultNone {
		wantAttempts = 2
	}
	switch summary.FaultBoundary {
	case FaultBeforeThreadObservation:
		wantThreads = 1
	case FaultAfterToolEffect:
		wantThreads, wantCompleteStreams = 2, 1
	case FaultAfterFinalOutput:
		wantThreads, wantCompleteStreams = 2, 2
	}
	if len(summary.Attempts) != wantAttempts || len(observedThreadIDs(summary.Attempts)) != wantThreads ||
		completeStreamCount(summary.Attempts) != wantCompleteStreams ||
		summary.WorkspaceBeforeHash == summary.WorkspaceAfterHash {
		return errors.New("control process, thread, stream, or workspace transition differs from the exact boundary")
	}
	last := summary.Attempts[len(summary.Attempts)-1]
	if !last.StreamComplete || last.Thread == nil || summary.WorkflowResult.PhysicalAttemptID != last.PhysicalAttemptID ||
		summary.WorkflowResult.ProcessIdentity != last.Process.Identity || summary.WorkflowResult.ThreadID != last.Thread.ThreadID {
		return errors.New("accepted control outcome does not match the final complete process/thread attempt")
	}
	if wantNegative {
		wantReasons := []string{"competing_execution"}
		if wantEffects == 2 {
			wantReasons = append(wantReasons, "duplicate_effect")
		}
		if !reflect.DeepEqual(summary.Verdict.ReasonCodes, wantReasons) {
			return errors.New("distinguishing control failure reasons differ from independent observations")
		}
	} else if len(summary.Verdict.ReasonCodes) != 0 {
		return errors.New("passing control contains failure reasons")
	}
	threads := observedThreadIDs(summary.Attempts)
	if summary.Mode == RecoveryModeResumeOnly && len(threads) > 1 {
		for _, threadID := range threads[1:] {
			if threadID != threads[0] {
				return errors.New("explicit resume did not preserve the learned Codex thread")
			}
		}
	}
	if summary.Mode == RecoveryModeUnsafeFresh && wantNegative && len(threads) == 2 && threads[0] == threads[1] {
		return errors.New("unsafe fresh relaunch unexpectedly reused a Codex thread")
	}
	return validateWorkspaceDestination(summary.WorkspaceEffects, summary.Destination.Attempts)
}

func validateFencedTrial(trial auditedTrial) error {
	summary, authority := trial.summary, trial.summary.Authority
	if authority == nil || authority.SessionID != summary.LogicalSessionID || authority.Mode != workstore.ModeFenced ||
		!summary.Verdict.SafetyPassed || summary.Verdict.NegativeControlTriggered || len(summary.Verdict.ReasonCodes) != 0 {
		return errors.New("fenced trial lacks one consistent protected authority verdict")
	}
	if len(summary.Attempts) != len(authority.Executors) {
		return errors.New("fenced process attempts and durable executors differ")
	}
	for index, attempt := range summary.Attempts {
		executor := authority.Executors[index]
		if attempt.Request == nil || attempt.Request.OwnershipGeneration != executor.Generation ||
			attempt.Request.OwnerCapabilitySHA256 != executor.OwnerTokenHash || attempt.Process == nil ||
			attempt.Process.PID != executor.PID || attempt.Process.StartIdentity != executor.ProcessStart ||
			attempt.Process.ProcessGroupID != executor.ProcessGroupID {
			return errors.New("fenced request capability or process identity differs from its durable executor")
		}
	}
	if summary.FaultBoundary == FaultCancellationWhileExecuting {
		if trial.effectCount != 0 || len(summary.WorkspaceEffects) != 0 || authority.Outcome != nil ||
			authority.Cancellation == nil || authority.Cancellation.Acknowledgement == nil || !trial.cancellation ||
			summary.WorkspaceBeforeHash != summary.WorkspaceAfterHash || len(summary.Attempts) != 1 ||
			summary.Attempts[0].Thread == nil || summary.Attempts[0].StreamComplete {
			return errors.New("cancellation did not revoke and acknowledge the exact owner before effects/outcome")
		}
		executor := authority.Executors[0]
		cancellation := authority.Cancellation
		if executor.Status != workstore.ExecutorStatusCanceled || cancellation.Generation != executor.Generation ||
			cancellation.OwnerTokenHash != executor.OwnerTokenHash || cancellation.Target.Process.PID != executor.PID ||
			cancellation.Acknowledgement.Process.PID != executor.PID {
			return errors.New("cancellation target/acknowledgement differs from the revoked executor")
		}
		return nil
	}
	if trial.effectCount != 1 || len(summary.WorkspaceEffects) != 1 || authority.Outcome == nil ||
		authority.Outcome.Value != summary.WorkflowResult.Result || summary.WorkspaceBeforeHash == summary.WorkspaceAfterHash {
		return errors.New("fenced trial does not contain one effect, workspace mutation, and outcome")
	}
	wantGeneration, wantExecutors, wantAttachments := uint64(1), 1, 0
	if summary.FaultBoundary == FaultProcessFailureReplacement {
		wantGeneration, wantExecutors = 2, 2
		if !trial.replacement {
			return errors.New("authorized process failure lacks a committed replacement")
		}
	} else if summary.FaultBoundary == FaultConcurrentRecovery {
		wantAttachments = 2
	} else if summary.FaultBoundary != FaultNone {
		wantAttachments = 1
	}
	if authority.ActiveGeneration != wantGeneration || len(authority.Executors) != wantExecutors ||
		trial.attachments != wantAttachments || authority.Effects[0].Generation != wantGeneration {
		return errors.New("fenced generation, executor, effect, or attachment count differs from the exact boundary")
	}
	if summary.FaultBoundary == FaultProcessFailureReplacement {
		if authority.Executors[0].Status != workstore.ExecutorStatusSuperseded ||
			authority.Executors[1].Status != workstore.ExecutorStatusCompleted ||
			summary.Attempts[0].Thread != nil || summary.Attempts[1].Thread == nil {
			return errors.New("replacement did not supersede the threadless process before generation two completed")
		}
	} else if len(summary.Attempts) != 1 || summary.Attempts[0].Thread == nil {
		return errors.New("fenced start-or-attach launched more than one Codex process/thread")
	}
	if authority.Effects[0].ID != summary.LogicalEffectID || authority.ActiveOwnerTokenHash == "" {
		return errors.New("fenced effect does not match the active logical authority")
	}
	last := summary.Attempts[len(summary.Attempts)-1]
	if !last.StreamComplete || last.Thread == nil || summary.WorkflowResult.PhysicalAttemptID != last.PhysicalAttemptID ||
		summary.WorkflowResult.ProcessIdentity != last.Process.Identity || summary.WorkflowResult.ThreadID != last.Thread.ThreadID ||
		authority.ActiveOwnerTokenHash != last.Request.OwnerCapabilitySHA256 ||
		authority.Effects[0].OwnerTokenHash != authority.ActiveOwnerTokenHash {
		return errors.New("accepted fenced outcome/effect does not match the active complete process/thread authority")
	}
	return nil
}

func validateAttemptIdentities(directory string, summary trialSummary) error {
	summaryBytes, err := os.ReadFile(filepath.Join(directory, "trial-summary.json"))
	if err != nil {
		return err
	}
	for _, attempt := range summary.Attempts {
		if attempt.Process == nil || attempt.Request == nil || attempt.PhysicalAttemptID != attempt.Process.AttemptID ||
			attempt.PhysicalAttemptID != attempt.Request.PhysicalAttemptID ||
			attempt.Process.ActorID != attempt.Request.ActorID || attempt.Process.Identity == "" ||
			attempt.Request.LogicalSessionID != summary.LogicalSessionID ||
			attempt.Request.LogicalTurnID != summary.LogicalTurnID || attempt.Request.LogicalEffectID != summary.LogicalEffectID {
			return errors.New("attempt process, request, and logical identities differ")
		}
		if attempt.Process.State != "running" {
			return errors.New("attempt start record does not describe a running process")
		}
		expectedBinary := summary.Metadata.CodexBinaryPath
		if summary.FaultBoundary == FaultBeforeThreadObservation || summary.FaultBoundary == FaultProcessFailureReplacement {
			expectedBinary = summary.Metadata.LauncherBinaryPath
		}
		expectedThread := ""
		if attempt.Thread != nil {
			expectedThread = attempt.Thread.ThreadID
		}
		if err := validatePinnedCodexProcess(*attempt.Process, summary.Metadata, expectedBinary, expectedThread); err != nil {
			return err
		}
		if attempt.StreamComplete {
			completedPath := filepath.Join(directory, "attempts", attempt.PhysicalAttemptID,
				attempt.PhysicalAttemptID+".process-completed.json")
			completed, err := readStrictJSON[ProcessRecord](completedPath)
			if err != nil {
				return err
			}
			if completed.State != "exited" || completed.Failure != "" ||
				completed.PID != attempt.Process.PID || completed.StartIdentity != attempt.Process.StartIdentity ||
				completed.ProcessGroupID != attempt.Process.ProcessGroupID || completed.Identity != attempt.Process.Identity {
				return errors.New("complete stream process exit differs from its start identity")
			}
		}
		if attempt.Thread != nil {
			if attempt.Thread.PhysicalAttemptID != attempt.PhysicalAttemptID ||
				attempt.Thread.ActorID != attempt.Process.ActorID || attempt.Thread.ProcessIdentity != attempt.Process.Identity ||
				attempt.ThreadID != "" && attempt.ThreadID != attempt.Thread.ThreadID {
				return errors.New("thread receipt differs from its exact process attempt")
			}
		}
		request, err := ReadControlledEffectRequest(filepath.Join(directory, "attempts", attempt.PhysicalAttemptID, effectRequestFile))
		if err != nil {
			return err
		}
		if request.OwnerCapability != "" {
			digest := workstore.HashToken(request.OwnerCapability)
			if digest != attempt.Request.OwnerCapabilitySHA256 || bytes.Contains(summaryBytes, []byte(request.OwnerCapability)) {
				return errors.New("raw owner capability is absent, mismatched, or leaked into the published summary")
			}
		}
	}
	return nil
}

func validatePinnedCodexProcess(process ProcessRecord, metadata experimentMetadata, expectedBinary, expectedThread string) error {
	effort := fmt.Sprintf("model_reasoning_effort=%q", metadata.ReasoningEffort)
	args := process.Args
	valid := len(args) == 15 && reflect.DeepEqual(args[:13], []string{
		"--cd", process.WorkDir, "exec", "--json", "--ignore-user-config", "--ignore-rules",
		"--model", metadata.Model, "-c", effort, "--sandbox", metadata.Sandbox, "--output-schema",
	}) && args[13] == metadata.OutputSchemaPath && args[14] == "-"
	if !valid && len(args) == 17 {
		valid = reflect.DeepEqual(args[:14], []string{
			"--cd", process.WorkDir, "exec", "--sandbox", metadata.Sandbox, "resume",
			"--json", "--ignore-user-config", "--ignore-rules", "--model", metadata.Model,
			"-c", effort, "--output-schema",
		}) && args[14] == metadata.OutputSchemaPath && args[15] == expectedThread && validThreadID(args[15]) && args[16] == "-"
	}
	if process.Binary != expectedBinary || !valid {
		return errors.New("recorded Codex process does not pin the admitted model, effort, and sandbox invocation")
	}
	return nil
}

func validateWorkspaceDestination(workspace []WorkspaceEffect, destination []EffectAttempt) error {
	if len(workspace) != len(destination) {
		return errors.New("workspace and destination physical effect counts differ")
	}
	byAttempt := make(map[string]WorkspaceEffect, len(workspace))
	for _, effect := range workspace {
		byAttempt[effect.PhysicalAttemptID] = effect
	}
	for _, attempt := range destination {
		effect, ok := byAttempt[attempt.PhysicalAttemptID]
		if !ok || !attempt.Applied || attempt.LogicalEffectID != effect.LogicalEffectID ||
			attempt.ActorID != effect.ActorID || attempt.ProcessIdentity != effect.ProcessIdentity {
			return errors.New("workspace and destination effect identities differ")
		}
	}
	return nil
}

func observedThreadIDs(attempts []trialAttemptEvidence) []string {
	var threads []string
	for _, attempt := range attempts {
		if attempt.Thread != nil {
			threads = append(threads, attempt.Thread.ThreadID)
		}
	}
	return threads
}

func completeStreamCount(attempts []trialAttemptEvidence) int {
	count := 0
	for _, attempt := range attempts {
		if attempt.StreamComplete {
			count++
		}
	}
	return count
}

func validateEvidencePopulation(report EvidenceAudit, suite ExperimentResult, entries []os.DirEntry,
	seen, seenSchedule map[string]bool,
) error {
	schedule := experimentSchedule(report.Mode)
	if len(schedule) == 0 || len(suite.RunDirectories)%len(schedule) != 0 {
		return errors.New("suite population is not an exact whole schedule")
	}
	trials := len(suite.RunDirectories) / len(schedule)
	for trial := 1; trial <= trials; trial++ {
		for _, boundary := range schedule {
			if !seenSchedule[fmt.Sprintf("%d/%s", trial, boundary)] {
				return fmt.Errorf("suite population lacks trial %d boundary %s", trial, boundary)
			}
		}
	}
	allowed := map[string]bool{"suite-summary.json": true, "temporal-server.log": true, "temporal.db": true}
	for name := range seen {
		allowed[name] = true
	}
	if len(entries) != len(allowed) {
		return errors.New("evidence root does not contain the exact sealed population")
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected evidence-root entry %q", entry.Name())
		}
		if seen[entry.Name()] {
			if !entry.IsDir() {
				return fmt.Errorf("evidence-root run is not a directory: %q", entry.Name())
			}
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("evidence-root artifact is not a regular file: %q", entry.Name())
		}
	}
	return nil
}

func collectAuditSources(destination map[string]string, metadata experimentMetadata) error {
	values := map[string]string{
		"codex": metadata.CodexBinarySHA256, "wrapper": metadata.CodexWrapperSHA256,
		"worker": metadata.WorkerSHA256, "effect": metadata.EffectSHA256,
		"launcher": metadata.LauncherSHA256, "schema": metadata.SchemaSHA256,
		"harness": metadata.HarnessSHA256,
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !validSHA256(values[name]) {
			return fmt.Errorf("%s source SHA-256 is invalid", name)
		}
		destination[name] = values[name]
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func readBoundedFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maximum {
		return nil, fmt.Errorf("artifact %q is not a bounded regular file", path)
	}
	return os.ReadFile(path)
}
