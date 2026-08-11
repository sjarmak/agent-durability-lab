package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const fencedEvidenceAuditVersion = "claude-direct-fenced-disk-audit-v1"

type FencedEvidenceAudit struct {
	Version                 string            `json:"version"`
	EvidenceRoot            string            `json:"evidence_root"`
	Runs                    int               `json:"runs"`
	UnfaultedRuns           int               `json:"unfaulted_runs"`
	ProtectedRuns           int               `json:"protected_runs"`
	RunsByBoundary          map[string]int    `json:"runs_by_boundary"`
	ValidPassVerdicts       int               `json:"valid_pass_verdicts"`
	VerdictsRecomputed      int               `json:"verdicts_recomputed"`
	HistoriesReplayed       int               `json:"histories_replayed"`
	RawInventoriesVerified  int               `json:"raw_inventories_verified"`
	RawArtifactsVerified    int               `json:"raw_artifacts_verified"`
	ProcessesObserved       int               `json:"processes_observed"`
	AttachmentsObserved     int               `json:"attachments_observed"`
	AuthoritativeEffects    int               `json:"authoritative_effects"`
	WorkspaceEffects        int               `json:"workspace_effects"`
	AcceptedOutcomes        int               `json:"accepted_outcomes"`
	CapabilityLeaks         int               `json:"capability_leaks"`
	SourceSHA256            map[string]string `json:"source_sha256"`
	ClaudeVersion           string            `json:"claude_version"`
	AllRequirementsVerified bool              `json:"all_requirements_verified"`
}

type auditedFencedAttempt struct {
	number  int32
	request ControlledEffectInput
	process ProcessRecord
	stream  ClaudeStreamResult
}

func AuditFencedEvidence(ctx context.Context, root string) (FencedEvidenceAudit, error) {
	population, err := inspectAuditedPopulation(ctx, root, 15)
	if err != nil {
		return FencedEvidenceAudit{}, err
	}
	absoluteRoot, entries, suite := population.root, population.entries, population.suite

	report := FencedEvidenceAudit{
		Version: fencedEvidenceAuditVersion, EvidenceRoot: absoluteRoot,
		RunsByBoundary: make(map[string]int), SourceSHA256: make(map[string]string),
	}
	seenDirectories := make(map[string]bool, len(suite.RunDirectories))
	var claudeVersion string
	for _, runDirectory := range suite.RunDirectories {
		run, err := readPopulationRun(ctx, population, seenDirectories, runDirectory)
		if err != nil {
			return FencedEvidenceAudit{}, err
		}
		manifest, storedVerdict, input := run.manifest, run.verdict, run.input
		summary, inventory, rawDirectory := run.summary, run.inventory, run.rawRoot
		attempt, err := readSingleFencedAttempt(rawDirectory, summary.SelectedVendorSessionID)
		if err != nil {
			return FencedEvidenceAudit{}, fmt.Errorf("audit attempt %s: %w", manifest.RunID, err)
		}
		if err := validateFencedAuditTrial(manifest, input, storedVerdict, summary, attempt); err != nil {
			return FencedEvidenceAudit{}, fmt.Errorf("audit safety facts for %s: %w", manifest.RunID, err)
		}
		leaks, err := countPublishedCapabilityLeaks(runDirectory, attempt.request.OwnerCapability)
		if err != nil {
			return FencedEvidenceAudit{}, err
		}
		if leaks != 0 {
			return FencedEvidenceAudit{}, fmt.Errorf("raw owner capability leaked into %d published artifacts for %s", leaks, manifest.RunID)
		}
		if err := collectSourceIdentity(report.SourceSHA256, input); err != nil {
			return FencedEvidenceAudit{}, fmt.Errorf("source identity for %s: %w", manifest.RunID, err)
		}
		version := input.Settings["claude_version"]
		if version == "" {
			return FencedEvidenceAudit{}, errors.New("claude version is absent")
		}
		if claudeVersion == "" {
			claudeVersion = version
		} else if claudeVersion != version {
			return FencedEvidenceAudit{}, errors.New("claude version differs across admitted runs")
		}

		report.Runs++
		report.ValidPassVerdicts++
		report.VerdictsRecomputed++
		report.HistoriesReplayed++
		report.RawInventoriesVerified++
		report.RawArtifactsVerified += len(inventory.Files)
		report.ProcessesObserved++
		report.AuthoritativeEffects += len(summary.Authority.Effects)
		report.WorkspaceEffects += len(summary.WorkspaceEffects)
		if summary.Authority.Outcome != nil {
			report.AcceptedOutcomes++
		}
		for _, event := range summary.Authority.Events {
			if event.Kind == "activity_reattached" {
				report.AttachmentsObserved++
			}
		}
		boundary := string(summary.FaultBoundary)
		report.RunsByBoundary[boundary]++
		if summary.Probe == protocol.ProbeUnfaulted {
			report.UnfaultedRuns++
		} else {
			report.ProtectedRuns++
		}
	}
	if err := validateFencedAuditPopulation(report, seenDirectories, entries); err != nil {
		return FencedEvidenceAudit{}, err
	}
	report.ClaudeVersion = claudeVersion
	report.AllRequirementsVerified = true
	return report, nil
}

func validateFencedAuditTrial(manifest protocol.Manifest, input protocol.EffectiveInput,
	verdict protocol.Verdict, summary trialSummary, attempt auditedFencedAttempt,
) error {
	request, process, stream := attempt.request, attempt.process, attempt.stream
	if manifest.Case != protocol.CaseAmbiguousEffect || manifest.SessionID == "" ||
		manifest.Probe != summary.Probe || manifest.Trial != summary.Trial ||
		verdict.RunID != manifest.RunID || verdict.Case != manifest.Case || verdict.Probe != manifest.Probe ||
		verdict.Trial != manifest.Trial || verdict.Class != protocol.VerdictValidPass || len(verdict.ReasonCodes) != 0 {
		return errors.New("manifest, summary, and valid-pass verdict identities differ")
	}
	metrics := verdict.Metrics
	if metrics.AcceptedOutcomeCount != 1 || metrics.PhysicalEffectCount != 1 ||
		metrics.PhysicalAttemptCount != 1 || metrics.ConcurrentOwnerCount != 1 ||
		metrics.StaleActionAcceptCount != 0 || metrics.PostCancelAcceptCount != 0 {
		return errors.New("recomputed verdict does not prove one owner/effect/outcome with no stale acceptance")
	}
	if summary.RecoveryMode != RecoveryModeFenced || input.Settings["recovery_mode"] != string(RecoveryModeFenced) ||
		!summary.ReplayVerified || input.Settings["workflow_history_replay_verified"] != "true" ||
		!validVendorSessionID(summary.SelectedVendorSessionID) ||
		input.Settings["selected_vendor_session_id"] != summary.SelectedVendorSessionID ||
		summary.WorkflowResult.VendorSessionID != summary.SelectedVendorSessionID ||
		summary.WorkflowResult.Result != "EFFECT_COMPLETE" || summary.WorkflowResult.ProcessIdentity == "" ||
		stream.SessionID != summary.SelectedVendorSessionID || stream.Result != summary.WorkflowResult.Result || stream.IsError {
		return errors.New("fenced session, replay, result, or process evidence is incomplete")
	}
	if summary.WorkspaceBeforeHash == "" || summary.WorkspaceAfterHash == "" ||
		summary.WorkspaceBeforeHash == summary.WorkspaceAfterHash ||
		input.Settings["workspace_effect_count"] != "1" || len(summary.WorkspaceEffects) != 1 ||
		len(summary.Destination.Attempts) != 1 || summary.Authority == nil {
		return errors.New("independent workspace or destination evidence is incomplete")
	}
	authority := summary.Authority
	if authority.SessionID != manifest.SessionID || authority.Mode != workstore.ModeFenced ||
		authority.ActiveGeneration != 1 || authority.ActiveOwnerTokenHash == "" || authority.Cancellation != nil ||
		len(authority.Executors) != 1 || len(authority.Effects) != 1 || authority.Outcome == nil ||
		authority.Outcome.Value != summary.WorkflowResult.Result {
		return errors.New("authority state does not contain one completed generation/effect/outcome")
	}
	executor := authority.Executors[0]
	effect := authority.Effects[0]
	if executor.Generation != 1 || executor.OwnerTokenHash != authority.ActiveOwnerTokenHash ||
		executor.PID <= 0 || executor.ProcessStart == "" || executor.Status != workstore.ExecutorStatusCompleted ||
		executor.PID != process.PID || executor.ProcessStart != process.StartIdentity ||
		executor.ProcessGroupID != process.ProcessGroupID ||
		effect.Generation != 1 || effect.OwnerTokenHash != authority.ActiveOwnerTokenHash ||
		effect.ID != request.LogicalEffectID || effect.Value != request.Payload {
		return errors.New("executor or effect authority differs from the active lease")
	}
	workspaceEffect := summary.WorkspaceEffects[0]
	destinationEffect := summary.Destination.Attempts[0]
	if !destinationEffect.Applied || destinationEffect.LogicalSessionID != manifest.SessionID ||
		destinationEffect.LogicalTurnID != request.LogicalTurnID || destinationEffect.LogicalEffectID != request.LogicalEffectID ||
		destinationEffect.PhysicalAttemptID != request.PhysicalAttemptID ||
		destinationEffect.ActorID != request.ActorID || destinationEffect.ProcessIdentity != process.Identity ||
		workspaceEffect.LogicalEffectID != request.LogicalEffectID || workspaceEffect.PhysicalAttemptID != request.PhysicalAttemptID ||
		workspaceEffect.Payload != request.Payload || workspaceEffect.ActorID != request.ActorID {
		return errors.New("workspace, destination, and capability request identities differ")
	}
	if request.LogicalSessionID != manifest.SessionID || request.OwnershipGeneration != 1 ||
		request.OwnerCapability == "" || workstore.HashToken(request.OwnerCapability) != authority.ActiveOwnerTokenHash {
		return errors.New("raw capability does not authorize the admitted generation")
	}
	wantAttempt := int32(1)
	wantAttachments := 0
	if manifest.Probe == protocol.ProbeProtected {
		wantAttempt = 2
		wantAttachments = 1
		if summary.FaultBoundary == FaultNone || input.Settings["fault_boundary"] != string(summary.FaultBoundary) {
			return errors.New("protected trial lacks its exact fault boundary")
		}
	} else if manifest.Probe != protocol.ProbeUnfaulted || summary.FaultBoundary != FaultNone {
		return errors.New("run is neither protected nor unfaulted")
	}
	attachments := 0
	for _, event := range authority.Events {
		if event.Kind != "activity_reattached" {
			continue
		}
		attachments++
		if event.Generation != 1 || event.Attempt != 2 || event.OwnerTokenHash != authority.ActiveOwnerTokenHash {
			return errors.New("retry did not attach to the exact generation-one capability")
		}
	}
	if summary.WorkflowResult.TemporalAttempt != wantAttempt || summary.WorkflowResult.PhysicalAttemptID != request.PhysicalAttemptID ||
		summary.WorkflowResult.ProcessIdentity != process.Identity || attempt.number != 1 || attachments != wantAttachments {
		return errors.New("temporal attempt and supervisor attachment counts differ")
	}
	return nil
}

func validateFencedAuditPopulation(report FencedEvidenceAudit, seen map[string]bool, entries []os.DirEntry) error {
	want := map[string]int{
		string(FaultNone):                     3,
		string(FaultAfterClaimBeforeExec):     3,
		string(FaultBeforeVendorRegistration): 3,
		string(FaultAfterToolEffect):          3,
		string(FaultAfterFinalOutput):         3,
	}
	if !reflect.DeepEqual(report.RunsByBoundary, want) || report.Runs != 15 || report.UnfaultedRuns != 3 ||
		report.ProtectedRuns != 12 || report.ValidPassVerdicts != 15 || report.VerdictsRecomputed != 15 ||
		report.HistoriesReplayed != 15 || report.RawInventoriesVerified != 15 || report.ProcessesObserved != 15 ||
		report.AttachmentsObserved != 12 || report.AuthoritativeEffects != 15 || report.WorkspaceEffects != 15 ||
		report.AcceptedOutcomes != 15 || report.CapabilityLeaks != 0 {
		return errors.New("disk reconstruction does not match the exact fenced population")
	}
	return validateListedRunDirectories(report.EvidenceRoot, seen, entries)
}

func readSingleFencedAttempt(rawDirectory, selectedSession string) (auditedFencedAttempt, error) {
	attemptRoot := filepath.Join(rawDirectory, "attempts")
	entries, err := os.ReadDir(attemptRoot)
	if err != nil {
		return auditedFencedAttempt{}, fmt.Errorf("read attempts: %w", err)
	}
	var attemptDirectory string
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || attemptDirectory != "" {
			return auditedFencedAttempt{}, errors.New("attempt root must contain exactly one real attempt directory")
		}
		attemptDirectory = filepath.Join(attemptRoot, entry.Name())
	}
	if attemptDirectory == "" {
		return auditedFencedAttempt{}, errors.New("attempt root is empty")
	}
	name := filepath.Base(attemptDirectory)
	number, err := parseAttemptNumber(name)
	if err != nil {
		return auditedFencedAttempt{}, err
	}
	request, err := readAuditedEffectRequest(filepath.Join(attemptDirectory, effectRequestFile))
	if err != nil {
		return auditedFencedAttempt{}, err
	}
	process, err := readStrictJSON[ProcessRecord](filepath.Join(attemptDirectory, name+".process-started.json"))
	if err != nil {
		return auditedFencedAttempt{}, err
	}
	stream, err := readAuditedClaudeStream(filepath.Join(attemptDirectory, name+".stdout.jsonl"))
	if err != nil {
		return auditedFencedAttempt{}, err
	}
	if process.AttemptID != name || process.ActorID != request.ActorID || process.Identity == "" ||
		process.State != "running" || request.PhysicalAttemptID != name || stream.SessionID != selectedSession ||
		stream.Result != "EFFECT_COMPLETE" || stream.IsError || request.OwnershipGeneration != uint64(number) {
		return auditedFencedAttempt{}, errors.New("fenced process, request, and stream identities differ")
	}
	if err := validateRecordedInvocation(process, RecoveryModeFenced, selectedSession, number); err != nil {
		return auditedFencedAttempt{}, fmt.Errorf("validate fenced invocation: %w", err)
	}
	processCount := 0
	err = filepath.WalkDir(attemptRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".process-started.json") {
			processCount++
		}
		return nil
	})
	if err != nil {
		return auditedFencedAttempt{}, err
	}
	if processCount != 1 {
		return auditedFencedAttempt{}, errors.New("fenced attempt root does not contain exactly one process record")
	}
	return auditedFencedAttempt{number: number, request: request, process: process, stream: stream}, nil
}

func auditPreservedHistory(path string, manifest protocol.Manifest, summary trialSummary) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect Temporal history: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<20 {
		return errors.New("temporal history is not a bounded regular artifact")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read Temporal history: %w", err)
	}
	value := &history.History{}
	if err := protojson.Unmarshal(encoded, value); err != nil {
		return fmt.Errorf("decode Temporal history: %w", err)
	}
	if err := validatePreservedHistory(value, manifest.SessionID, summary); err != nil {
		return err
	}
	return replayDecodedWorkflowHistory(value)
}

func countPublishedCapabilityLeaks(runDirectory, capability string) (int, error) {
	if capability == "" {
		return 0, errors.New("empty capability cannot be audited")
	}
	names := append(protocol.RawEvidenceFiles(), protocol.VerdictFile)
	leaks := 0
	for _, name := range names {
		path := filepath.Join(runDirectory, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return 0, fmt.Errorf("read published artifact %s: %w", name, err)
		}
		if bytes.Contains(data, []byte(capability)) {
			leaks++
		}
	}
	return leaks, nil
}

func collectSourceIdentity(sources map[string]string, input protocol.EffectiveInput) error {
	values := map[string]string{
		"claude":   input.AgentBinarySHA256,
		"harness":  input.Settings["harness_binary_sha256"],
		"worker":   input.Settings["worker_binary_sha256"],
		"effect":   input.Settings["effect_binary_sha256"],
		"launcher": input.Settings["launcher_binary_sha256"],
	}
	return collectNamedSourceIdentity(sources, values)
}

func collectLegacySourceIdentity(sources map[string]string, input protocol.EffectiveInput) error {
	values := map[string]string{
		"claude":   input.AgentBinarySHA256,
		"worker":   input.Settings["worker_binary_sha256"],
		"effect":   input.Settings["effect_binary_sha256"],
		"launcher": input.Settings["launcher_binary_sha256"],
	}
	return collectNamedSourceIdentity(sources, values)
}

func collectCompatibleSourceIdentity(sources map[string]string, input protocol.EffectiveInput, harnessBound bool) error {
	if harnessBound {
		return collectSourceIdentity(sources, input)
	}
	if input.Settings["harness_binary_sha256"] != "" {
		return errors.New("harness SHA-256 appears in a legacy source population")
	}
	return collectLegacySourceIdentity(sources, input)
}

func collectNamedSourceIdentity(sources, values map[string]string) error {
	for name, value := range values {
		if !validSHA256Digest(value) {
			return fmt.Errorf("%s SHA-256 is absent", name)
		}
		if previous := sources[name]; previous != "" && previous != value {
			return fmt.Errorf("%s SHA-256 differs across runs", name)
		}
		sources[name] = value
	}
	return nil
}

func readStrictJSON[T any](path string) (T, error) {
	var value T
	info, err := os.Lstat(path)
	if err != nil {
		return value, fmt.Errorf("inspect %s: %w", filepath.Base(path), err)
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<20 {
		return value, fmt.Errorf("%s is not a bounded regular JSON artifact", filepath.Base(path))
	}
	file, err := os.Open(path)
	if err != nil {
		return value, fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 16<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return value, fmt.Errorf("decode %s: trailing content", filepath.Base(path))
	}
	return value, nil
}

func WriteFencedEvidenceAudit(path string, report FencedEvidenceAudit) error {
	return writeEvidenceAudit(path, report.EvidenceRoot, report.AllRequirementsVerified, report)
}

func WriteResumeEvidenceAudit(path string, report ResumeEvidenceAudit) error {
	return writeEvidenceAudit(path, report.EvidenceRoot, report.AllRequirementsVerified, report)
}

func writeEvidenceAudit(path, evidenceRoot string, verified bool, report any) error {
	if path == "" || evidenceRoot == "" || !verified {
		return errors.New("audit output path and verified report are required")
	}
	absoluteOutput, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve audit output: %w", err)
	}
	absoluteRoot, err := filepath.Abs(evidenceRoot)
	if err != nil {
		return fmt.Errorf("resolve audited root: %w", err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(filepath.Clean(absoluteRoot))
	if err != nil {
		return fmt.Errorf("resolve audited root: %w", err)
	}
	resolvedOutput, err := resolveThroughExistingAncestor(filepath.Clean(absoluteOutput))
	if err != nil {
		return fmt.Errorf("resolve audit output: %w", err)
	}
	relative, err := filepath.Rel(canonicalRoot, resolvedOutput)
	if err != nil {
		return fmt.Errorf("compare audit output and evidence root: %w", err)
	}
	if relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("audit report must be written outside the sealed evidence root")
	}
	return writeJSONExclusive(filepath.Clean(absoluteOutput), report)
}

func resolveThroughExistingAncestor(path string) (string, error) {
	current := path
	var missing []string
	for {
		_, err := os.Lstat(current)
		if err == nil {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", err
			}
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}
