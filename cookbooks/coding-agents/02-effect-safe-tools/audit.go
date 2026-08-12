package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const maxAuditFileBytes = 16 << 20

type auditReport struct {
	Runs                     int
	UnsafePhysicalEffects    int
	ProtectedPhysicalEffects int
	VerifiedGitBundles       int
	VerifiedArtifactFiles    int
}

type manifest struct {
	Experiment      string      `json:"experiment"`
	Destination     Destination `json:"destination"`
	Mode            string      `json:"mode"`
	EffectID        string      `json:"effect_id"`
	FailureBoundary string      `json:"failure_boundary"`
}

type observations struct {
	Destination      Destination      `json:"destination"`
	Mode             string           `json:"mode"`
	EffectID         string           `json:"effect_id"`
	Attempts         []attempt        `json:"attempts"`
	Kill             killObservation  `json:"kill"`
	DestinationState destinationState `json:"destination_state"`
	History          history          `json:"history"`
	WorkflowOutcome  string           `json:"workflow_outcome"`
}

type attempt struct {
	Attempt           int       `json:"attempt"`
	WorkerID          string    `json:"worker_id"`
	PID               int       `json:"pid"`
	StartedAt         time.Time `json:"started_at"`
	EffectRespondedAt time.Time `json:"effect_responded_at"`
	Outcome           string    `json:"outcome"`
	Receipt           string    `json:"receipt"`
}

type killObservation struct {
	BarrierObservedAt time.Time `json:"barrier_observed_at"`
	KilledAt          time.Time `json:"killed_at"`
	WorkerID          string    `json:"worker_id"`
	PID               int       `json:"pid"`
	Signal            string    `json:"signal"`
}

type destinationState struct {
	PhysicalEffects []physicalEffect `json:"physical_effects"`
}

type physicalEffect struct {
	PhysicalID string      `json:"physical_id"`
	LogicalID  string      `json:"logical_id"`
	Receipt    string      `json:"receipt"`
	Attempt    int         `json:"attempt"`
	Kind       Destination `json:"kind"`
}

type history struct {
	RetryTimedOut    bool `json:"retry_timed_out"`
	CompletedCount   int  `json:"completed_count"`
	CompletedAttempt int  `json:"completed_attempt"`
}

type verdict struct {
	RunValid            bool `json:"run_valid"`
	ExpectedObservation bool `json:"expected_observation"`
	InvariantSatisfied  bool `json:"invariant_satisfied"`
}

type temporalHistory struct {
	Events []historyEvent `json:"events"`
}

type historyEvent struct {
	EventID                                 string `json:"event_id"`
	EventType                               string `json:"event_type"`
	WorkflowExecutionStartedEventAttributes struct {
		Input struct {
			Payloads []struct {
				Data string `json:"data"`
			} `json:"payloads"`
		} `json:"input"`
	} `json:"workflow_execution_started_event_attributes"`
	ActivityTaskStartedEventAttributes struct {
		Attempt     int `json:"attempt"`
		LastFailure struct {
			TimeoutFailureInfo struct {
				TimeoutType string `json:"timeout_type"`
			} `json:"timeout_failure_info"`
		} `json:"last_failure"`
	} `json:"activity_task_started_event_attributes"`
	ActivityTaskCompletedEventAttributes struct {
		StartedEventID string `json:"started_event_id"`
	} `json:"activity_task_completed_event_attributes"`
}

type recordedWorkflowInput struct {
	Destination Destination `json:"destination"`
	Mode        string      `json:"mode"`
	EffectID    string      `json:"effect_id"`
	Payload     string      `json:"payload"`
}

func auditFinalEvidence(repositoryRoot string) (auditReport, error) {
	evidenceRoot := filepath.Join(repositoryRoot, "experiments", "external-effects", "evidence")
	report, err := auditEvidenceRoot(evidenceRoot)
	if err != nil {
		return auditReport{}, err
	}
	for _, recipe := range recipes() {
		for _, mode := range []string{"unsafe", "protected"} {
			for trial := 1; trial <= 3; trial++ {
				runDirectory := filepath.Join(evidenceRoot, evidenceRunName(recipe, mode, trial))
				if err := requireRawEvidence(runDirectory); err != nil {
					return auditReport{}, err
				}
				switch recipe.Destination {
				case "git":
					var state destinationState
					if err := readJSON(filepath.Join(runDirectory, "destination-state.json"), &state); err != nil {
						return auditReport{}, err
					}
					input, err := readWorkflowInput(filepath.Join(runDirectory, "temporal-history.json"))
					if err != nil {
						return auditReport{}, err
					}
					if err := verifyGitBundle(runDirectory, input.Payload, state); err != nil {
						return auditReport{}, err
					}
					report.VerifiedGitBundles++
				case "artifact":
					count, err := verifyArtifactFiles(runDirectory)
					if err != nil {
						return auditReport{}, err
					}
					report.VerifiedArtifactFiles += count
				}
			}
		}
	}
	return report, nil
}

func auditEvidenceRoot(evidenceRoot string) (auditReport, error) {
	var report auditReport
	for _, recipe := range recipes() {
		for _, mode := range []string{"unsafe", "protected"} {
			for trial := 1; trial <= 3; trial++ {
				runName := evidenceRunName(recipe, mode, trial)
				runDirectory := filepath.Join(evidenceRoot, runName)
				physicalEffects, err := auditRun(runDirectory, recipe, mode)
				if err != nil {
					return auditReport{}, fmt.Errorf("%s: %w", runName, err)
				}
				report.Runs++
				if mode == "unsafe" {
					report.UnsafePhysicalEffects += physicalEffects
				} else {
					report.ProtectedPhysicalEffects += physicalEffects
				}
			}
		}
	}
	return report, nil
}

func auditRun(runDirectory string, recipe Recipe, mode string) (int, error) {
	if err := requireEvidenceDirectory(runDirectory); err != nil {
		return 0, err
	}
	var runManifest manifest
	var runObservations observations
	var independentState destinationState
	var runVerdict verdict
	for path, target := range map[string]any{
		"manifest.json":          &runManifest,
		"observations.json":      &runObservations,
		"destination-state.json": &independentState,
		"verdict.json":           &runVerdict,
	} {
		if err := readJSON(filepath.Join(runDirectory, path), target); err != nil {
			return 0, err
		}
	}
	if runManifest.Experiment != "external-effect-ambiguity" || runManifest.Destination != recipe.Destination || runManifest.Mode != mode {
		return 0, fmt.Errorf("manifest identity does not match recipe")
	}
	if !strings.Contains(runManifest.FailureBoundary, "SIGKILL after destination effect") || !strings.Contains(runManifest.FailureBoundary, "before Activity return") {
		return 0, fmt.Errorf("manifest does not declare the exact after-effect/before-return boundary")
	}
	if runManifest.EffectID == "" || runObservations.EffectID != runManifest.EffectID || runObservations.Destination != recipe.Destination || runObservations.Mode != mode {
		return 0, fmt.Errorf("stable effect identity does not cross manifest and observations")
	}
	if err := auditRawHistory(filepath.Join(runDirectory, "temporal-history.json"), runManifest, runObservations); err != nil {
		return 0, err
	}
	if len(runObservations.Attempts) != 2 || runObservations.Attempts[0].Attempt != 1 || runObservations.Attempts[1].Attempt != 2 {
		return 0, fmt.Errorf("attempt sequence is not exactly [1, 2]")
	}
	first, second := runObservations.Attempts[0], runObservations.Attempts[1]
	if first.StartedAt.IsZero() || first.EffectRespondedAt.IsZero() || runObservations.Kill.BarrierObservedAt.IsZero() ||
		runObservations.Kill.KilledAt.IsZero() || second.StartedAt.IsZero() {
		return 0, fmt.Errorf("exact boundary timestamps are incomplete")
	}
	if first.StartedAt.After(first.EffectRespondedAt) || first.EffectRespondedAt.After(runObservations.Kill.BarrierObservedAt) ||
		runObservations.Kill.BarrierObservedAt.After(runObservations.Kill.KilledAt) ||
		runObservations.Kill.KilledAt.After(second.StartedAt) {
		return 0, fmt.Errorf("exact boundary ordering is invalid")
	}
	if first.WorkerID == "" || second.WorkerID == "" || runObservations.Kill.WorkerID == "" || first.PID < 1 || second.PID < 1 || runObservations.Kill.PID < 1 {
		return 0, fmt.Errorf("boundary identity is incomplete")
	}
	if runObservations.Kill.WorkerID != first.WorkerID || runObservations.Kill.PID != first.PID || runObservations.Kill.Signal != "SIGKILL" {
		return 0, fmt.Errorf("kill does not target attempt 1's exact Worker process")
	}
	if !runObservations.History.RetryTimedOut || runObservations.History.CompletedCount != 1 || runObservations.History.CompletedAttempt != 2 {
		return 0, fmt.Errorf("history completed count/attempt or timeout oracle is invalid")
	}
	if runObservations.WorkflowOutcome != second.Receipt {
		return 0, fmt.Errorf("Workflow outcome does not equal attempt 2 receipt")
	}
	if !reflect.DeepEqual(runObservations.DestinationState, independentState) {
		return 0, fmt.Errorf("embedded and independently exported destination states differ")
	}
	if !runVerdict.RunValid || !runVerdict.ExpectedObservation || runVerdict.InvariantSatisfied != (mode == "protected") {
		return 0, fmt.Errorf("verdict does not match the unsafe/protected oracle")
	}
	wantEffects := 2
	if mode == "protected" {
		wantEffects = 1
	}
	if len(independentState.PhysicalEffects) != wantEffects {
		return 0, fmt.Errorf("independent physical-effect count = %d, want %d", len(independentState.PhysicalEffects), wantEffects)
	}
	for _, effect := range independentState.PhysicalEffects {
		if effect.LogicalID != runManifest.EffectID || effect.Kind != recipe.Destination || effect.PhysicalID == "" || effect.Receipt == "" {
			return 0, fmt.Errorf("physical effect does not bind the stable effect identity and receipt")
		}
	}
	if mode == "protected" {
		if first.Receipt != second.Receipt || second.Outcome != recipe.RetryOutcome || independentState.PhysicalEffects[0].Receipt != second.Receipt {
			return 0, fmt.Errorf("protected retry did not reuse the surviving receipt")
		}
	} else {
		if first.Receipt == second.Receipt {
			return 0, fmt.Errorf("unsafe control did not produce distinct receipts")
		}
		receipts := make(map[string]bool, len(independentState.PhysicalEffects))
		for _, effect := range independentState.PhysicalEffects {
			receipts[effect.Receipt] = true
		}
		if !receipts[first.Receipt] || !receipts[second.Receipt] {
			return 0, fmt.Errorf("independent destination state does not contain both unsafe attempt receipts")
		}
	}
	return len(independentState.PhysicalEffects), nil
}

func readJSON(path string, target any) error {
	data, err := readRegularFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func readRegularFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	if info.Size() > maxAuditFileBytes {
		return nil, fmt.Errorf("%s exceeds the %d-byte audit limit", path, maxAuditFileBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("%s changed type or identity while opening", path)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxAuditFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxAuditFileBytes {
		return nil, fmt.Errorf("%s exceeds the %d-byte audit limit", path, maxAuditFileBytes)
	}
	return data, nil
}

func requireEvidenceDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s is not a real evidence directory", path)
	}
	return nil
}

func confinedEvidenceName(name string) bool {
	return name != "" && filepath.Base(name) == name && filepath.VolumeName(name) == "" &&
		!strings.ContainsAny(name, `/\\`) && name != "." && name != ".."
}

func readConfinedRegularFile(rootDirectory, relative string) ([]byte, error) {
	if filepath.IsAbs(relative) || filepath.VolumeName(relative) != "" || strings.Contains(relative, `\`) {
		return nil, fmt.Errorf("artifact path %q is not portable and relative", relative)
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("artifact path %q escapes the evidence root", relative)
	}
	if err := requireEvidenceDirectory(rootDirectory); err != nil {
		return nil, err
	}
	prefix := rootDirectory
	parts := strings.Split(filepath.ToSlash(clean), "/")
	for index, part := range parts {
		prefix = filepath.Join(prefix, part)
		info, err := os.Lstat(prefix)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("artifact path %q contains a symlink", relative)
		}
		if index < len(parts)-1 && !info.IsDir() {
			return nil, fmt.Errorf("artifact path %q has a non-directory parent", relative)
		}
	}
	return readRegularFile(filepath.Join(rootDirectory, clean))
}

func auditRawHistory(path string, runManifest manifest, runObservations observations) error {
	var raw temporalHistory
	if err := readJSON(path, &raw); err != nil {
		return fmt.Errorf("raw Temporal history: %w", err)
	}
	input, err := workflowInputFromHistory(raw)
	if err != nil {
		return fmt.Errorf("raw Temporal history: %w", err)
	}
	if input.Destination != runManifest.Destination || input.Mode != runManifest.Mode || input.EffectID != runManifest.EffectID || input.Payload == "" {
		return fmt.Errorf("raw Temporal history input does not bind destination, mode, effect ID, and payload")
	}
	startedAttempts := make(map[string]int)
	completedCount := 0
	completedAttempt := 0
	retryTimedOut := false
	for _, event := range raw.Events {
		switch event.EventType {
		case "EVENT_TYPE_ACTIVITY_TASK_STARTED":
			attributes := event.ActivityTaskStartedEventAttributes
			startedAttempts[event.EventID] = attributes.Attempt
			if attributes.Attempt == 2 && attributes.LastFailure.TimeoutFailureInfo.TimeoutType == "TIMEOUT_TYPE_START_TO_CLOSE" {
				retryTimedOut = true
			}
		case "EVENT_TYPE_ACTIVITY_TASK_COMPLETED":
			completedCount++
			completedAttempt = startedAttempts[event.ActivityTaskCompletedEventAttributes.StartedEventID]
		}
	}
	if !retryTimedOut || completedCount != runObservations.History.CompletedCount || completedAttempt != runObservations.History.CompletedAttempt {
		return fmt.Errorf("raw Temporal history contradicts timeout/completion summary")
	}
	return nil
}

func readWorkflowInput(path string) (recordedWorkflowInput, error) {
	var raw temporalHistory
	if err := readJSON(path, &raw); err != nil {
		return recordedWorkflowInput{}, err
	}
	return workflowInputFromHistory(raw)
}

func workflowInputFromHistory(raw temporalHistory) (recordedWorkflowInput, error) {
	for _, event := range raw.Events {
		if event.EventType != "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED" {
			continue
		}
		payloads := event.WorkflowExecutionStartedEventAttributes.Input.Payloads
		if len(payloads) != 1 || payloads[0].Data == "" {
			return recordedWorkflowInput{}, errors.New("Workflow start input must contain one payload")
		}
		data, err := base64.StdEncoding.DecodeString(payloads[0].Data)
		if err != nil {
			return recordedWorkflowInput{}, fmt.Errorf("decode Workflow input payload: %w", err)
		}
		var input recordedWorkflowInput
		if err := json.Unmarshal(data, &input); err != nil {
			return recordedWorkflowInput{}, fmt.Errorf("decode Workflow input JSON: %w", err)
		}
		return input, nil
	}
	return recordedWorkflowInput{}, errors.New("Workflow start event is missing")
}

func requireRawEvidence(runDirectory string) error {
	if err := requireEvidenceDirectory(runDirectory); err != nil {
		return err
	}
	for _, relative := range []string{
		"manifest.json", "observations.json", "destination-state.json", "verdict.json",
		"temporal-history.json", "temporal-server.log", "workers/worker-1.log", "workers/worker-2.log",
	} {
		data, err := readConfinedRegularFile(runDirectory, relative)
		if err != nil {
			return fmt.Errorf("%s: required raw evidence %s: %w", filepath.Base(runDirectory), relative, err)
		}
		if len(data) == 0 {
			return fmt.Errorf("%s: required raw evidence %s is empty", filepath.Base(runDirectory), relative)
		}
	}
	return nil
}

func verifyArtifactFiles(runDirectory string) (int, error) {
	if err := requireEvidenceDirectory(runDirectory); err != nil {
		return 0, err
	}
	var state destinationState
	if err := readJSON(filepath.Join(runDirectory, "destination-state.json"), &state); err != nil {
		return 0, err
	}
	verified := 0
	for _, effect := range state.PhysicalEffects {
		if !confinedEvidenceName(effect.PhysicalID) {
			return 0, fmt.Errorf("%s: physical ID is not a confined filename", filepath.Base(runDirectory))
		}
		content, err := readConfinedRegularFile(runDirectory, filepath.Join("artifacts", "blobs", effect.PhysicalID))
		if err != nil {
			return 0, fmt.Errorf("%s: read preserved blob: %w", filepath.Base(runDirectory), err)
		}
		digest := sha256.Sum256(content)
		if !strings.HasSuffix(effect.PhysicalID, hex.EncodeToString(digest[:])+".blob") {
			return 0, fmt.Errorf("%s: blob name does not bind its content", filepath.Base(runDirectory))
		}
		verified++
	}
	if len(state.PhysicalEffects) == 1 {
		if !confinedEvidenceName(state.PhysicalEffects[0].LogicalID) {
			return 0, fmt.Errorf("%s: logical ID is not a confined filename", filepath.Base(runDirectory))
		}
		reference, err := readConfinedRegularFile(runDirectory, filepath.Join("artifacts", "refs", state.PhysicalEffects[0].LogicalID+".ref"))
		if err != nil {
			return 0, fmt.Errorf("%s: read preserved reference: %w", filepath.Base(runDirectory), err)
		}
		if strings.TrimSpace(string(reference)) != state.PhysicalEffects[0].Receipt {
			return 0, fmt.Errorf("%s: reference does not contain the preserved receipt", filepath.Base(runDirectory))
		}
		verified++
	}
	return verified, nil
}

func verifyGitBundle(runDirectory, payload string, state destinationState) error {
	bundle := filepath.Join(runDirectory, "destination.git.bundle")
	if _, err := readConfinedRegularFile(runDirectory, "destination.git.bundle"); err != nil {
		return fmt.Errorf("%s: validate Git bundle: %w", filepath.Base(runDirectory), err)
	}
	if output, err := runGitCommand(filepath.Dir(runDirectory), "bundle", "verify", bundle); err != nil {
		return fmt.Errorf("%s: verify Git bundle: %w: %s", filepath.Base(runDirectory), err, strings.TrimSpace(output))
	}
	temporaryDirectory, err := os.MkdirTemp("", "effect-safe-git-audit-")
	if err != nil {
		return fmt.Errorf("create Git audit directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	repositoryPath := filepath.Join(temporaryDirectory, "repository")
	if output, err := runGitCommand(temporaryDirectory, "clone", "--quiet", bundle, repositoryPath); err != nil {
		return fmt.Errorf("%s: clone Git bundle: %w: %s", filepath.Base(runDirectory), err, strings.TrimSpace(output))
	}
	protected := strings.Contains(filepath.Base(runDirectory), "-protected-")
	for _, effect := range state.PhysicalEffects {
		commit := strings.TrimPrefix(effect.Receipt, "git:")
		if commit == effect.Receipt || commit == "" {
			return fmt.Errorf("%s: Git receipt is malformed", filepath.Base(runDirectory))
		}
		if output, err := runGitCommand(repositoryPath, "merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
			return fmt.Errorf("%s: receipt commit is not reachable: %w: %s", filepath.Base(runDirectory), err, strings.TrimSpace(output))
		}
		marker := filepath.ToSlash(filepath.Join("effects", effect.LogicalID+".txt"))
		if !protected {
			if effect.Attempt < 1 {
				return fmt.Errorf("%s: unsafe Git effect has no attempt", filepath.Base(runDirectory))
			}
			marker = filepath.ToSlash(filepath.Join("effects", effect.LogicalID+"-attempt-"+strconv.Itoa(effect.Attempt)+".txt"))
		}
		content, err := runGitCommand(repositoryPath, "show", commit+":"+marker)
		if err != nil {
			return fmt.Errorf("%s: receipt commit lacks marker %s: %w: %s", filepath.Base(runDirectory), marker, err, strings.TrimSpace(content))
		}
		if content != payload {
			return fmt.Errorf("%s: receipt commit marker has conflicting content", filepath.Base(runDirectory))
		}
	}
	return nil
}

func runGitCommand(directory string, arguments ...string) (string, error) {
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	return string(output), err
}
