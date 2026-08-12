package lab

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func (t *codexTrial) publish() (string, error) {
	// Release every exact barrier before sealing the append-only raw inventory.
	// Cancellation may never reach the effect boundary, but cleanup must not add
	// a release marker after the evidence population has been inventoried.
	if err := t.releaseEffectBarrier(); err != nil {
		return "", err
	}
	history, err := exportWorkflowHistory(t.ctx, t.client, t.workflowID, t.workflowRun.GetRunID())
	if err != nil {
		return "", err
	}
	if err := replayWorkflowHistory(history); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(t.staging, "workflow-history.json"), history, 0o600); err != nil {
		return "", err
	}
	summary, err := t.collectSummary()
	if err != nil {
		return "", err
	}
	if err := writeJSONExclusive(filepath.Join(t.staging, "trial-summary.json"), summary); err != nil {
		return "", err
	}
	if _, err := writeRawInventory(t.staging); err != nil {
		return "", err
	}
	if err := os.Rename(t.staging, t.finalDirectory); err != nil {
		return "", err
	}
	if t.effectBarrier != nil {
		t.effectBarrier.directory = filepath.Join(t.finalDirectory, "effect-barrier")
	}
	return t.finalDirectory, nil
}

func (t *codexTrial) collectSummary() (trialSummary, error) {
	workspace, err := ReadWorkspaceEffects(t.workspacePath)
	if err != nil && !(t.boundary == FaultCancellationWhileExecuting && errors.Is(err, os.ErrNotExist)) {
		return trialSummary{}, err
	}
	after, err := hashWorkspace(t.fixture)
	if err != nil {
		return trialSummary{}, err
	}
	var destination DestinationSnapshot
	var authority *workstore.Snapshot
	if t.authorityStore == nil {
		destination, err = ReadDestination(t.ctx, t.destinationPath)
	} else {
		snapshot, snapshotErr := t.authorityStore.Snapshot(t.ctx, t.logicalSessionID)
		if snapshotErr != nil {
			return trialSummary{}, snapshotErr
		}
		authority = &snapshot
	}
	if err != nil {
		return trialSummary{}, err
	}
	attempts, err := collectTrialAttempts(t.attemptRoot, t.boundary, t.options.EffectBinary)
	if err != nil {
		return trialSummary{}, err
	}
	effectCount := len(destination.Attempts)
	if authority != nil {
		effectCount = len(authority.Effects)
	}
	verdict := trialVerdict{Admitted: true, SafetyPassed: effectCount == 1 && len(workspace) == 1}
	if t.boundary == FaultCancellationWhileExecuting {
		verdict.SafetyPassed = authority != nil && authority.Cancellation != nil &&
			authority.Outcome == nil && effectCount == 0 && len(workspace) == 0
	}
	if (t.boundary == FaultAfterToolEffect || t.boundary == FaultAfterFinalOutput) &&
		t.options.RecoveryMode.normalized() != RecoveryModeFenced {
		verdict = classifyPostExecutionControl(effectCount, len(workspace), len(attempts))
	}
	if !verdict.SafetyPassed && !verdict.NegativeControlTriggered {
		verdict.Admitted = false
	}
	if t.boundary != FaultCancellationWhileExecuting &&
		(t.result.Result != "EFFECT_COMPLETE" || !validThreadID(t.result.ThreadID)) {
		verdict.Admitted = false
	}
	return trialSummary{
		SchemaVersion: "codex-direct-trial-v1", Mode: t.options.RecoveryMode.normalized(),
		FaultBoundary: t.boundary, Trial: t.trial, LogicalSessionID: t.logicalSessionID,
		LogicalTurnID: "turn-1", LogicalEffectID: "effect-1", WorkflowID: t.workflowID,
		WorkflowRunID: t.workflowRun.GetRunID(), StartedAt: t.startedAt, FaultAt: t.faultAt,
		CompletedAt: time.Now().UTC(), BarrierArrivals: t.arrivals, WorkflowResult: t.result,
		WorkspaceBeforeHash: t.workspaceBefore, WorkspaceAfterHash: after, WorkspaceEffects: workspace,
		Destination: destination, Authority: authority, Attempts: attempts, ReplayVerified: true,
		Metadata: t.metadata, Verdict: verdict,
	}, nil
}

func classifyPostExecutionControl(effectCount, workspaceCount, attemptCount int) trialVerdict {
	if attemptCount != 2 || effectCount < 1 || effectCount > 2 || workspaceCount != effectCount {
		return trialVerdict{}
	}
	verdict := trialVerdict{
		Admitted:                 true,
		NegativeControlTriggered: true,
		ReasonCodes:              []string{"competing_execution"},
	}
	if effectCount == 2 {
		verdict.ReasonCodes = append(verdict.ReasonCodes, "duplicate_effect")
	}
	return verdict
}

func collectTrialAttempts(root string, boundary FaultBoundary, effectBinary string) ([]trialAttemptEvidence, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var attempts []trialAttemptEvidence
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "threads" {
			continue
		}
		directory := filepath.Join(root, entry.Name())
		attempt := trialAttemptEvidence{PhysicalAttemptID: entry.Name()}
		if process, readErr := readOptionalJSON[ProcessRecord](filepath.Join(directory, entry.Name()+".process-started.json")); readErr == nil {
			attempt.Process = &process
		}
		if thread, readErr := ReadThreadReceipt(filepath.Join(directory, threadReceiptFile)); readErr == nil {
			attempt.Thread = &thread
		}
		if request, readErr := ReadControlledEffectRequest(filepath.Join(directory, effectRequestFile)); readErr == nil {
			digest := ""
			if request.OwnerCapability != "" {
				digest = workstore.HashToken(request.OwnerCapability)
				request.OwnerCapability = ""
			}
			attempt.Request = &trialEffectRequestEvidence{
				ControlledEffectInput: request, OwnerCapabilitySHA256: digest,
			}
		}
		if file, openErr := os.Open(filepath.Join(directory, entry.Name()+".stdout.jsonl")); openErr == nil {
			if attempt.Request == nil {
				_ = file.Close()
				return nil, fmt.Errorf("attempt %s has a stream without an effect request", entry.Name())
			}
			expectedCommand, commandErr := expectedAttemptCommand(root, entry.Name(),
				attempt.Request.ControlledEffectInput, effectBinary)
			if commandErr != nil {
				_ = file.Close()
				return nil, commandErr
			}
			stream, parseErr := ParseCodexStream(file, StreamHooks{ExpectedCommand: expectedCommand})
			_ = file.Close()
			if parseErr == nil {
				attempt.StreamComplete, attempt.ThreadID = true, stream.ThreadID
			} else if !errors.Is(parseErr, errCodexStreamIncomplete) {
				return nil, fmt.Errorf("attempt %s contains invalid Codex stream evidence: %w", entry.Name(), parseErr)
			}
		}
		threadRequired := attempt.Request != nil && attempt.Request.ThreadReceiptPath != "" &&
			boundary != FaultProcessFailureReplacement
		if attempt.Process == nil || attempt.Request == nil || (threadRequired && attempt.Thread == nil) {
			return nil, fmt.Errorf("attempt %s lacks identity evidence", entry.Name())
		}
		attempts = append(attempts, attempt)
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].PhysicalAttemptID < attempts[j].PhysicalAttemptID })
	return attempts, nil
}

func expectedAttemptCommand(attemptRoot, attemptID string, request ControlledEffectInput,
	effectBinary string,
) (string, error) {
	if !safeCommandPath(effectBinary) || attemptID == "" || attemptID != filepath.Base(attemptID) {
		return "", errors.New("attempt command lacks a safe effect or attempt identity")
	}
	fixtureDirectory := filepath.Dir(request.WorkspacePath)
	if filepath.Base(fixtureDirectory) != "fixture" {
		return "", errors.New("attempt workspace does not identify its original fixture")
	}
	originalRunRoot := filepath.Dir(fixtureDirectory)
	currentRunName := filepath.Base(filepath.Dir(attemptRoot))
	wantOriginalName := currentRunName
	if !strings.HasPrefix(wantOriginalName, ".staging-") {
		wantOriginalName = ".staging-" + wantOriginalName
	}
	if filepath.Base(originalRunRoot) != wantOriginalName {
		return "", errors.New("attempt request does not identify the sealed run staging root")
	}
	requestPath := filepath.Join(originalRunRoot, "attempts", attemptID, effectRequestFile)
	return effectBinary + " --request " + requestPath, nil
}

func readOptionalJSON[T any](path string) (T, error) {
	var value T
	data, err := os.ReadFile(path)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return value, err
	}
	return value, nil
}
