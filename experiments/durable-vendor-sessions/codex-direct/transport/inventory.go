package transport

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

func inventoryTree(ctx context.Context, root string) ([]Artifact, int64, error) {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, 0, fmt.Errorf("%w: source bundle is not a real directory", ErrInvalidTransport)
	}
	var artifacts []Artifact
	var total int64
	err = filepath.WalkDir(root, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if filePath == root || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&fs.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maxArtifactBytes {
			return fmt.Errorf("%w: source artifact %q is not a bounded regular file", ErrInvalidTransport, filePath)
		}
		relative, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !safeRelativePath(relative) {
			return fmt.Errorf("%w: unsafe source path %q", ErrInvalidTransport, relative)
		}
		digest, err := hashFile(filePath, maxArtifactBytes)
		if err != nil {
			return err
		}
		total += info.Size()
		if total < 0 || total > maxArchiveBytes {
			return fmt.Errorf("%w: bundle exceeds archive size bound", ErrInvalidTransport)
		}
		artifacts = append(artifacts, Artifact{
			Path: relative, Size: info.Size(), Mode: info.Mode().Perm(), SHA256: digest,
		})
		return nil
	})
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Path < artifacts[j].Path })
	return artifacts, total, err
}

func bindRuns(root string, files []Artifact) ([]RunBinding, error) {
	byPath := make(map[string]Artifact, len(files))
	for _, artifact := range files {
		byPath[artifact.Path] = artifact
	}
	suite, err := readJSON[suiteSummary](filepath.Join(root, "suite-summary.json"))
	if err != nil || len(suite.RunDirectories) == 0 {
		return nil, fmt.Errorf("%w: suite summary is absent or empty", ErrInvalidTransport)
	}
	seen := make(map[string]bool, len(suite.RunDirectories))
	runs := make([]RunBinding, 0, len(suite.RunDirectories))
	for _, recorded := range suite.RunDirectories {
		runID := path.Base(filepath.ToSlash(filepath.Clean(recorded)))
		if !safeBaseName(runID) || seen[runID] {
			return nil, fmt.Errorf("%w: invalid or duplicate suite run %q", ErrInvalidTransport, recorded)
		}
		seen[runID] = true
		binding, err := bindRun(root, runID, byPath)
		if err != nil {
			return nil, err
		}
		runs = append(runs, binding)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].RunID < runs[j].RunID })
	return runs, nil
}

func bindRun(root, runID string, byPath map[string]Artifact) (RunBinding, error) {
	prefix := runID + "/"
	inventoryPath := prefix + "raw-inventory.json"
	summaryPath := prefix + "trial-summary.json"
	historyPath := prefix + "workflow-history.json"
	inventoryArtifact, inventoryOK := byPath[inventoryPath]
	summaryArtifact, summaryOK := byPath[summaryPath]
	historyArtifact, historyOK := byPath[historyPath]
	if !inventoryOK || !summaryOK || !historyOK {
		return RunBinding{}, fmt.Errorf("%w: finalized run files are incomplete for %s", ErrInvalidTransport, runID)
	}
	inventory, err := readJSON[rawInventory](filepath.Join(root, filepath.FromSlash(inventoryPath)))
	if err != nil {
		return RunBinding{}, err
	}
	if inventory.Version != RawInventoryVersion || len(inventory.Files) == 0 || !validRawArtifacts(inventory.Files) {
		return RunBinding{}, fmt.Errorf("%w: raw inventory is invalid for %s", ErrInvalidTransport, runID)
	}
	var actual []rawArtifact
	for relative, artifact := range byPath {
		if strings.HasPrefix(relative, prefix) && relative != inventoryPath {
			actual = append(actual, rawArtifact{
				Path: strings.TrimPrefix(relative, prefix), Size: artifact.Size, SHA256: artifact.SHA256,
			})
		}
	}
	sort.Slice(actual, func(i, j int) bool { return actual[i].Path < actual[j].Path })
	if !slices.Equal(inventory.Files, actual) {
		return RunBinding{}, fmt.Errorf("%w: raw inventory differs from archive files for %s", ErrInvalidTransport, runID)
	}
	summary, err := readJSONProjection[trialSummary](filepath.Join(root, filepath.FromSlash(summaryPath)))
	if err != nil || summary.SchemaVersion != "codex-direct-trial-v1" ||
		summary.LogicalSessionID != runID || summary.Trial < 1 || summary.Mode == "" || summary.FaultBoundary == "" {
		return RunBinding{}, fmt.Errorf("%w: trial summary identity differs for %s", ErrInvalidTransport, runID)
	}
	return RunBinding{
		RunID: runID, RawInventoryPath: inventoryPath, RawInventorySHA256: inventoryArtifact.SHA256,
		SummaryPath: summaryPath, SummarySHA256: summaryArtifact.SHA256,
		HistoryPath: historyPath, HistorySHA256: historyArtifact.SHA256,
	}, nil
}

func validRawArtifacts(files []rawArtifact) bool {
	previous := ""
	for _, artifact := range files {
		if !safeRelativePath(artifact.Path) || artifact.Path <= previous || artifact.Size < 0 || !validSHA256(artifact.SHA256) {
			return false
		}
		previous = artifact.Path
	}
	return true
}

func validateAudit(path string, wantRuns int) (auditReport, string, error) {
	report, err := readJSONProjection[auditReport](path)
	if err != nil {
		return auditReport{}, "", err
	}
	if report.Version != "codex-direct-disk-audit-v1" || report.Runs != wantRuns || !report.AllRequirementsVerified {
		return auditReport{}, "", fmt.Errorf("%w: audit report is absent or did not verify the exact population", ErrInvalidTransport)
	}
	hash, err := hashFile(path, maxJSONBytes)
	return report, hash, err
}

func validateRejectedBundle(root string, files []Artifact) (string, error) {
	var failureHash string
	for _, artifact := range files {
		if artifact.Path == "failure.json" {
			failureHash = artifact.SHA256
			break
		}
	}
	if failureHash == "" {
		return "", fmt.Errorf("%w: rejected bundle lacks failure.json", ErrInvalidTransport)
	}
	failure, err := readJSON[failureRecord](filepath.Join(root, "failure.json"))
	if err != nil {
		return "", err
	}
	if _, err := time.Parse(time.RFC3339Nano, failure.RecordedAt); err != nil || failure.Error == "" || !failure.Preserved {
		return "", fmt.Errorf("%w: rejected bundle has an invalid failure record", ErrInvalidTransport)
	}
	return failureHash, nil
}

func validateRejectionAudit(path, wantFailureHash string) (rejectionAuditReport, string, error) {
	report, err := readJSON[rejectionAuditReport](path)
	if err != nil {
		return rejectionAuditReport{}, "", err
	}
	if report.Version != rejectionAuditVersion || report.EvidenceRoot == "" || !report.FailurePreserved ||
		report.FailureSHA256 != wantFailureHash || !validSHA256(report.FailureSHA256) {
		return rejectionAuditReport{}, "", fmt.Errorf("%w: rejection audit does not bind the preserved failure", ErrInvalidTransport)
	}
	hash, err := hashFile(path, maxJSONBytes)
	return report, hash, err
}

func validLineage(lineage Lineage) error {
	if lineage.SchemaVersion != LineageVersion || len(lineage.Entries) == 0 {
		return fmt.Errorf("%w: lineage is absent or unsupported", ErrInvalidTransport)
	}
	seen := make(map[string]bool, len(lineage.Entries))
	for index, entry := range lineage.Entries {
		if !safeBaseName(entry.Bundle) || !safeBaseName(entry.Audit) || entry.Reason == "" || seen[entry.Bundle] {
			return fmt.Errorf("%w: invalid lineage entry", ErrInvalidTransport)
		}
		seen[entry.Bundle] = true
		last := index == len(lineage.Entries)-1
		if last {
			if entry.Disposition != DispositionAdmitted || entry.SupersededBy != "" {
				return fmt.Errorf("%w: final lineage entry must be admitted", ErrInvalidTransport)
			}
			continue
		}
		switch entry.Disposition {
		case DispositionRejected:
			if entry.SupersededBy != "" {
				return fmt.Errorf("%w: rejected entry cannot name a superseding bundle", ErrInvalidTransport)
			}
		case DispositionSuperseded:
			if entry.SupersededBy != lineage.Entries[index+1].Bundle {
				return fmt.Errorf("%w: superseded entry does not name the next bundle", ErrInvalidTransport)
			}
		default:
			return fmt.Errorf("%w: invalid lineage disposition", ErrInvalidTransport)
		}
	}
	return nil
}

func validateSourceEntries(root string, lineage []LineageEntry) error {
	want := make(map[string]bool, len(lineage)*2)
	for _, entry := range lineage {
		_, bundleExists := want[entry.Bundle]
		_, auditExists := want[entry.Audit]
		if bundleExists || auditExists || entry.Bundle == entry.Audit {
			return fmt.Errorf("%w: source entry identity is duplicated", ErrInvalidTransport)
		}
		want[entry.Bundle] = true
		want[entry.Audit] = false
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	if len(entries) != len(want) {
		return fmt.Errorf("%w: source file set differs from lineage", ErrInvalidTransport)
	}
	for _, entry := range entries {
		wantDirectory, ok := want[entry.Name()]
		if !ok || entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%w: unexpected or symlinked source entry %q", ErrInvalidTransport, entry.Name())
		}
		info, err := entry.Info()
		if err != nil || wantDirectory != info.IsDir() || !wantDirectory && !info.Mode().IsRegular() {
			return fmt.Errorf("%w: source entry has the wrong type %q", ErrInvalidTransport, entry.Name())
		}
	}
	return nil
}
