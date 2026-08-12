package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRecipesDeclareExactlySixDestinationContracts(t *testing.T) {
	t.Parallel()
	want := []Destination{
		"idempotent-api",
		"non-idempotent-api",
		"database",
		"git",
		"message",
		"artifact",
	}
	got := make([]Destination, 0, len(recipes()))
	for _, recipe := range recipes() {
		got = append(got, recipe.Destination)
		for field, value := range map[string]string{
			"mechanism":     recipe.Mechanism,
			"atomicity":     recipe.Atomicity,
			"lookup":        recipe.Lookup,
			"serialization": recipe.Serialization,
			"retention":     recipe.Retention,
			"conflict":      recipe.Conflict,
			"artifacts":     recipe.Artifacts,
			"limits":        recipe.Limits,
		} {
			if strings.TrimSpace(value) == "" {
				t.Errorf("%s has no %s contract", recipe.Destination, field)
			}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("destinations = %v, want %v", got, want)
	}
}

func TestAuditFinalEvidence(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	report, err := auditFinalEvidence(repositoryRoot)
	if err != nil {
		t.Fatalf("audit final evidence: %v", err)
	}
	if report.Runs != 36 || report.UnsafePhysicalEffects != 36 || report.ProtectedPhysicalEffects != 18 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.VerifiedGitBundles != 6 || report.VerifiedArtifactFiles != 12 {
		t.Fatalf("portable artifact counts: %+v", report)
	}
}

func TestAuditRejectsEvidenceMutation(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	source := filepath.Join(repositoryRoot, "experiments", "external-effects", "evidence")
	copyRoot := t.TempDir()
	copyEvidenceForAudit(t, source, copyRoot)

	path := filepath.Join(copyRoot, "external-effects-20260806-v1-idempotent-api-protected-trial-1", "observations.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"completed_count": 1`), []byte(`"completed_count": 2`), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := auditEvidenceRoot(copyRoot); err == nil || !strings.Contains(err.Error(), "raw Temporal history") {
		t.Fatalf("mutation error = %v", err)
	}
}

func TestAuditRejectsRawHistoryContradiction(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	source := filepath.Join(repositoryRoot, "experiments", "external-effects", "evidence")
	copyRoot := t.TempDir()
	copyEvidenceForAudit(t, source, copyRoot)

	path := filepath.Join(copyRoot, "external-effects-20260806-v1-idempotent-api-protected-trial-1", "temporal-history.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte("EVENT_TYPE_ACTIVITY_TASK_COMPLETED"), []byte("EVENT_TYPE_ACTIVITY_TASK_FAILED"), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := auditEvidenceRoot(copyRoot); err == nil || !strings.Contains(err.Error(), "raw Temporal history") {
		t.Fatalf("history mutation error = %v", err)
	}
}

func TestAuditRejectsMissingBoundaryIdentity(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	source := filepath.Join(repositoryRoot, "experiments", "external-effects", "evidence")
	copyRoot := t.TempDir()
	copyEvidenceForAudit(t, source, copyRoot)

	path := filepath.Join(copyRoot, "external-effects-20260806-v1-idempotent-api-protected-trial-1", "observations.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"worker_id": "worker-1"`), []byte(`"worker_id": ""`), 2)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := auditEvidenceRoot(copyRoot); err == nil || !strings.Contains(err.Error(), "boundary identity") {
		t.Fatalf("boundary mutation error = %v", err)
	}
}

func TestGitBundleMustContainClaimedReceiptAndContent(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	runDirectory := filepath.Join(repositoryRoot, "experiments", "external-effects", "evidence",
		"external-effects-20260806-v2-git-protected-trial-1")
	var state destinationState
	if err := readJSON(filepath.Join(runDirectory, "destination-state.json"), &state); err != nil {
		t.Fatal(err)
	}
	if err := verifyGitBundle(runDirectory, "agent-output-v1", state); err != nil {
		t.Fatalf("verify cited Git bundle: %v", err)
	}
	if err := verifyGitBundle(runDirectory, "conflicting-content", state); err == nil {
		t.Fatal("Git bundle accepted conflicting content")
	}
}

func TestVerifyArtifactFilesRejectsTraversal(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	runDirectory := filepath.Join(parent, "run")
	if err := os.MkdirAll(filepath.Join(runDirectory, "artifacts", "blobs"), 0o750); err != nil {
		t.Fatal(err)
	}
	content := []byte("outside evidence boundary")
	digest := sha256.Sum256(content)
	physicalID := "../../../" + hex.EncodeToString(digest[:]) + ".blob"
	for index := 0; index < 2; index++ {
		if err := os.WriteFile(filepath.Join(parent, hex.EncodeToString(digest[:])+".blob"), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	state := destinationState{PhysicalEffects: []physicalEffect{
		{PhysicalID: physicalID, LogicalID: "effect-1"},
		{PhysicalID: physicalID, LogicalID: "effect-2"},
	}}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDirectory, "destination-state.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyArtifactFiles(runDirectory); err == nil {
		t.Fatal("artifact traversal outside the run directory succeeded")
	}
}

func TestRequireRawEvidenceRejectsSymlinks(t *testing.T) {
	t.Parallel()
	runDirectory := t.TempDir()
	target := filepath.Join(t.TempDir(), "outside-evidence")
	if err := os.WriteFile(target, []byte("not preserved in the run"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{
		"manifest.json", "observations.json", "destination-state.json", "verdict.json",
		"temporal-history.json", "temporal-server.log", "workers/worker-1.log", "workers/worker-2.log",
	} {
		path := filepath.Join(runDirectory, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
	}
	if err := requireRawEvidence(runDirectory); err == nil {
		t.Fatal("symlinked raw evidence succeeded")
	}
}

func TestConfinedEvidenceReaderRejectsUnsafePathsAndTypes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "regular.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if data, err := readConfinedRegularFile(root, "regular.json"); err != nil || string(data) != "{}" {
		t.Fatalf("read regular evidence = %q, %v", data, err)
	}
	for _, relative := range []string{"", ".", "../outside", "/absolute", `C:\outside`, `nested\outside`} {
		if _, err := readConfinedRegularFile(root, relative); err == nil {
			t.Errorf("unsafe path %q succeeded", relative)
		}
	}
	if _, err := readRegularFile(root); err == nil {
		t.Fatal("directory accepted as a regular file")
	}
	if err := os.WriteFile(filepath.Join(root, "not-a-directory"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfinedRegularFile(root, filepath.Join("not-a-directory", "child")); err == nil {
		t.Fatal("non-directory artifact parent succeeded")
	}
	if err := requireEvidenceDirectory(filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing evidence root succeeded")
	}
	outside := t.TempDir()
	linkedRoot := filepath.Join(t.TempDir(), "linked-root")
	if err := os.Symlink(outside, linkedRoot); err != nil {
		t.Fatal(err)
	}
	if err := requireEvidenceDirectory(linkedRoot); err == nil {
		t.Fatal("symlinked evidence root succeeded")
	}
	oversized := filepath.Join(root, "oversized")
	file, err := os.Create(oversized)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxAuditFileBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegularFile(oversized); err == nil {
		t.Fatal("oversized evidence file succeeded")
	}
}

func TestGitContentReadPreservesConflictingWhitespace(t *testing.T) {
	t.Parallel()
	repository := t.TempDir()
	for _, arguments := range [][]string{
		{"init", "--quiet"},
		{"config", "user.name", "Cookbook Test"},
		{"config", "user.email", "cookbook@example.invalid"},
	} {
		if output, err := runGitCommand(repository, arguments...); err != nil {
			t.Fatalf("git %v: %v: %s", arguments, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repository, "marker.txt"), []byte("payload\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, arguments := range [][]string{{"add", "marker.txt"}, {"commit", "--quiet", "-m", "marker"}} {
		if output, err := runGitCommand(repository, arguments...); err != nil {
			t.Fatalf("git %v: %v: %s", arguments, err, output)
		}
	}
	content, err := runGitCommand(repository, "show", "HEAD:marker.txt")
	if err != nil {
		t.Fatal(err)
	}
	if content != "payload\n" {
		t.Fatalf("Git content = %q; exact trailing newline was lost", content)
	}
}

func TestWorkflowInputRejectsMalformedRawHistory(t *testing.T) {
	t.Parallel()
	if _, err := workflowInputFromHistory(temporalHistory{}); err == nil {
		t.Fatal("missing Workflow start succeeded")
	}
	history := temporalHistory{Events: []historyEvent{{EventType: "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED"}}}
	if _, err := workflowInputFromHistory(history); err == nil {
		t.Fatal("missing Workflow payload succeeded")
	}
	history.Events[0].WorkflowExecutionStartedEventAttributes.Input.Payloads = append(
		history.Events[0].WorkflowExecutionStartedEventAttributes.Input.Payloads,
		struct {
			Data string `json:"data"`
		}{Data: "not-base64"},
	)
	if _, err := workflowInputFromHistory(history); err == nil {
		t.Fatal("invalid base64 Workflow payload succeeded")
	}
	history.Events[0].WorkflowExecutionStartedEventAttributes.Input.Payloads[0].Data = "bm90LWpzb24="
	if _, err := workflowInputFromHistory(history); err == nil {
		t.Fatal("invalid JSON Workflow payload succeeded")
	}
}

func TestRunRecipeBuildsWorkerAndUsesCallerEvidenceRoot(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	outputRoot := filepath.Join(t.TempDir(), "fresh-evidence")
	var calls [][]string
	runner := func(_ context.Context, directory, name string, arguments ...string) error {
		calls = append(calls, append([]string{directory, name}, arguments...))
		if name == "go" && len(arguments) > 2 && arguments[0] == "build" {
			workerPath := arguments[2]
			return os.WriteFile(workerPath, []byte("worker"), 0o700)
		}
		return nil
	}
	err := runRecipe(context.Background(), runner, runOptions{
		RepositoryRoot: repositoryRoot,
		Destination:    "git",
		OutputRoot:     outputRoot,
		RunID:          "cookbook-test",
		Trials:         3,
	})
	if err != nil {
		t.Fatalf("run recipe: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("calls = %v", calls)
	}
	joined := strings.Join(calls[1], " ")
	for _, required := range []string{"./experiments/external-effects/cmd/experiment", "--destination git", "--mode all", "--trials 3", "--output " + outputRoot} {
		if !strings.Contains(joined, required) {
			t.Errorf("experiment invocation %q lacks %q", joined, required)
		}
	}
}

func TestRunRecipePropagatesBuildAndExperimentErrors(t *testing.T) {
	t.Parallel()
	repositoryRoot := findRepositoryRoot(t)
	options := runOptions{
		RepositoryRoot: repositoryRoot,
		Destination:    "artifact",
		OutputRoot:     t.TempDir(),
		RunID:          "cookbook-errors",
		Trials:         1,
		TemporalPath:   "/opt/temporal",
	}
	buildFailure := func(context.Context, string, string, ...string) error { return os.ErrPermission }
	if err := runRecipe(context.Background(), buildFailure, options); err == nil || !strings.Contains(err.Error(), "build") {
		t.Fatalf("build failure = %v", err)
	}
	calls := 0
	experimentFailure := func(_ context.Context, _ string, _ string, arguments ...string) error {
		calls++
		if calls == 1 {
			return os.WriteFile(arguments[2], []byte("worker"), 0o700)
		}
		if !strings.Contains(strings.Join(arguments, " "), "--temporal /opt/temporal") {
			t.Fatalf("Temporal override missing from %v", arguments)
		}
		return os.ErrInvalid
	}
	if err := runRecipe(context.Background(), experimentFailure, options); err == nil || !strings.Contains(err.Error(), "experiment") {
		t.Fatalf("experiment failure = %v", err)
	}
}

func TestRunRecipeRejectsInvalidOrPreservedEvidenceTargets(t *testing.T) {
	t.Parallel()
	repositoryRoot := findRepositoryRoot(t)
	nop := func(context.Context, string, string, ...string) error { return nil }
	for name, options := range map[string]runOptions{
		"unknown destination": {RepositoryRoot: repositoryRoot, Destination: "queue", OutputRoot: t.TempDir(), RunID: "x", Trials: 1},
		"missing run ID":      {RepositoryRoot: repositoryRoot, Destination: "message", OutputRoot: t.TempDir(), Trials: 1},
		"missing output":      {RepositoryRoot: repositoryRoot, Destination: "message", RunID: "x", Trials: 1},
		"non-positive trials": {RepositoryRoot: repositoryRoot, Destination: "message", OutputRoot: t.TempDir(), RunID: "x"},
		"preserved evidence": {RepositoryRoot: repositoryRoot, Destination: "message", OutputRoot: filepath.Join(repositoryRoot,
			"experiments", "external-effects", "evidence", "new"), RunID: "x", Trials: 1},
	} {
		options := options
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := runRecipe(context.Background(), nop, options); err == nil {
				t.Fatal("invalid recipe run succeeded")
			}
		})
	}
}

func TestRunRecipeRejectsSymlinkIntoPreservedEvidence(t *testing.T) {
	t.Parallel()
	repositoryRoot := findRepositoryRoot(t)
	link := filepath.Join(t.TempDir(), "evidence-link")
	preserved := filepath.Join(repositoryRoot, "experiments", "external-effects", "evidence")
	if err := os.Symlink(preserved, link); err != nil {
		t.Fatal(err)
	}
	options := runOptions{
		RepositoryRoot: repositoryRoot,
		Destination:    "message",
		OutputRoot:     filepath.Join(link, "new"),
		RunID:          "symlink-test",
		Trials:         1,
	}
	nop := func(context.Context, string, string, ...string) error { return nil }
	if err := runRecipe(context.Background(), nop, options); err == nil {
		t.Fatal("symlink into preserved evidence succeeded")
	}
}

func TestRunCLI(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	var stdout bytes.Buffer
	if err := runCLI(context.Background(), []string{"audit", "--repo", repositoryRoot}, &stdout, &stdout, nil); err != nil {
		t.Fatalf("audit CLI: %v", err)
	}
	if !strings.Contains(stdout.String(), "36 runs") {
		t.Fatalf("audit output = %q", stdout.String())
	}
	stdout.Reset()
	if err := runCLI(context.Background(), []string{"list"}, &stdout, &stdout, nil); err != nil {
		t.Fatalf("list CLI: %v", err)
	}
	if strings.Count(stdout.String(), "\n") != 6 {
		t.Fatalf("list output = %q", stdout.String())
	}
	if err := runCLI(context.Background(), nil, &stdout, &stdout, nil); err == nil {
		t.Fatal("empty command succeeded")
	}
	if err := runCLI(context.Background(), []string{"unknown"}, &stdout, &stdout, nil); err == nil {
		t.Fatal("unknown command succeeded")
	}
	if err := runCLI(context.Background(), []string{"list", "extra"}, &stdout, &stdout, nil); err == nil {
		t.Fatal("list arguments succeeded")
	}
	if err := runCLI(context.Background(), []string{"audit", "--bad-flag"}, &stdout, &stdout, nil); err == nil {
		t.Fatal("invalid audit flag succeeded")
	}

	stdout.Reset()
	outputRoot := filepath.Join(t.TempDir(), "cli-evidence")
	calls := 0
	runner := func(_ context.Context, _ string, _ string, arguments ...string) error {
		calls++
		if calls == 1 {
			return os.WriteFile(arguments[2], []byte("worker"), 0o700)
		}
		return nil
	}
	if err := runCLI(context.Background(), []string{
		"run", "--repo", repositoryRoot, "--destination", "database", "--output", outputRoot,
		"--run-id", "cli-test", "--trials", "1",
	}, &stdout, &stdout, runner); err != nil {
		t.Fatalf("run CLI: %v", err)
	}
	if calls != 2 {
		t.Fatalf("run CLI calls = %d", calls)
	}
}

func TestRepositoryRootAndCommandExecution(t *testing.T) {
	repositoryRoot := findRepositoryRoot(t)
	explicit, err := resolveRepositoryRoot(repositoryRoot)
	if err != nil || explicit != repositoryRoot {
		t.Fatalf("explicit root = %q, %v", explicit, err)
	}
	automatic, err := resolveRepositoryRoot("")
	if err != nil || automatic != repositoryRoot {
		t.Fatalf("automatic root = %q, %v", automatic, err)
	}
	if err := executeCommand(context.Background(), repositoryRoot, "go", "version"); err != nil {
		t.Fatalf("execute command: %v", err)
	}
}

func findRepositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository root not found")
		}
		directory = parent
	}
}

func copyEvidenceForAudit(t *testing.T, source, destination string) {
	t.Helper()
	for _, recipe := range recipes() {
		for _, mode := range []string{"unsafe", "protected"} {
			for trial := 1; trial <= 3; trial++ {
				name := evidenceRunName(recipe, mode, trial)
				from := filepath.Join(source, name)
				to := filepath.Join(destination, name)
				if err := os.MkdirAll(to, 0o750); err != nil {
					t.Fatal(err)
				}
				for _, file := range []string{"manifest.json", "observations.json", "destination-state.json", "verdict.json", "temporal-history.json"} {
					data, err := os.ReadFile(filepath.Join(from, file))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(to, file), data, 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}
