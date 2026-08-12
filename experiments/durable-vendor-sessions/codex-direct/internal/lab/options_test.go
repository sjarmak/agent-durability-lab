package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInspectAndReverifyHermeticExperimentInputs(t *testing.T) {
	root := t.TempDir()
	tool := filepath.Join(root, "codex")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\nprintf 'codex-test 1.0\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	schema := filepath.Join(root, "schema.json")
	if err := os.WriteFile(schema, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := ExperimentOptions{
		WorkerBinary: tool, EffectBinary: tool, LauncherBinary: tool,
		CodexBinary: tool, CodexWrapper: tool, CodexHome: root, OutputSchema: schema,
		Model: "gpt-5.6-sol", ReasoningEffort: "low", Hermetic: true,
	}
	metadata, err := inspectExperimentInputs(context.Background(), options)
	if err != nil {
		t.Fatalf("inspect hermetic inputs: %v", err)
	}
	if metadata.CodexVersion != "codex-test 1.0" || metadata.Authentication != "not-applicable-hermetic" ||
		metadata.InvocationPath != "pinned-underlying-cli-with-codex-2-profile" {
		t.Fatalf("metadata = %+v", metadata)
	}
	if err := verifyExperimentInputsUnchanged(options, metadata); err != nil {
		t.Fatalf("reverify unchanged inputs: %v", err)
	}
	metadata.SchemaSHA256 = metadata.CodexBinarySHA256
	if err := verifyExperimentInputsUnchanged(options, metadata); err == nil {
		t.Fatal("changed input digest was accepted")
	}
}

func TestInspectExperimentInputsRejectsFailedAndEmptyVersions(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name, script string
	}{
		{name: "failed", script: "#!/bin/sh\nexit 7\n"},
		{name: "empty", script: "#!/bin/sh\nexit 0\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tool := filepath.Join(root, test.name)
			if err := os.WriteFile(tool, []byte(test.script), 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := inspectExperimentInputs(context.Background(), ExperimentOptions{
				CodexBinary: tool, Hermetic: true,
			}); err == nil {
				t.Fatal("invalid Codex version response was accepted")
			}
		})
	}
}

func TestInspectAuthenticatedExperimentInputsChecksBothPinnedEntrypoints(t *testing.T) {
	root := t.TempDir()
	tool := filepath.Join(root, "codex")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then printf 'codex-test 1.0\\n'; exit 0; fi\n" +
		"if [ \"$1\" = \"login\" ] && [ \"$2\" = \"status\" ]; then printf 'Logged in using ChatGPT\\n'; exit 0; fi\n" +
		"exit 9\n"
	if err := os.WriteFile(tool, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	schema := filepath.Join(root, "schema.json")
	if err := os.WriteFile(schema, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	metadata, err := inspectExperimentInputs(context.Background(), ExperimentOptions{
		WorkerBinary: tool, EffectBinary: tool, LauncherBinary: tool,
		CodexBinary: tool, CodexWrapper: tool, CodexHome: root, OutputSchema: schema,
		Model: "gpt-5.6-sol", ReasoningEffort: "low",
	})
	if err != nil {
		t.Fatalf("inspect authenticated inputs: %v", err)
	}
	if metadata.Authentication != "wrapper-and-pinned-cli-profile-logged-in-using-chatgpt" || metadata.Hermetic {
		t.Fatalf("authenticated metadata = %+v", metadata)
	}
}

func TestInputInspectionPropagatesAuthenticationAndHashFailures(t *testing.T) {
	root := t.TempDir()
	writeTool := func(name, login string) string {
		t.Helper()
		path := filepath.Join(root, name)
		script := "#!/bin/sh\n" +
			"if [ \"$1\" = \"--version\" ]; then printf 'codex-test 1.0\\n'; exit 0; fi\n" + login + "\n"
		if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	good := writeTool("good", "printf 'Logged in using ChatGPT\\n'; exit 0")
	bad := writeTool("bad", "printf 'login unavailable\\n'; exit 3")
	if _, err := inspectExperimentInputs(context.Background(), ExperimentOptions{
		CodexBinary: good, CodexWrapper: bad,
	}); err == nil {
		t.Fatal("wrapper login failure was accepted")
	}
	if _, err := inspectExperimentInputs(context.Background(), ExperimentOptions{
		CodexBinary: bad, CodexWrapper: good, CodexHome: root,
	}); err == nil {
		t.Fatal("pinned profile login failure was accepted")
	}
	if _, err := hashExperimentInputs(ExperimentOptions{}); err == nil {
		t.Fatal("missing pinned input paths were hashed")
	}
	if err := verifyExperimentInputsUnchanged(ExperimentOptions{}, experimentMetadata{}); err == nil {
		t.Fatal("missing pinned inputs were accepted as unchanged")
	}
}

func TestExperimentOptionsRequireNewEvidenceAndPinnedInputs(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "tool")
	if err := os.WriteFile(executable, []byte("tool"), 0o700); err != nil {
		t.Fatalf("write tool: %v", err)
	}
	schema := filepath.Join(directory, "schema.json")
	if err := os.WriteFile(schema, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	options := ExperimentOptions{
		EvidenceRoot: filepath.Join(directory, "evidence"), TemporalPath: executable,
		WorkerBinary: executable, EffectBinary: executable, LauncherBinary: executable,
		CodexBinary: executable, CodexWrapper: executable, CodexHome: directory, OutputSchema: schema,
		Trials: 3, Timeout: time.Minute, Model: "gpt-5.6-sol", ReasoningEffort: "low",
		RecoveryMode: RecoveryModeFenced,
	}
	if err := validateExperimentOptions(options); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := os.Mkdir(options.EvidenceRoot, 0o750); err != nil {
		t.Fatalf("create evidence: %v", err)
	}
	if err := validateExperimentOptions(options); err == nil {
		t.Fatal("existing evidence root unexpectedly succeeded")
	}
}

func TestRequireChatGPTLoginRejectsCommandAndAuthenticationFailures(t *testing.T) {
	commandErr := errors.New("command failed")
	if err := requireChatGPTLogin("wrapper", []byte("not logged in"), commandErr); !errors.Is(err, commandErr) {
		t.Fatalf("command failure = %v", err)
	}
	if err := requireChatGPTLogin("wrapper", []byte("Logged out"), nil); err == nil {
		t.Fatal("logged-out profile was accepted")
	}
	if err := requireChatGPTLogin("wrapper", []byte("Logged in using ChatGPT\n"), nil); err != nil {
		t.Fatalf("authenticated profile = %v", err)
	}
}

func TestExperimentOptionsRejectInvalidPinnedPaths(t *testing.T) {
	root := t.TempDir()
	executable := filepath.Join(root, "tool")
	nonExecutable := filepath.Join(root, "not-executable")
	schema := filepath.Join(root, "schema.json")
	if err := os.WriteFile(executable, []byte("tool"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nonExecutable, []byte("tool"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(schema, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	valid := ExperimentOptions{
		EvidenceRoot: filepath.Join(root, "evidence"), TemporalPath: executable,
		WorkerBinary: executable, EffectBinary: executable, LauncherBinary: executable,
		CodexBinary: executable, CodexWrapper: executable, CodexHome: root, OutputSchema: schema,
		Trials: 1, Timeout: time.Minute, Model: "gpt-5.6-sol", ReasoningEffort: "low",
		RecoveryMode: RecoveryModeUnsafeFresh,
	}
	tests := []struct {
		name   string
		mutate func(*ExperimentOptions)
	}{
		{name: "incomplete", mutate: func(options *ExperimentOptions) { options.Model = "" }},
		{name: "non-executable", mutate: func(options *ExperimentOptions) { options.WorkerBinary = nonExecutable }},
		{name: "codex-home-file", mutate: func(options *ExperimentOptions) { options.CodexHome = schema }},
		{name: "schema-directory", mutate: func(options *ExperimentOptions) { options.OutputSchema = root }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options)
			if err := validateExperimentOptions(options); err == nil {
				t.Fatal("invalid pinned options were accepted")
			}
		})
	}
}
