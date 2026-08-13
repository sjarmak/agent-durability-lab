package explorer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

const unsafeEpisodeID = "codex-v12-unsafe-effect-1"

func TestRepositoryReadsOnlyCatalogedVerifiedArtifacts(t *testing.T) {
	repository, err := OpenRepository(repositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })

	artifact, err := repository.ReadEpisodeArtifact(
		context.Background(), unsafeEpisodeID, "history",
	)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ContentType != "application/json" || artifact.Filename != "workflow-history.json" {
		t.Fatalf("artifact metadata = %+v", artifact)
	}
	var history struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(artifact.Data, &history); err != nil {
		t.Fatalf("history JSON: %v", err)
	}
	if len(history.Events) == 0 {
		t.Fatal("native history has no events")
	}

	raw, err := repository.ReadEpisodeArtifact(
		context.Background(), unsafeEpisodeID, "raw-0",
	)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Filename != "trial-summary.json" || !json.Valid(raw.Data) {
		t.Fatalf("raw artifact = %+v", raw)
	}

	for _, selector := range []string{"", "raw", "raw-99", "../history", "effect-request"} {
		if _, err := repository.ReadEpisodeArtifact(context.Background(), unsafeEpisodeID, selector); !errors.Is(err, ErrArtifactNotFound) {
			t.Fatalf("selector %q = %v, want ErrArtifactNotFound", selector, err)
		}
	}
	if _, err := repository.ReadEpisodeArtifact(context.Background(), "missing", "history"); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("missing episode = %v", err)
	}
	for _, selector := range []string{"manifest", "report", "lineage-0"} {
		artifact, err := repository.ReadEpisodeArtifact(context.Background(), unsafeEpisodeID, selector)
		if err != nil {
			t.Fatalf("read %s: %v", selector, err)
		}
		if !json.Valid(artifact.Data) {
			t.Fatalf("%s is not JSON", selector)
		}
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repository.ReadEpisodeArtifact(canceled, unsafeEpisodeID, "history"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
}

func TestRepositoryRejectsTamperedExtraAndSymlinkedTransportInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "tampered archive",
			mutate: func(t *testing.T, copiedRoot string) {
				archive := filepath.Join(copiedRoot, "codex-direct-hermetic-unsafe-20260812-v12.tar.gz")
				file, err := os.OpenFile(archive, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.Write([]byte("tamper")); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra file",
			mutate: func(t *testing.T, copiedRoot string) {
				if err := os.WriteFile(filepath.Join(copiedRoot, "attacker-extra.json"), []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlinked archive",
			mutate: func(t *testing.T, copiedRoot string) {
				archive := filepath.Join(copiedRoot, "codex-direct-hermetic-unsafe-20260812-v12.tar.gz")
				target := filepath.Join(t.TempDir(), "archive.tar.gz")
				copyRegularFile(t, archive, target)
				if err := os.Remove(archive); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, archive); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			temporaryRepository := t.TempDir()
			relative := "experiments/durable-vendor-sessions/codex-direct/evidence-transport/hermetic-unsafe-20260812-v4"
			copiedRoot := filepath.Join(temporaryRepository, relative)
			copyRegularTree(t, filepath.Join(repositoryRoot(t), relative), copiedRoot)
			test.mutate(t, copiedRoot)
			repository, err := OpenRepository(temporaryRepository)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = repository.Close() })
			if _, err := repository.ReadEpisodeArtifact(context.Background(), unsafeEpisodeID, "history"); err == nil {
				t.Fatal("tampered transport was accepted")
			}
		})
	}
}

func TestRepositoryCloseIsEnforced(t *testing.T) {
	repository, err := OpenRepository(repositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := repository.Catalog(); err == nil {
		t.Fatal("closed repository returned its catalog")
	}
	if _, err := repository.ReadEpisodeArtifact(context.Background(), unsafeEpisodeID, "history"); err == nil {
		t.Fatal("closed repository served evidence")
	}
}

func TestRepositoryRejectsSymlinkedRoot(t *testing.T) {
	link := filepath.Join(t.TempDir(), "repository-link")
	if err := os.Symlink(repositoryRoot(t), link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRepository(link); err == nil {
		t.Fatal("symlinked repository root was accepted")
	}
}

func TestManifestDecoderRejectsDuplicateKeys(t *testing.T) {
	data := []byte(`{"schema_version":"one","schema_version":"two"}`)
	if _, err := decodeManifest(data); err == nil {
		t.Fatal("duplicate manifest key was accepted")
	}
}

func TestReadRegularAtEnforcesByteBound(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "oversized.json"), []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := readRegularAt(root, "oversized.json", 4); err == nil {
		t.Fatal("oversized regular file was accepted")
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate explorer tests")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
}

func copyRegularTree(t *testing.T, source, destination string) {
	t.Helper()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destination, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			t.Fatalf("fixture transport entry %q is not a regular file", entry.Name())
		}
		copyRegularFile(t, filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name()))
	}
}

func copyRegularFile(t *testing.T, source, destination string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("source fixture %q is not regular: %v", source, err)
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy fixture: %v / %v", copyErr, closeErr)
	}
}
