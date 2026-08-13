package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestLiveLargeArtifactDurabilityMatrix(t *testing.T) {
	if os.Getenv("LARGE_ARTIFACT_LIVE") != "1" {
		t.Skip("set LARGE_ARTIFACT_LIVE=1 to run the real-service matrix")
	}
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Fatalf("locate Temporal CLI: %v", err)
	}
	repositoryRoot := liveRepositoryRoot(t)
	workerBinary := filepath.Join(t.TempDir(), "large-artifact-worker")
	buildArguments := []string{"build", "-race", "-trimpath"}
	coverageRoot := os.Getenv("LARGE_ARTIFACT_CHILD_COVERAGE")
	if coverageRoot != "" {
		if !filepath.IsAbs(coverageRoot) {
			t.Fatal("LARGE_ARTIFACT_CHILD_COVERAGE must be absolute")
		}
		buildArguments = append(buildArguments, "-cover", "-covermode=atomic",
			"-coverpkg=./experiments/large-artifact-durability/...")
	}
	buildArguments = append(buildArguments, "-o", workerBinary,
		"./experiments/large-artifact-durability/cmd/worker")
	build := exec.Command("go", buildArguments...)
	build.Dir = repositoryRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Worker: %v\n%s", err, output)
	}
	provenance, err := CaptureRuntimeProvenance(context.Background(), temporalPath, workerBinary)
	if err != nil {
		t.Fatalf("capture runtime provenance: %v", err)
	}
	sourcePins, err := CaptureSourcePins()
	if err != nil {
		t.Fatalf("capture source pins: %v", err)
	}

	boundaries := []Boundary{
		BoundaryBlobPublished,
		BoundaryReferenceCreated,
		BoundaryReferencePublished,
		BoundaryActivityCompleted,
		BoundaryAcknowledgementPublished,
		BoundaryExternalStorageStored,
	}
	modes := []Mode{ModeUnsafe, ModeProtected}
	if selected := os.Getenv("LARGE_ARTIFACT_BOUNDARY"); selected != "" {
		boundaries = []Boundary{Boundary(selected)}
	}
	if selected := os.Getenv("LARGE_ARTIFACT_MODE"); selected != "" {
		modes = []Mode{Mode(selected)}
	}
	trials := 3
	if os.Getenv("LARGE_ARTIFACT_SINGLE_TRIAL") == "1" {
		trials = 1
	}
	outputRoot := os.Getenv("LARGE_ARTIFACT_EVIDENCE_ROOT")
	if outputRoot == "" {
		outputRoot = t.TempDir()
	} else if !filepath.IsAbs(outputRoot) {
		t.Fatal("LARGE_ARTIFACT_EVIDENCE_ROOT must be absolute")
	}
	for _, boundary := range boundaries {
		for _, mode := range modes {
			for trial := 1; trial <= trials; trial++ {
				boundary, mode, trial := boundary, mode, trial
				name := fmt.Sprintf("%s/%s/trial-%d", boundary, mode, trial)
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
					defer cancel()
					result, err := Run(ctx, Options{
						Boundary: boundary, Mode: mode,
						TemporalPath: temporalPath, WorkerBinary: workerBinary,
						CoverageRoot: coverageRoot,
						Provenance:   provenance,
						SourcePins:   sourcePins,
						OutputRoot:   outputRoot,
						RunID:        fmt.Sprintf("%s-%s-trial-%d", boundary, mode, trial),
						Timeout:      80 * time.Second,
					})
					if err != nil {
						t.Fatalf("Run: %v", err)
					}
					if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
						t.Fatalf("verdict = %+v", result.Verdict)
					}
					audited, err := AuditRun(result.RunDirectory)
					if err != nil {
						t.Fatalf("AuditRun: %v", err)
					}
					if !audited.RunValid || !audited.ExpectedObservation {
						t.Fatalf("audited verdict = %+v", audited)
					}
					if boundary == BoundaryReferencePublished && mode == ModeProtected && trial == 1 {
						testLiveAuditMutations(t, result.RunDirectory)
					}
				})
			}
		}
	}
	if os.Getenv("LARGE_ARTIFACT_BOUNDARY") == "" && os.Getenv("LARGE_ARTIFACT_MODE") == "" {
		index, err := PreservePopulationIndex(outputRoot, boundaries, modes, trials)
		if err != nil {
			t.Fatalf("PreservePopulationIndex: %v", err)
		}
		if trials == 3 {
			audited, err := AuditPopulation(outputRoot)
			if err != nil {
				t.Fatalf("AuditPopulation: %v", err)
			}
			if !reflect.DeepEqual(audited, index) {
				t.Fatalf("population views differ: %+v != %+v", audited, index)
			}
		}
	}
}

func testLiveAuditMutations(t *testing.T, source string) {
	t.Helper()
	for name, mutate := range map[string]func(*testing.T, string){
		"runtime digest": func(t *testing.T, root string) {
			var manifest Manifest
			readLiveJSON(t, filepath.Join(root, "manifest.json"), &manifest)
			manifest.Runtime.WorkerSHA256 = strings.Repeat("c", 64)
			writeLiveJSON(t, filepath.Join(root, "manifest.json"), manifest)
		},
		"evidence identity": func(t *testing.T, root string) {
			var evidence Evidence
			readLiveJSON(t, filepath.Join(root, "evidence.json"), &evidence)
			evidence.Boundary = BoundaryBlobPublished
			writeLiveJSON(t, filepath.Join(root, "evidence.json"), evidence)
			refreshLiveManifest(t, root)
		},
		"stored verdict": func(t *testing.T, root string) {
			var verdict Verdict
			readLiveJSON(t, filepath.Join(root, "verdict.json"), &verdict)
			verdict.InvariantSatisfied = !verdict.InvariantSatisfied
			writeLiveJSON(t, filepath.Join(root, "verdict.json"), verdict)
			refreshLiveManifest(t, root)
		},
		"source artifact": func(t *testing.T, root string) {
			path := filepath.Join(root, "source-artifact.bin")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read source artifact: %v", err)
			}
			data[0] ^= 0xff
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("mutate source artifact: %v", err)
			}
			refreshLiveManifest(t, root)
		},
		"raw history": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "temporal-history.json"), []byte("{\"events\":[]}\n"), 0o600); err != nil {
				t.Fatalf("mutate history: %v", err)
			}
			refreshLiveManifest(t, root)
		},
		"extra inventory": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "extra.json"), []byte("{}\n"), 0o600); err != nil {
				t.Fatalf("write extra evidence: %v", err)
			}
		},
		"symlink": func(t *testing.T, root string) {
			if err := os.Symlink("manifest.json", filepath.Join(root, "link")); err != nil {
				t.Fatalf("write evidence symlink: %v", err)
			}
		},
	} {
		name, mutate := name, mutate
		t.Run("audit-rejects-"+name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), filepath.Base(source))
			copyLiveTree(t, source, root)
			mutate(t, root)
			if _, err := AuditRun(root); err == nil {
				t.Fatal("coherently mutated evidence accepted")
			}
		})
	}
}

func copyLiveTree(t *testing.T, source, destination string) {
	t.Helper()
	if err := os.Mkdir(destination, 0o750); err != nil {
		t.Fatalf("create evidence copy: %v", err)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatalf("read evidence source: %v", err)
	}
	for _, entry := range entries {
		from, to := filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name())
		if entry.IsDir() {
			copyLiveTree(t, from, to)
			continue
		}
		data, err := os.ReadFile(from)
		if err != nil {
			t.Fatalf("read evidence file: %v", err)
		}
		if err := os.WriteFile(to, data, 0o600); err != nil {
			t.Fatalf("write evidence file: %v", err)
		}
	}
}

func readLiveJSON(t *testing.T, path string, target any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || decodeStrictJSON(data, target) != nil {
		t.Fatalf("read JSON %s: %v", path, err)
	}
}

func writeLiveJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("encode JSON %s: %v", path, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write JSON %s: %v", path, err)
	}
}

func refreshLiveManifest(t *testing.T, root string) {
	t.Helper()
	var manifest Manifest
	readLiveJSON(t, filepath.Join(root, "manifest.json"), &manifest)
	files, directories, err := evidenceInventory(root)
	if err != nil {
		t.Fatalf("inventory mutated evidence: %v", err)
	}
	manifest.Files = files
	manifest.Directories = directories
	writeLiveJSON(t, filepath.Join(root, "manifest.json"), manifest)
}

func liveRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate live test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
