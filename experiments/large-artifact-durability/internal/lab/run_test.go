package lab

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateOptionsFailsBeforeCreatingEvidence(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	valid := Options{
		Boundary:     BoundaryReferencePublished,
		Mode:         ModeProtected,
		TemporalPath: executable,
		WorkerBinary: executable,
		OutputRoot:   t.TempDir(),
		RunID:        "large-artifact-test-1",
		Timeout:      time.Minute,
		Provenance:   testRuntimeProvenance(t),
	}
	valid.SourcePins, err = CaptureSourcePins()
	if err != nil {
		t.Fatalf("CaptureSourcePins: %v", err)
	}
	if err := validateOptions(valid); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	for name, mutate := range map[string]func(*Options){
		"boundary": func(options *Options) { options.Boundary = "invalid" },
		"mode":     func(options *Options) { options.Mode = "invalid" },
		"run ID":   func(options *Options) { options.RunID = "../escape" },
		"worker":   func(options *Options) { options.WorkerBinary = filepath.Join(t.TempDir(), "missing") },
		"provenance": func(options *Options) {
			options.Provenance.WorkerSHA256 = "invalid"
		},
		"coverage": func(options *Options) { options.CoverageRoot = filepath.Join(t.TempDir(), "missing") },
		"output symlink": func(options *Options) {
			alias := filepath.Join(t.TempDir(), "alias")
			if err := os.Symlink(options.OutputRoot, alias); err != nil {
				t.Fatalf("create output symlink: %v", err)
			}
			options.OutputRoot = alias
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if err := validateOptions(changed); err == nil {
				t.Fatal("invalid options accepted")
			}
			if _, err := os.Stat(filepath.Join(changed.OutputRoot, changed.RunID)); !os.IsNotExist(err) {
				t.Fatalf("validation created run directory: %v", err)
			}
		})
	}
}

func TestWorkerEnvironmentAddsOnlyExplicitCoverageRoot(t *testing.T) {
	t.Parallel()

	config := workerProcessConfig{}
	if got := workerEnvironment(config); len(got) != 3 {
		t.Fatalf("default Worker environment = %v", got)
	}
	config.CoverageRoot = "/tmp/coverage"
	got := workerEnvironment(config)
	if len(got) != 4 || got[3] != "GOCOVERDIR=/tmp/coverage" {
		t.Fatalf("covered Worker environment = %v", got)
	}
}
