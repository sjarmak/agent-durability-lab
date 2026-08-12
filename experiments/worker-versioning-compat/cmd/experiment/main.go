package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/experiments/worker-versioning-compat/internal/lab"
	"go.temporal.io/sdk/testsuite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var outputRoot, temporalPath string
	flag.StringVar(&outputRoot, "output", "", "new append-only evidence directory")
	flag.StringVar(&temporalPath, "temporal", "", "Temporal CLI executable")
	flag.Parse()
	if outputRoot == "" || flag.NArg() != 0 {
		return fmt.Errorf("--output is required; positional arguments are forbidden")
	}
	if temporalPath == "" {
		var err error
		temporalPath, err = exec.LookPath("temporal")
		if err != nil {
			return fmt.Errorf("locate Temporal CLI: %w", err)
		}
	}
	versionOutput, err := exec.Command(temporalPath, "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("read Temporal CLI version: %w", err)
	}
	executableDigest, err := currentExecutableSHA256()
	if err != nil {
		return err
	}
	workingRoot, err := os.MkdirTemp("", "worker-versioning-compat-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workingRoot)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: temporalPath, DBFilename: filepath.Join(workingRoot, "temporal.db"), EnableUI: false,
	})
	if err != nil {
		return err
	}
	defer server.Stop()
	result, err := lab.RunExperiment(ctx, lab.RunOptions{
		Client: server.Client(), Root: outputRoot,
		Environment: lab.Environment{
			CapturedAt: time.Now().UTC(), GoVersion: runtime.Version(), SDKVersion: temporalSDKVersion(),
			TemporalCLI: strings.TrimSpace(string(versionOutput)), ExecutableSHA256: executableDigest,
			OS: runtime.GOOS, Architecture: runtime.GOARCH,
		},
	})
	if err != nil {
		return err
	}
	if _, err := lab.AuditEvidence(outputRoot); err != nil {
		return fmt.Errorf("audit preserved evidence: %w", err)
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func currentExecutableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate experiment executable: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open experiment executable: %w", err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", fmt.Errorf("hash experiment executable: %w", err)
	}
	return fmt.Sprintf("%x", digest.Sum(nil)), nil
}

func temporalSDKVersion() string {
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dependency := range build.Deps {
		if dependency.Path == "go.temporal.io/sdk" {
			return dependency.Version
		}
	}
	return "unknown"
}
