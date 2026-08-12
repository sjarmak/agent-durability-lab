package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func main() {
	if err := runCLI(context.Background(), os.Args[1:], os.Stdout, os.Stderr, executeCommand); err != nil {
		fmt.Fprintf(os.Stderr, "effect-safe-tools: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, arguments []string, stdout, stderr io.Writer, runner commandRunner) error {
	if len(arguments) == 0 {
		return errors.New("usage: effect-safe-tools <audit|list|run>")
	}
	switch arguments[0] {
	case "audit":
		flags := flag.NewFlagSet("audit", flag.ContinueOnError)
		flags.SetOutput(stderr)
		repositoryRoot := flags.String("repo", "", "repository root (auto-detected by default)")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("audit accepts no positional arguments")
		}
		root, err := resolveRepositoryRoot(*repositoryRoot)
		if err != nil {
			return err
		}
		report, err := auditFinalEvidence(root)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "verified %d runs: unsafe=%d physical effects, protected=%d physical effects, git bundles=%d, artifact files=%d\n",
			report.Runs, report.UnsafePhysicalEffects, report.ProtectedPhysicalEffects,
			report.VerifiedGitBundles, report.VerifiedArtifactFiles)
		return nil
	case "list":
		if len(arguments) != 1 {
			return errors.New("list accepts no arguments")
		}
		for _, recipe := range recipes() {
			fmt.Fprintf(stdout, "%s: %s\n", recipe.Destination, recipe.Mechanism)
		}
		return nil
	case "run":
		flags := flag.NewFlagSet("run", flag.ContinueOnError)
		flags.SetOutput(stderr)
		repositoryRoot := flags.String("repo", "", "repository root (auto-detected by default)")
		destination := flags.String("destination", "", "one of the six destination names")
		outputRoot := flags.String("output", "", "new append-only evidence root outside preserved final evidence")
		runID := flags.String("run-id", "", "stable prefix for the fresh run")
		trials := flags.Int("trials", 3, "fresh trials per unsafe/protected arm")
		temporalPath := flags.String("temporal", "", "Temporal CLI path (PATH lookup by default)")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("run accepts no positional arguments")
		}
		root, err := resolveRepositoryRoot(*repositoryRoot)
		if err != nil {
			return err
		}
		if runner == nil {
			runner = executeCommand
		}
		return runRecipe(ctx, runner, runOptions{
			RepositoryRoot: root,
			Destination:    Destination(*destination),
			OutputRoot:     *outputRoot,
			RunID:          *runID,
			Trials:         *trials,
			TemporalPath:   *temporalPath,
		})
	default:
		return fmt.Errorf("unknown command %q", arguments[0])
	}
}

func resolveRepositoryRoot(explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	directory, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", errors.New("repository root not found; pass --repo")
		}
		directory = parent
	}
}
