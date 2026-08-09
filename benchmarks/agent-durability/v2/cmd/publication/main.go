package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/publication"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent durability v2 publication: %v\n", err)
		os.Exit(1)
	}
}

func run() (returnErr error) {
	phaseValue := flag.String("phase", "", "required phase: pilot or publication")
	root := flag.String("root", "", "required append-only population evidence root")
	workRoot := flag.String("work-root", "", "required Temporal service work root outside the evidence root")
	preregistrationPath := flag.String("preregistration", "benchmarks/agent-durability/publication-preregistration-v2.json", "frozen preregistration path")
	benchmarkRoot := flag.String("benchmark-root", "benchmarks/agent-durability", "benchmark root containing contract-v2.json and v2 source")
	repositoryRoot := flag.String("repository-root", ".", "repository root used for frozen harness source verification")
	harnessFreezePath := flag.String("harness-freeze", "benchmarks/agent-durability/publication-harness-freeze-v2.json", "post-pilot harness freeze")
	temporalPath := flag.String("temporal-path", "", "required Temporal CLI path")
	postgresDSN := flag.String("postgres-dsn", "", "required PostgreSQL connection string")
	deadline := flag.Duration("deadline", 2*time.Hour, "whole-run liveness deadline")
	flag.Parse()
	phase := publication.Phase(*phaseValue)
	if *root == "" || *workRoot == "" || *temporalPath == "" || *postgresDSN == "" || *deadline <= 0 ||
		(phase != publication.PhasePilot && phase != publication.PhasePublication) {
		return errors.New("phase, root, work-root, temporal-path, postgres-dsn, and a positive deadline are required")
	}
	evidenceRoot, err := filepath.Abs(*root)
	if err != nil {
		return err
	}
	temporalWorkRoot, err := filepath.Abs(*workRoot)
	if err != nil {
		return err
	}
	relativeWork, err := filepath.Rel(evidenceRoot, temporalWorkRoot)
	if err != nil {
		return err
	}
	if relativeWork == "." || (relativeWork != ".." && !strings.HasPrefix(relativeWork, ".."+string(filepath.Separator))) {
		return errors.New("work-root must be outside the append-only evidence root")
	}
	registration, err := publication.LoadPreregistration(*preregistrationPath)
	if err != nil {
		return err
	}
	if err := publication.VerifyHashPins(registration, *benchmarkRoot); err != nil {
		return err
	}
	schedule, err := publication.BuildPilotSchedule(registration)
	if phase == publication.PhasePublication {
		schedule, err = publication.BuildSchedule(registration)
	}
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	if phase == publication.PhasePublication {
		freeze, err := publication.LoadHarnessFreeze(*harnessFreezePath)
		if err != nil {
			return err
		}
		if err := publication.VerifyHarnessFreeze(freeze, *repositoryRoot, executable, *preregistrationPath); err != nil {
			return err
		}
	}
	agentHash, err := protocol.FileSHA256(executable)
	if err != nil {
		return err
	}
	adapterVersion := "source-sha256:" + registration.Hashes.AdapterBaselineSHA256
	ctx, cancel := context.WithTimeout(context.Background(), *deadline)
	defer cancel()
	temporal, err := publication.OpenTemporalTimedExecutor(ctx, publication.TemporalExecutorConfig{
		TemporalPath: *temporalPath, WorkRoot: temporalWorkRoot,
		EvidenceRoot:   filepath.Join(evidenceRoot, "systems", publication.SystemTemporal),
		AdapterVersion: adapterVersion, AgentBinarySHA256: agentHash,
	})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, temporal.Close()) }()
	postgresql, err := publication.OpenPostgreSQLTimedExecutor(ctx, publication.PostgreSQLExecutorConfig{
		DSN: *postgresDSN, EvidenceRoot: filepath.Join(evidenceRoot, "systems", publication.SystemPostgreSQL),
		AdapterVersion: adapterVersion, AgentBinarySHA256: agentHash,
	})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, postgresql.Close()) }()
	runner := publication.RunnerConfig{
		Root: filepath.Join(evidenceRoot, "pairs"), Phase: phase,
		Executors: map[string]publication.TimedExecutor{
			publication.SystemTemporal: temporal, publication.SystemPostgreSQL: postgresql,
		},
	}
	execution, err := publication.RunPopulation(ctx, publication.PopulationRunConfig{
		Root: evidenceRoot, Runner: runner, Phase: phase, Schedule: schedule, Registration: registration,
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(execution)
}
