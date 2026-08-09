package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/systemsuite"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/postgresadapter"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/temporaladapter"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent durability v1 system suite: %v\n", err)
		os.Exit(1)
	}
}

func run() (returnErr error) {
	system := flag.String("system", "", "temporal or postgresql-queue")
	root := flag.String("evidence-root", "", "append-only evidence root")
	trials := flag.Int("trials", 3, "development trials")
	agentBinary := flag.String("agent-binary", "", "common agent simulator binary")
	adapterVersion := flag.String("adapter-version", "", "source-sha256:<64 hex characters>")
	workRoot := flag.String("work-root", "", "Temporal work root")
	temporalPath := flag.String("temporal-path", "", "Temporal CLI path")
	postgresDSN := flag.String("postgres-dsn", "", "PostgreSQL connection string")
	flag.Parse()
	if *system == "" || *root == "" || *trials < 1 || *agentBinary == "" || *adapterVersion == "" {
		return errors.New("--system, --evidence-root, --trials, --agent-binary, and --adapter-version are required")
	}
	ctx := context.Background()
	config := systemsuite.Config{Root: *root, Trials: *trials, AgentBinary: *agentBinary}
	var verdicts any
	switch *system {
	case "temporal":
		if *temporalPath == "" || *workRoot == "" {
			return errors.New("temporal requires --temporal-path and --work-root")
		}
		session, err := temporaladapter.Open(ctx, temporaladapter.Config{
			TemporalPath: *temporalPath, WorkRoot: *workRoot, AdapterVersion: *adapterVersion,
		})
		if err != nil {
			return err
		}
		defer func() {
			returnErr = errors.Join(returnErr, session.Close())
		}()
		verdicts, err = systemsuite.Run(ctx, session, config)
		if err != nil {
			return err
		}
	case "postgresql-queue":
		if *postgresDSN == "" {
			return errors.New("postgresql requires --postgres-dsn")
		}
		session, err := postgresadapter.Open(ctx, postgresadapter.Config{DSN: *postgresDSN, AdapterVersion: *adapterVersion})
		if err != nil {
			return err
		}
		verdicts, err = systemsuite.Run(ctx, session, config)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported system %q", *system)
	}
	return json.NewEncoder(os.Stdout).Encode(verdicts)
}
