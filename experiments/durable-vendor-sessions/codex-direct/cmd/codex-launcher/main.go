package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

const (
	realBinaryEnvironment        = "CODEX_DIRECT_REAL_BINARY"
	barrierURLEnvironment        = "CODEX_DIRECT_PRE_THREAD_BARRIER_URL"
	barrierPointEnvironment      = "CODEX_DIRECT_PRE_THREAD_BARRIER_POINT"
	physicalAttemptEnvironment   = "CODEX_DIRECT_PHYSICAL_ATTEMPT_ID"
	logicalSessionEnvironment    = "CODEX_DIRECT_LOGICAL_SESSION_ID"
	actorEnvironment             = "CODEX_DIRECT_ACTOR_ID"
	generationEnvironment        = "CODEX_DIRECT_GENERATION"
	parentProcessGateEnvironment = "CODEX_DIRECT_PROCESS_START_GATE_FD"
)

type launcherConfig struct {
	RealBinary          string
	BarrierURL          string
	BarrierPoint        string
	PhysicalAttemptID   string
	LogicalSessionID    string
	ActorID             string
	Generation          uint64
	ParentProcessGateFD int
	Args                []string
	BarrierCredential   failureinject.Credential
}

type execCodexFunc func(launcherConfig) error

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	parentProcessGateFD, _ := strconv.Atoi(os.Getenv(parentProcessGateEnvironment))
	generation, _ := strconv.ParseUint(os.Getenv(generationEnvironment), 10, 64)
	config := launcherConfig{
		RealBinary: os.Getenv(realBinaryEnvironment), BarrierURL: os.Getenv(barrierURLEnvironment),
		BarrierPoint: os.Getenv(barrierPointEnvironment), PhysicalAttemptID: os.Getenv(physicalAttemptEnvironment),
		LogicalSessionID: os.Getenv(logicalSessionEnvironment), ActorID: os.Getenv(actorEnvironment),
		Generation:          generation,
		ParentProcessGateFD: parentProcessGateFD,
		Args:                append([]string(nil), os.Args[1:]...),
	}
	credential, err := failureinject.ReadCredentialFromEnvironment()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	config.BarrierCredential = credential
	if err := runLauncher(ctx, config, execCodex); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runLauncher(ctx context.Context, config launcherConfig, execute execCodexFunc) error {
	if !filepath.IsAbs(config.RealBinary) || filepath.Clean(config.RealBinary) != config.RealBinary ||
		config.BarrierURL == "" || config.BarrierPoint == "" || config.PhysicalAttemptID == "" ||
		config.LogicalSessionID == "" || config.Generation == 0 || config.ActorID == "" ||
		config.ParentProcessGateFD < 3 || len(config.Args) == 0 {
		return errors.New("codex launcher requires binary, exact barrier, identities, and arguments")
	}
	if execute == nil {
		return errors.New("codex exec function is required")
	}
	if err := waitForParentProcessReceipt(config.ParentProcessGateFD); err != nil {
		return err
	}
	pid := os.Getpid()
	processStart, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return fmt.Errorf("identify pre-thread Codex launcher: %w", err)
	}
	client := failureinject.NewClient(config.BarrierURL)
	if config.BarrierCredential.IsSet() {
		client = failureinject.NewAuthenticatedClient(config.BarrierURL, config.BarrierCredential)
	}
	if err := client.Arrive(ctx, failureinject.Arrival{
		ID: config.PhysicalAttemptID, Point: config.BarrierPoint, SessionID: config.LogicalSessionID,
		Generation: config.Generation, ActorID: config.ActorID, PID: pid, ProcessStart: processStart,
	}); err != nil {
		return fmt.Errorf("wait before Codex thread observation: %w", err)
	}
	return execute(config)
}

func waitForParentProcessReceipt(fd int) (returnErr error) {
	gate := os.NewFile(uintptr(fd), "codex-parent-process-receipt")
	if gate == nil {
		return errors.New("open Codex parent process receipt gate")
	}
	defer func() { returnErr = errors.Join(returnErr, gate.Close()) }()
	var released [1]byte
	if _, err := io.ReadFull(gate, released[:]); err != nil {
		return fmt.Errorf("wait for Codex parent process receipt: %w", err)
	}
	return nil
}

func execCodex(config launcherConfig) error {
	arguments := append([]string{config.RealBinary}, config.Args...)
	if err := syscall.Exec(config.RealBinary, arguments, os.Environ()); err != nil {
		return fmt.Errorf("exec Codex binary: %w", err)
	}
	return nil
}
