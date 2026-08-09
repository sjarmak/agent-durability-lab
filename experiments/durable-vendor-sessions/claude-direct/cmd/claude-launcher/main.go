package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

const (
	realBinaryEnv   = "CLAUDE_DIRECT_REAL_BINARY"
	barrierURLEnv   = "CLAUDE_DIRECT_PRE_SESSION_BARRIER_URL"
	barrierPointEnv = "CLAUDE_DIRECT_PRE_SESSION_BARRIER_POINT"
	arrivalIDEnv    = "CLAUDE_DIRECT_PHYSICAL_ATTEMPT_ID"
	sessionIDEnv    = "CLAUDE_DIRECT_LOGICAL_SESSION_ID"
	actorIDEnv      = "CLAUDE_DIRECT_ACTOR_ID"
)

type launcherConfig struct {
	RealBinary   string
	BarrierURL   string
	BarrierPoint string
	ArrivalID    string
	SessionID    string
	ActorID      string
	Args         []string
}

type execVendorFunc func(launcherConfig) error

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config := launcherConfigFromEnvironment(os.Args[1:])
	if err := runLauncher(ctx, config, execVendor); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func launcherConfigFromEnvironment(args []string) launcherConfig {
	return launcherConfig{
		RealBinary: os.Getenv(realBinaryEnv), BarrierURL: os.Getenv(barrierURLEnv),
		BarrierPoint: os.Getenv(barrierPointEnv), ArrivalID: os.Getenv(arrivalIDEnv),
		SessionID: os.Getenv(sessionIDEnv), ActorID: os.Getenv(actorIDEnv),
		Args: append([]string(nil), args...),
	}
}

func runLauncher(ctx context.Context, config launcherConfig, execute execVendorFunc) error {
	if err := config.validate(); err != nil {
		return err
	}
	if execute == nil {
		return errors.New("vendor exec function is required")
	}
	pid := os.Getpid()
	processStart, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return fmt.Errorf("identify pre-registration launcher: %w", err)
	}
	arrival := failureinject.Arrival{
		ID: config.ArrivalID, Point: config.BarrierPoint, SessionID: config.SessionID,
		ActorID: config.ActorID, PID: pid, ProcessStart: processStart,
	}
	if err := failureinject.NewClient(config.BarrierURL).Arrive(ctx, arrival); err != nil {
		return fmt.Errorf("wait before vendor registration: %w", err)
	}
	return execute(config)
}

func (c launcherConfig) validate() error {
	if c.RealBinary == "" || !filepath.IsAbs(c.RealBinary) || c.BarrierURL == "" ||
		c.BarrierPoint == "" || c.ArrivalID == "" || c.SessionID == "" || c.ActorID == "" || len(c.Args) == 0 {
		return errors.New("launcher requires vendor binary, barrier, and stable attempt identities")
	}
	return nil
}

func execVendor(config launcherConfig) error {
	argv := append([]string{config.RealBinary}, config.Args...)
	if err := syscall.Exec(config.RealBinary, argv, os.Environ()); err != nil {
		return fmt.Errorf("exec Claude binary: %w", err)
	}
	return nil
}
