package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/agentsim"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent simulator: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	requestPath := flag.String("request", "", "path to the private launch request")
	toolChild := flag.Bool("tool-child", false, "run as an agent-spawned tool child")
	toolStore := flag.String("tool-store", "", "application work store used by a tool child")
	toolBarrier := flag.String("tool-barrier", "", "failure-injection barrier used by a tool child")
	toolSession := flag.String("tool-session", "", "logical session used by a tool child")
	toolGeneration := flag.Uint64("tool-generation", 0, "logical generation used by a tool child")
	toolOwnerHash := flag.String("tool-owner-hash", "", "owner capability hash used by a tool child")
	flag.Parse()
	if *toolChild {
		return runToolChild(toolChildOptions{
			StorePath: *toolStore, BarrierURL: *toolBarrier, SessionID: *toolSession,
			Generation: *toolGeneration, OwnerTokenHash: *toolOwnerHash,
		})
	}
	if *requestPath == "" {
		return errors.New("--request is required")
	}
	request, err := readRequest(*requestPath)
	if err != nil {
		return err
	}
	if err := os.Remove(*requestPath); err != nil {
		return fmt.Errorf("remove consumed launch request: %w", err)
	}

	processStart, err := agentprocess.CurrentProcessStartIdentity()
	if err != nil {
		return err
	}
	request.Config.PID = os.Getpid()
	request.Config.ProcessStart = processStart
	processGroupID, err := agentprocess.CurrentProcessGroupID()
	if err != nil {
		return err
	}
	request.Config.ProcessGroupID = processGroupID
	store, err := workstore.Open(request.StorePath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	stopResult := make(chan error, 1)
	go func() {
		select {
		case received := <-signals:
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			err := store.RecordObservation(stopCtx, workstore.Event{
				Kind: "executor_stop_received", SessionID: request.Config.Lease.SessionID,
				Generation:     request.Config.Lease.Generation,
				OwnerTokenHash: workstore.HashToken(request.Config.Lease.OwnerToken), PID: request.Config.PID,
				Details: map[string]string{
					"signal": received.String(), "process_start": request.Config.ProcessStart,
				},
			})
			if err == nil {
				_, err = acknowledgeCommittedCancellation(stopCtx, store, request.Config)
			}
			stopResult <- err
			cancel()
		case <-ctx.Done():
		}
	}()
	if request.Config.SpawnToolChild {
		if err := startToolChild(request); err != nil {
			return err
		}
	}
	runner := agentsim.New(store, failureinject.NewClient(request.BarrierURL))
	result, err := runner.Run(ctx, request.Config)
	select {
	case stopErr := <-stopResult:
		if stopErr != nil {
			return fmt.Errorf("handle executor stop: %w", stopErr)
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
	default:
	}
	if err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return fmt.Errorf("write simulator result: %w", err)
	}
	return nil
}

type toolChildOptions struct {
	StorePath      string
	BarrierURL     string
	SessionID      string
	Generation     uint64
	OwnerTokenHash string
}

func startToolChild(request agentprocess.LaunchRequest) error {
	command := exec.Command(
		os.Args[0],
		"--tool-child",
		"--tool-store", request.StorePath,
		"--tool-barrier", request.BarrierURL,
		"--tool-session", request.Config.Lease.SessionID,
		"--tool-generation", fmt.Sprint(request.Config.Lease.Generation),
		"--tool-owner-hash", workstore.HashToken(request.Config.Lease.OwnerToken),
	)
	command.Env = []string{}
	command.Stdin = nil
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start tool child: %w", err)
	}
	if err := command.Process.Release(); err != nil {
		_ = command.Process.Kill()
		return fmt.Errorf("release tool child: %w", err)
	}
	return nil
}

func runToolChild(options toolChildOptions) error {
	if options.StorePath == "" || options.BarrierURL == "" || options.SessionID == "" ||
		options.Generation == 0 || options.OwnerTokenHash == "" {
		return errors.New("tool child requires store, barrier, session, generation, and owner hash")
	}
	processStart, err := agentprocess.CurrentProcessStartIdentity()
	if err != nil {
		return err
	}
	processGroupID, err := agentprocess.CurrentProcessGroupID()
	if err != nil {
		return err
	}
	store, err := workstore.Open(options.StorePath)
	if err != nil {
		return err
	}
	pid := os.Getpid()
	if err := store.RecordObservation(context.Background(), workstore.Event{
		Kind: "tool_child_registered", SessionID: options.SessionID, Generation: options.Generation,
		OwnerTokenHash: options.OwnerTokenHash, PID: pid,
		Details: map[string]string{
			"process_start": processStart, "process_group_id": fmt.Sprint(processGroupID),
		},
	}); err != nil {
		return fmt.Errorf("record tool child identity: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	point := fmt.Sprintf("tool-child-alive/%d", options.Generation)
	err = failureinject.NewClient(options.BarrierURL).Arrive(ctx, failureinject.Arrival{
		ID:    fmt.Sprintf("tool/%s/g%d/%d", options.SessionID, options.Generation, pid),
		Point: point, SessionID: options.SessionID, OwnerTokenHash: options.OwnerTokenHash,
		Generation: options.Generation, ActorID: "tool-child", PID: pid, ProcessStart: processStart,
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("tool child barrier: %w", err)
	}
	eventKind := "tool_child_released"
	if errors.Is(err, context.Canceled) {
		eventKind = "tool_child_stop_received"
	}
	recordCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.RecordObservation(recordCtx, workstore.Event{
		Kind: eventKind, SessionID: options.SessionID, Generation: options.Generation,
		OwnerTokenHash: options.OwnerTokenHash, PID: pid,
		Details: map[string]string{
			"process_start": processStart, "process_group_id": fmt.Sprint(processGroupID),
		},
	}); err != nil {
		return fmt.Errorf("record tool child disposition: %w", err)
	}
	return nil
}

func acknowledgeCommittedCancellation(
	ctx context.Context,
	store *workstore.Store,
	config agentsim.Config,
) (bool, error) {
	snapshot, err := store.Snapshot(ctx, config.Lease.SessionID)
	if err != nil {
		return false, fmt.Errorf("observe cancellation before acknowledgement: %w", err)
	}
	if snapshot.Cancellation == nil {
		return false, nil
	}
	process := workstore.Process{
		PID: config.PID, StartIdentity: config.ProcessStart, ProcessGroupID: config.ProcessGroupID,
	}
	target := workstore.CancellationTarget{
		SessionID: config.Lease.SessionID, Generation: config.Lease.Generation,
		OwnerTokenHash: workstore.HashToken(config.Lease.OwnerToken), Process: process,
	}
	if snapshot.Cancellation.Target != target {
		return false, nil
	}
	if err := store.AcknowledgeCancellation(ctx, workstore.CancellationAcknowledgementRequest{
		RequestID: snapshot.Cancellation.RequestID, Lease: config.Lease, Process: process,
	}); err != nil {
		return false, fmt.Errorf("persist cancellation acknowledgement: %w", err)
	}
	return true, nil
}

func readRequest(path string) (request agentprocess.LaunchRequest, returnErr error) {
	file, err := os.Open(path)
	if err != nil {
		return agentprocess.LaunchRequest{}, fmt.Errorf("open launch request: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return agentprocess.LaunchRequest{}, fmt.Errorf("decode launch request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return agentprocess.LaunchRequest{}, errors.New("launch request contains trailing data")
	}
	return request, nil
}
