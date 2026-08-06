package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/temporalio-labs/agent-durability-lab/internal/agentprocess"
	"github.com/temporalio-labs/agent-durability-lab/internal/agentsim"
	"github.com/temporalio-labs/agent-durability-lab/internal/failureinject"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent simulator: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	requestPath := flag.String("request", "", "path to the private launch request")
	flag.Parse()
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
	store, err := workstore.Open(request.StorePath)
	if err != nil {
		return err
	}
	runner := agentsim.New(store, failureinject.NewClient(request.BarrierURL))
	result, err := runner.Run(context.Background(), request.Config)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return fmt.Errorf("write simulator result: %w", err)
	}
	return nil
}

func readRequest(path string) (agentprocess.LaunchRequest, error) {
	file, err := os.Open(path)
	if err != nil {
		return agentprocess.LaunchRequest{}, fmt.Errorf("open launch request: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var request agentprocess.LaunchRequest
	if err := decoder.Decode(&request); err != nil {
		return agentprocess.LaunchRequest{}, fmt.Errorf("decode launch request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return agentprocess.LaunchRequest{}, errors.New("launch request contains trailing data")
	}
	return request, nil
}
