package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/abalive"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ABA client: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	requestPath := flag.String("request", "", "path to the private ABA client request")
	flag.Parse()
	if *requestPath == "" {
		return errors.New("--request is required")
	}
	request, err := abalive.ReadLaunchRequest(*requestPath)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	result, err := abalive.RunClient(ctx, request)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
