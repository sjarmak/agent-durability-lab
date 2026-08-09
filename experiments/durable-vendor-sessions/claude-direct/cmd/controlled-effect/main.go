package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/internal/lab"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("controlled effect applied")
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("controlled-effect", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	requestPath := flags.String("request", "", "immutable controlled-effect request")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse controlled-effect flags: %w", err)
	}
	if *requestPath == "" || flags.NArg() != 0 {
		return errors.New("--request is required and positional arguments are not accepted")
	}
	request, err := lab.ReadControlledEffectRequest(*requestPath)
	if err != nil {
		return err
	}
	return lab.RunControlledEffect(ctx, request)
}
