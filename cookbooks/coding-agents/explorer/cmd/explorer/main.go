package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/explorer"
)

type config struct {
	listenAddress string
	repository    string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, arguments []string, output, errorOutput io.Writer) int {
	configuration, err := parseConfig(arguments, errorOutput)
	if err != nil {
		fmt.Fprintf(errorOutput, "evidence explorer: %v\n", err)
		return 2
	}
	repository, err := explorer.OpenRepository(configuration.repository)
	if err != nil {
		fmt.Fprintf(errorOutput, "evidence explorer: %v\n", err)
		return 1
	}
	defer repository.Close()
	handler, err := explorer.NewHandler(repository)
	if err != nil {
		fmt.Fprintf(errorOutput, "evidence explorer: %v\n", err)
		return 1
	}
	server := &http.Server{
		Addr:              configuration.listenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
	err = serve(ctx, server, output)
	if err != nil {
		fmt.Fprintf(errorOutput, "evidence explorer: %v\n", err)
		return 1
	}
	return 0
}

func parseConfig(arguments []string, errorOutput io.Writer) (config, error) {
	flags := flag.NewFlagSet("evidence-explorer", flag.ContinueOnError)
	flags.SetOutput(errorOutput)
	configuration := config{}
	flags.StringVar(&configuration.listenAddress, "listen", "127.0.0.1:8080", "literal loopback listen address")
	flags.StringVar(&configuration.repository, "repository", ".", "Agent Durability Lab repository root")
	if err := flags.Parse(arguments); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, errors.New("positional arguments are not accepted")
	}
	if err := explorer.ValidateListenAddress(configuration.listenAddress); err != nil {
		return config{}, err
	}
	if configuration.repository == "" {
		return config{}, errors.New("repository root is required")
	}
	return configuration, nil
}

func serve(ctx context.Context, server *http.Server, output io.Writer) error {
	result := make(chan error, 1)
	go func() {
		fmt.Fprintf(output, "Recovery evidence explorer: http://%s\n", server.Addr)
		result <- server.ListenAndServe()
	}()
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownContext)
		serveErr := <-result
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}
