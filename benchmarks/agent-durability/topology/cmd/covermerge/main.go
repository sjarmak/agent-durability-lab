package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/coverprofile"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("covermerge", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	outputPath := flags.String("output", "", "merged coverage profile path")
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("parse covermerge arguments: %w", err)
	}
	if *outputPath == "" {
		return fmt.Errorf("merge coverage profiles: --output is required")
	}
	if flags.NArg() == 0 {
		return fmt.Errorf("merge coverage profiles: at least one input is required")
	}
	profiles := make([]coverprofile.NamedReader, 0, flags.NArg())
	files := make([]*os.File, 0, flags.NArg())
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	for _, path := range flags.Args() {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open coverage profile %q: %w", path, err)
		}
		files = append(files, file)
		profiles = append(profiles, coverprofile.NamedReader{Name: path, Reader: file})
	}
	temporary, err := os.CreateTemp(filepath.Dir(*outputPath), ".coverprofile-*")
	if err != nil {
		return fmt.Errorf("create merged coverage profile: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := coverprofile.Merge(temporary, profiles); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync merged coverage profile: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close merged coverage profile: %w", err)
	}
	if err := os.Rename(temporaryPath, *outputPath); err != nil {
		return fmt.Errorf("install merged coverage profile: %w", err)
	}
	committed = true
	return nil
}
