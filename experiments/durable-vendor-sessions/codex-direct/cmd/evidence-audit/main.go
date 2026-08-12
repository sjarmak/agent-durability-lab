package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab"
	"golang.org/x/sys/unix"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, lab.AuditEvidence); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer,
	audit func(context.Context, string) (lab.EvidenceAudit, error),
) (returnErr error) {
	flags := flag.NewFlagSet("codex-evidence-audit", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	evidence := flags.String("evidence", "", "sealed Codex evidence root")
	output := flags.String("output", "", "new audit report outside the sealed evidence root")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *evidence == "" || *output == "" {
		return errors.New("audit requires --evidence and --output")
	}
	evidenceAbsolute, err := filepath.Abs(*evidence)
	if err != nil {
		return err
	}
	canonicalEvidence, err := filepath.EvalSymlinks(filepath.Clean(evidenceAbsolute))
	if err != nil {
		return fmt.Errorf("resolve sealed evidence root: %w", err)
	}
	outputAbsolute, err := filepath.Abs(*output)
	if err != nil {
		return err
	}
	outputDirectory, outputName, err := openValidatedOutputDirectory(canonicalEvidence, filepath.Clean(outputAbsolute))
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, outputDirectory.Close()) }()
	report, err := audit(ctx, evidenceAbsolute)
	if err != nil {
		return err
	}
	fd, err := unix.Openat(int(outputDirectory.Fd()), outputName,
		unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), outputName)
	if file == nil {
		_ = unix.Close(fd)
		return errors.New("create audit output returned an invalid descriptor")
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(report)
}

func openValidatedOutputDirectory(canonicalEvidence, output string) (*os.File, string, error) {
	resolvedOutput, err := resolveThroughExistingAncestor(output)
	if err != nil {
		return nil, "", fmt.Errorf("resolve audit output: %w", err)
	}
	if pathWithin(canonicalEvidence, resolvedOutput) {
		return nil, "", errors.New("audit output must be outside the sealed evidence root")
	}
	name := filepath.Base(resolvedOutput)
	directoryPath := filepath.Dir(resolvedOutput)
	fd, err := unix.Openat2(unix.AT_FDCWD, directoryPath, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return nil, "", fmt.Errorf("open audit output directory: %w", err)
	}
	directory := os.NewFile(uintptr(fd), directoryPath)
	if directory == nil {
		_ = unix.Close(fd)
		return nil, "", errors.New("open audit output directory returned an invalid descriptor")
	}
	pinnedPath, err := filepath.EvalSymlinks("/proc/self/fd/" + fmt.Sprint(fd))
	if err != nil || pathWithin(canonicalEvidence, filepath.Join(pinnedPath, name)) {
		_ = directory.Close()
		if err != nil {
			return nil, "", fmt.Errorf("resolve pinned audit output directory: %w", err)
		}
		return nil, "", errors.New("audit output must be outside the sealed evidence root")
	}
	return directory, name, nil
}

func resolveThroughExistingAncestor(path string) (string, error) {
	current := path
	var missing []string
	for {
		_, err := os.Lstat(current)
		if err == nil {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", err
			}
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func startsWithParent(path string) bool {
	separator := string(filepath.Separator)
	return path == ".." || len(path) > 3 && path[:3] == ".."+separator
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(candidate))
	return err == nil && (relative == "." || relative != ".." && !filepath.IsAbs(relative) && !startsWithParent(relative))
}
