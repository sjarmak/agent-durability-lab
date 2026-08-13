package lab

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

const maxExecutableBytes = 512 << 20

func CaptureRuntimeProvenance(ctx context.Context, temporalPath, workerPath string) (RuntimeProvenance, error) {
	temporalDigest, temporalBytes, err := executableIdentity(temporalPath)
	if err != nil {
		return RuntimeProvenance{}, fmt.Errorf("identify Temporal executable: %w", err)
	}
	workerDigest, workerBytes, err := executableIdentity(workerPath)
	if err != nil {
		return RuntimeProvenance{}, fmt.Errorf("identify Worker executable: %w", err)
	}
	workerBuild, err := buildinfo.ReadFile(workerPath)
	if err != nil {
		return RuntimeProvenance{}, fmt.Errorf("read Worker build metadata: %w", err)
	}
	sdkVersion := dependencyVersion(workerBuild, "go.temporal.io/sdk")
	if sdkVersion == "" {
		return RuntimeProvenance{}, errors.New("worker build lacks Temporal SDK provenance")
	}
	versionContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	command := exec.CommandContext(versionContext, temporalPath, "--version")
	command.Env = []string{"PATH=/usr/bin:/bin", "TZ=UTC"}
	versionOutput, err := command.Output()
	if err != nil {
		return RuntimeProvenance{}, fmt.Errorf("read Temporal version: %w", err)
	}
	version := strings.TrimSpace(string(versionOutput))
	if version == "" || len(version) > 512 {
		return RuntimeProvenance{}, errors.New("temporal version is empty or oversized")
	}
	provenance := RuntimeProvenance{
		CapturedAt: time.Now().UTC(), OS: runtime.GOOS, Architecture: runtime.GOARCH,
		GoVersion: runtime.Version(), SDKVersion: sdkVersion,
		TemporalVersion: version, TemporalSHA256: temporalDigest, TemporalBytes: temporalBytes,
		WorkerSHA256: workerDigest, WorkerBytes: workerBytes,
	}
	if err := ValidateCurrentRuntimeProvenance(provenance); err != nil {
		return RuntimeProvenance{}, err
	}
	return provenance, nil
}

func executableIdentity(path string) (digest string, size int64, returnErr error) {
	before, err := os.Lstat(path)
	if err != nil {
		return "", 0, err
	}
	if !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > maxExecutableBytes {
		return "", 0, errors.New("executable is not a bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return "", 0, errors.New("executable changed before open")
	}
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, maxExecutableBytes+1))
	if err != nil || written != opened.Size() || written > maxExecutableBytes {
		return "", 0, errors.New("executable changed or exceeds bound")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(opened, after) || after.Size() != written || !after.ModTime().Equal(opened.ModTime()) {
		return "", 0, errors.New("executable changed while hashing")
	}
	return hex.EncodeToString(hash.Sum(nil)), written, nil
}

func dependencyVersion(build *debug.BuildInfo, path string) string {
	for _, dependency := range build.Deps {
		if dependency.Path == path {
			return dependency.Version
		}
	}
	return ""
}

func validateRuntimeProvenance(value RuntimeProvenance) error {
	for _, digest := range []string{value.TemporalSHA256, value.WorkerSHA256} {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
			return errors.New("runtime provenance has an invalid executable digest")
		}
	}
	if value.CapturedAt.IsZero() || value.CapturedAt.Location() != time.UTC || value.OS == "" ||
		value.Architecture == "" || value.GoVersion == "" || value.SDKVersion == "" || value.SDKVersion == "unknown" ||
		value.TemporalVersion == "" || value.TemporalBytes < 1 || value.WorkerBytes < 1 {
		return errors.New("runtime provenance is incomplete")
	}
	return nil
}
