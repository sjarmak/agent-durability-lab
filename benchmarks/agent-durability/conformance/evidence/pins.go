package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

var schemaFiles = []string{
	"event.schema.json",
	"evidence.schema.json",
	"identity.schema.json",
	"transition.schema.json",
}

var sourceFiles = []string{
	"benchmarks/agent-durability/contract-v1.json",
	"benchmarks/agent-durability/calibration/run.go",
}

const configuration = "profile=deterministic-calibration-apparatus-v1;development_trials=3;cases=surviving-executor,ambiguous-effect,stale-generation,cancellation-unreachable;probes=unfaulted,unsafe,protected"

type schemaManifest struct {
	SchemaVersion string            `json:"schema_version"`
	Files         map[string]string `json:"files"`
}

func SchemaFiles() []string {
	return slices.Clone(schemaFiles)
}

func CapturePins(executablePath, sourceRoot, schemaRoot string) (Pins, error) {
	if executablePath == "" || sourceRoot == "" || schemaRoot == "" {
		return Pins{}, fmt.Errorf("executable, source root, and schema root are required")
	}
	executableData, err := readRegularFile(executablePath)
	if err != nil {
		return Pins{}, fmt.Errorf("hash executable: %w", err)
	}
	executableDigest := digestBytes(executableData)
	pins := Pins{Executable: Pin{Path: ExecutableArtifactPath, SHA256: executableDigest}}
	for _, name := range sourceFiles {
		path, err := confinedPath(sourceRoot, name)
		if err != nil {
			return Pins{}, err
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return Pins{}, fmt.Errorf("hash source %s: %w", name, err)
		}
		pins.Sources = append(pins.Sources, Pin{Path: filepath.ToSlash(name), SHA256: digest})
	}
	manifestPath, err := confinedPath(schemaRoot, "schema-manifest.json")
	if err != nil {
		return Pins{}, err
	}
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return Pins{}, fmt.Errorf("read schema manifest: %w", err)
	}
	if digestBytes(manifestData) != ProtocolSchemaManifestSHA256 {
		return Pins{}, fmt.Errorf("schema manifest is not the canonical coding-agent durability v1 inventory")
	}
	var manifest schemaManifest
	if err := DecodeJSONStrict(manifestData, &manifest); err != nil {
		return Pins{}, fmt.Errorf("decode schema manifest: %w", err)
	}
	if manifest.SchemaVersion != "1.0.0" || len(manifest.Files) != len(schemaFiles) {
		return Pins{}, fmt.Errorf("schema manifest has unsupported version or inventory")
	}
	pins.SchemaManifest = Pin{Path: "schema-manifest.json", SHA256: ProtocolSchemaManifestSHA256}
	for _, name := range schemaFiles {
		expected, found := manifest.Files[name]
		if !found {
			return Pins{}, fmt.Errorf("schema manifest lacks %s", name)
		}
		path, err := confinedPath(schemaRoot, name)
		if err != nil {
			return Pins{}, err
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return Pins{}, fmt.Errorf("hash schema %s: %w", name, err)
		}
		if expected != "sha256:"+digest {
			return Pins{}, fmt.Errorf("schema %s differs from manifest", name)
		}
		pins.Schemas = append(pins.Schemas, Pin{Path: name, SHA256: digest})
	}
	configDigest := sha256.Sum256([]byte(configuration))
	pins.ConfigurationSHA256 = hex.EncodeToString(configDigest[:])
	return pins, nil
}

func confinedPath(root, relative string) (string, error) {
	if filepath.IsAbs(relative) || relative == "" {
		return "", fmt.Errorf("path %q is not a confined relative path", relative)
	}
	clean := filepath.Clean(relative)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || strings.Contains(relative, `\`) {
		return "", fmt.Errorf("path %q escapes its root", relative)
	}
	rootPath, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve root: %w", err)
	}
	path := filepath.Join(rootPath, clean)
	realRoot, err := filepath.EvalSymlinks(rootPath)
	if err != nil {
		return "", fmt.Errorf("resolve root symlinks: %w", err)
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve %s symlinks: %w", relative, err)
	}
	rel, err := filepath.Rel(realRoot, realPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q resolves outside its root", relative)
	}
	return realPath, nil
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return digestBytes(data), nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func ValidatePins(pins Pins) error {
	all := append([]Pin{pins.Executable, pins.SchemaManifest}, pins.Sources...)
	all = append(all, pins.Schemas...)
	if len(pins.Sources) != len(sourceFiles) || len(pins.Schemas) != len(schemaFiles) || len(pins.ConfigurationSHA256) != sha256.Size*2 {
		return fmt.Errorf("pin inventory is incomplete")
	}
	paths := make([]string, 0, len(all))
	for _, pin := range all {
		if pin.Path == "" || len(pin.SHA256) != sha256.Size*2 {
			return fmt.Errorf("pin is incomplete")
		}
		if _, err := hex.DecodeString(pin.SHA256); err != nil {
			return fmt.Errorf("pin %s is not a SHA-256 digest: %w", pin.Path, err)
		}
		paths = append(paths, pin.Path)
	}
	sort.Strings(paths)
	for index := 1; index < len(paths); index++ {
		if paths[index] == paths[index-1] {
			return fmt.Errorf("duplicate pinned path %s", paths[index])
		}
	}
	return nil
}
