// Package transport packages nested-Git Claude evidence as deterministic,
// Git-safe archives with independently verifiable manifests.
package transport

import (
	"errors"
	"io/fs"
)

const (
	SchemaVersion       = "claude-direct-evidence-transport-v1"
	IndexFile           = "transport-index.json"
	RawInventoryFile    = "raw-inventory.json"
	RawInventoryVersion = "claude-direct-raw-v1"
)

var (
	ErrInvalidTransport  = errors.New("invalid evidence transport")
	ErrDestinationExists = errors.New("transport destination exists")
)

type Disposition string

const (
	DispositionRejected   Disposition = "rejected"
	DispositionSuperseded Disposition = "superseded"
	DispositionAdmitted   Disposition = "admitted"
)

type BuildConfig struct {
	SourceRoot  string
	LineagePath string
	OutputRoot  string
}

type Lineage struct {
	SchemaVersion string         `json:"schema_version"`
	Entries       []LineageEntry `json:"entries"`
}

type LineageEntry struct {
	Bundle       string      `json:"bundle"`
	Disposition  Disposition `json:"disposition"`
	SupersededBy string      `json:"superseded_by,omitempty"`
	Reason       string      `json:"reason"`
}

type Index struct {
	SchemaVersion string         `json:"schema_version"`
	Lineage       []LineageEntry `json:"lineage"`
	Bundles       []BundleEntry  `json:"bundles"`
}

type BundleEntry struct {
	Bundle         string `json:"bundle"`
	Archive        string `json:"archive"`
	ArchiveSHA256  string `json:"archive_sha256"`
	Manifest       string `json:"manifest"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type BundleManifest struct {
	SchemaVersion string       `json:"schema_version"`
	Bundle        string       `json:"bundle"`
	Archive       string       `json:"archive"`
	ArchiveSHA256 string       `json:"archive_sha256"`
	FileCount     int          `json:"file_count"`
	TotalBytes    int64        `json:"total_bytes"`
	Files         []Artifact   `json:"files"`
	Runs          []RunBinding `json:"runs"`
}

type Artifact struct {
	Path   string      `json:"path"`
	Size   int64       `json:"size"`
	Mode   fs.FileMode `json:"mode"`
	SHA256 string      `json:"sha256"`
}

type RunBinding struct {
	RunID                      string `json:"run_id"`
	RawInventoryPath           string `json:"raw_inventory_path"`
	RawInventorySHA256         string `json:"raw_inventory_sha256"`
	DeclaredRawInventorySHA256 string `json:"declared_raw_inventory_sha256"`
	EffectiveInputPath         string `json:"effective_input_path"`
	EffectiveInputSHA256       string `json:"effective_input_sha256"`
	CommonManifestPath         string `json:"common_manifest_path"`
	CommonManifestSHA256       string `json:"common_manifest_sha256"`
	VerdictPath                string `json:"verdict_path"`
	VerdictSHA256              string `json:"verdict_sha256"`
}

type rawArtifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type rawInventory struct {
	Version string        `json:"version"`
	Files   []rawArtifact `json:"files"`
}

type effectiveInput struct {
	AdapterID           string            `json:"adapter_id,omitempty"`
	AdapterVersion      string            `json:"adapter_version,omitempty"`
	AgentProtocol       string            `json:"agent_protocol,omitempty"`
	AgentBinarySHA256   string            `json:"agent_binary_sha256,omitempty"`
	AuthorityProtocol   string            `json:"authority_protocol,omitempty"`
	DestinationProtocol string            `json:"destination_protocol,omitempty"`
	DestinationID       string            `json:"destination_id,omitempty"`
	FailureProtocol     string            `json:"failure_protocol,omitempty"`
	OracleProtocol      string            `json:"oracle_protocol,omitempty"`
	OracleVisibility    []string          `json:"oracle_visibility,omitempty"`
	Runtime             string            `json:"runtime,omitempty"`
	Settings            map[string]string `json:"settings"`
}

type commonManifest struct {
	ContractVersion      string            `json:"contract_version,omitempty"`
	RunID                string            `json:"run_id"`
	Case                 string            `json:"case,omitempty"`
	Probe                string            `json:"probe,omitempty"`
	Trial                int               `json:"trial,omitempty"`
	SessionID            string            `json:"session_id,omitempty"`
	EffectiveInputSHA256 string            `json:"effective_input_sha256"`
	EvidenceSHA256       map[string]string `json:"evidence_sha256,omitempty"`
}

type verdict struct {
	ContractVersion string         `json:"contract_version,omitempty"`
	RunID           string         `json:"run_id"`
	Case            string         `json:"case,omitempty"`
	Probe           string         `json:"probe,omitempty"`
	Trial           int            `json:"trial,omitempty"`
	Class           string         `json:"class,omitempty"`
	ReasonCodes     []string       `json:"reason_codes,omitempty"`
	Metrics         map[string]int `json:"metrics,omitempty"`
	Oracle          string         `json:"oracle,omitempty"`
}
