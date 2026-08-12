// Package transport packages nested-Git Codex evidence as deterministic,
// Git-safe archives with independently verifiable manifests.
package transport

import (
	"errors"
	"io/fs"
)

const (
	SchemaVersion         = "codex-direct-evidence-transport-v1"
	LineageVersion        = "codex-direct-evidence-lineage-v1"
	RawInventoryVersion   = "codex-direct-raw-v1"
	rejectionAuditVersion = "codex-direct-rejection-audit-v1"
	IndexFile             = "transport-index.json"
)

var (
	ErrInvalidTransport  = errors.New("invalid Codex evidence transport")
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
	Audit        string      `json:"audit"`
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
	Audit          string `json:"audit"`
	AuditSHA256    string `json:"audit_sha256"`
}

type BundleManifest struct {
	SchemaVersion string       `json:"schema_version"`
	Bundle        string       `json:"bundle"`
	Disposition   Disposition  `json:"disposition"`
	Archive       string       `json:"archive"`
	ArchiveSHA256 string       `json:"archive_sha256"`
	Audit         string       `json:"audit"`
	AuditSHA256   string       `json:"audit_sha256"`
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
	RunID              string `json:"run_id"`
	RawInventoryPath   string `json:"raw_inventory_path"`
	RawInventorySHA256 string `json:"raw_inventory_sha256"`
	SummaryPath        string `json:"summary_path"`
	SummarySHA256      string `json:"summary_sha256"`
	HistoryPath        string `json:"history_path"`
	HistorySHA256      string `json:"history_sha256"`
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

type suiteSummary struct {
	EvidenceRoot   string   `json:"evidence_root"`
	RunDirectories []string `json:"run_directories"`
}

type trialSummary struct {
	SchemaVersion    string `json:"schema_version"`
	Mode             string `json:"mode"`
	FaultBoundary    string `json:"fault_boundary"`
	Trial            int    `json:"trial"`
	LogicalSessionID string `json:"logical_session_id"`
}

type auditReport struct {
	Version                 string `json:"version"`
	EvidenceRoot            string `json:"evidence_root"`
	Runs                    int    `json:"runs"`
	AllRequirementsVerified bool   `json:"all_requirements_verified"`
}

type rejectionAuditReport struct {
	Version          string `json:"version"`
	EvidenceRoot     string `json:"evidence_root"`
	FailureSHA256    string `json:"failure_sha256"`
	FailurePreserved bool   `json:"failure_preserved"`
}

type failureRecord struct {
	RecordedAt string `json:"recorded_at"`
	Error      string `json:"error"`
	Preserved  bool   `json:"preserved"`
}
