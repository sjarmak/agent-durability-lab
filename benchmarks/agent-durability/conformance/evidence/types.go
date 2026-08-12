package evidence

import legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"

const (
	DevelopmentTrials            = 3
	ContractVersion              = "coding-agent-durability-v1"
	CalibrationProfileKind       = "deterministic-calibration-apparatus-v1"
	CalibrationClaimBoundary     = "Validates the deterministic conformance apparatus only; it captures no Temporal Event History and makes no product-binding or live-recovery claim."
	CalibrationReplayExplanation = "Deterministic calibration emits a native journal, not a Temporal Event History; replay is not applicable and no recovery guarantee is claimed."
	ExecutableArtifactPath       = "inputs/executable/coding-agent-conformance"
	ProtocolSchemaManifestSHA256 = "2e0d0add405f4f3b65dc016006cd26cbf707ed057d4f13ebb098649659811e03"
	ReportFile                   = "conformance-report.json"
)

type Status string

const (
	StatusConformant    Status = "conformant"
	StatusNonconformant Status = "nonconformant"
)

type ReplayStatus string

const (
	ReplayPassed        ReplayStatus = "passed"
	ReplayNotApplicable ReplayStatus = "not_applicable"
)

type ReplayDisposition struct {
	Captured    bool         `json:"captured"`
	Status      ReplayStatus `json:"status"`
	Explanation string       `json:"explanation,omitempty"`
	HistoryPath string       `json:"history_path,omitempty"`
	HistoryHash string       `json:"history_hash,omitempty"`
}

type Pin struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type Pins struct {
	Executable          Pin    `json:"executable"`
	Sources             []Pin  `json:"sources"`
	SchemaManifest      Pin    `json:"schema_manifest"`
	Schemas             []Pin  `json:"schemas"`
	ConfigurationSHA256 string `json:"configuration_sha256"`
}

type EpisodeReference struct {
	RunID       string                      `json:"run_id"`
	Path        string                      `json:"path"`
	Case        legacyprotocol.CaseID       `json:"case"`
	Probe       legacyprotocol.Probe        `json:"probe"`
	Trial       int                         `json:"trial"`
	Verdict     legacyprotocol.VerdictClass `json:"verdict"`
	ReasonCodes []string                    `json:"reason_codes,omitempty"`
	Replay      ReplayDisposition           `json:"replay"`
}

type InvalidControlReference struct {
	ID             string                      `json:"id"`
	Path           string                      `json:"path"`
	ExpectedReason string                      `json:"expected_reason"`
	Verdict        legacyprotocol.VerdictClass `json:"verdict"`
	ReasonCodes    []string                    `json:"reason_codes"`
}

type Report struct {
	ContractVersion string                    `json:"contract_version"`
	ProfileKind     string                    `json:"profile_kind"`
	Status          Status                    `json:"status"`
	ClaimBoundary   string                    `json:"claim_boundary"`
	Pins            Pins                      `json:"pins"`
	Episodes        []EpisodeReference        `json:"episodes"`
	InvalidControls []InvalidControlReference `json:"invalid_controls"`
}
