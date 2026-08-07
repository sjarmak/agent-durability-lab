package workstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

var (
	ErrInvalidRequest         = errors.New("invalid work store request")
	ErrSessionNotFound        = errors.New("session not found")
	ErrStaleOwner             = errors.New("stale owner")
	ErrExecutorNotRunning     = errors.New("executor not running")
	ErrOutcomeAlreadyAccepted = errors.New("outcome already accepted")
)

type Mode string

const (
	ModeUnsafe   Mode = "unsafe"
	ModeReattach Mode = "reattach"
	ModeFenced   Mode = "fenced"
)

func (m Mode) Valid() bool {
	return m == ModeUnsafe || m == ModeReattach || m == ModeFenced
}

type Action string

const (
	ActionLaunch   Action = "launch"
	ActionAttach   Action = "attach"
	ActionComplete Action = "complete"
)

type Lease struct {
	SessionID  string `json:"session_id"`
	Generation uint64 `json:"generation"`
	OwnerToken string `json:"owner_token"`
}

type StartRequest struct {
	SessionID            string `json:"session_id"`
	Mode                 Mode   `json:"mode"`
	CandidateOwner       string `json:"candidate_owner"`
	WorkerID             string `json:"worker_id"`
	AgentBuild           string `json:"agent_build,omitempty"`
	Attempt              int32  `json:"attempt"`
	ReplaceOwner         bool   `json:"replace"`
	ReplacePendingLaunch bool   `json:"replace_pending"`
}

type Decision struct {
	Action  Action   `json:"action"`
	Lease   Lease    `json:"lease"`
	Outcome *Outcome `json:"outcome,omitempty"`
}

type Outcome struct {
	Value       string `json:"value"`
	ArtifactRef string `json:"artifact_ref,omitempty"`
}

type Effect struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type AcceptedEffect struct {
	Effect
	Generation     uint64    `json:"generation"`
	OwnerTokenHash string    `json:"owner_token_hash"`
	AcceptedAt     time.Time `json:"accepted_at"`
}

type Executor struct {
	Generation     uint64    `json:"generation"`
	OwnerTokenHash string    `json:"owner_token_hash"`
	WorkerID       string    `json:"worker_id"`
	AgentBuild     string    `json:"agent_build,omitempty"`
	Attempt        int32     `json:"attempt"`
	PID            int       `json:"pid,omitempty"`
	ProcessStart   string    `json:"process_start,omitempty"`
	Status         string    `json:"status"`
	StartedAt      time.Time `json:"started_at"`
}

const (
	ExecutorStatusLaunchPending     = "launch_pending"
	ExecutorStatusRunning           = "running"
	ExecutorStatusSuperseded        = "superseded"
	ExecutorStatusCompleted         = "completed"
	ExecutorStatusTerminalRejected  = "terminal_rejected"
	ExecutorStatusTerminalDuplicate = "terminal_duplicate"
)

type Event struct {
	Sequence       uint64            `json:"sequence"`
	Time           time.Time         `json:"time"`
	Kind           string            `json:"kind"`
	SessionID      string            `json:"session_id"`
	Generation     uint64            `json:"generation,omitempty"`
	OwnerTokenHash string            `json:"owner_token_hash,omitempty"`
	WorkerID       string            `json:"worker_id,omitempty"`
	Attempt        int32             `json:"attempt,omitempty"`
	PID            int               `json:"pid,omitempty"`
	Details        map[string]string `json:"details,omitempty"`
}

type Snapshot struct {
	SessionID            string           `json:"session_id"`
	Mode                 Mode             `json:"mode"`
	ActiveGeneration     uint64           `json:"active_generation"`
	ActiveOwnerTokenHash string           `json:"active_owner_token_hash"`
	Executors            []Executor       `json:"executors"`
	Effects              []AcceptedEffect `json:"effects"`
	Outcome              *Outcome         `json:"outcome,omitempty"`
	Events               []Event          `json:"events"`
}

type Process struct {
	PID           int
	StartIdentity string
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

type sessionRecord struct {
	SessionID   string           `json:"session_id"`
	Mode        Mode             `json:"mode"`
	ActiveLease Lease            `json:"active_lease"`
	Executors   []executorRecord `json:"executors"`
	Effects     []AcceptedEffect `json:"effects"`
	Outcome     *Outcome         `json:"outcome,omitempty"`
}

type executorRecord struct {
	Lease
	WorkerID     string    `json:"worker_id"`
	AgentBuild   string    `json:"agent_build,omitempty"`
	Attempt      int32     `json:"attempt"`
	PID          int       `json:"pid,omitempty"`
	ProcessStart string    `json:"process_start,omitempty"`
	Status       string    `json:"status"`
	StartedAt    time.Time `json:"started_at"`
}
