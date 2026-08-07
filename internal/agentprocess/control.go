package agentprocess

import (
	"errors"
	"time"
)

var (
	ErrInvalidControlRequest     = errors.New("invalid process control request")
	ErrProcessIdentityMismatch   = errors.New("process identity mismatch")
	ErrProcessGone               = errors.New("process is gone")
	ErrProcessControlUnsupported = errors.New("process control is unsupported")
)

type ProcessIdentity struct {
	PID            int    `json:"pid"`
	StartIdentity  string `json:"start_identity"`
	ProcessGroupID int    `json:"process_group_id"`
}

type ControlTarget struct {
	SessionID      string            `json:"session_id"`
	Generation     uint64            `json:"generation"`
	OwnerTokenHash string            `json:"owner_token_hash"`
	Leader         ProcessIdentity   `json:"leader"`
	Members        []ProcessIdentity `json:"members,omitempty"`
}

type Scope string

const (
	ScopeLeader      Scope = "leader"
	ScopeProcessTree Scope = "process_tree"
)

type ControlSignal string

const (
	SignalTerminate ControlSignal = "terminate"
	SignalKill      ControlSignal = "kill"
	SignalStop      ControlSignal = "stop"
	SignalContinue  ControlSignal = "continue"
)

type SignalMethod string

const (
	MethodPIDFDLeader          SignalMethod = "pidfd_leader"
	MethodProcessGroupAndPIDFD SignalMethod = "process_group_and_pidfd"
)

type Disposition string

const (
	DispositionAlive  Disposition = "alive"
	DispositionGone   Disposition = "gone"
	DispositionReused Disposition = "reused"
)

type ControlRequest struct {
	Target ControlTarget `json:"target"`
	Scope  Scope         `json:"scope"`
	Signal ControlSignal `json:"signal"`
}

type ControlResult struct {
	Target      ControlTarget `json:"target"`
	Scope       Scope         `json:"scope"`
	Signal      ControlSignal `json:"signal"`
	Method      SignalMethod  `json:"method"`
	RequestedAt time.Time     `json:"requested_at"`
}
