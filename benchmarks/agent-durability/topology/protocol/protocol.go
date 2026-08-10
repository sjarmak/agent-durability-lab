// Package protocol defines the frozen, topology-neutral contract used by both
// Temporal orchestration arms. It intentionally does not extend the earlier
// cross-system protocols in place.
package protocol

import (
	"errors"
	"fmt"
	"slices"
)

const (
	ContractVersion            = "adl.temporal-topology.v1"
	PublicationProtocolVersion = "adl.temporal-topology.publication.v1"
	ScheduleAlgorithm          = "balanced-splitmix64-fisher-yates-v1-namespaced-ids"
)

var (
	ErrInvalidEvidence = errors.New("invalid topology benchmark evidence")
	ErrEvidenceExists  = errors.New("topology benchmark evidence already exists")
)

type Phase string

const (
	PhasePilot       Phase = "pilot"
	PhasePublication Phase = "publication"
)

func (p Phase) Valid() bool { return p == PhasePilot || p == PhasePublication }

type Topology string

const (
	TopologyDirectActivity Topology = "direct-activity"
	TopologyChildWorkflow  Topology = "child-workflow"
)

func Topologies() []Topology {
	return []Topology{TopologyDirectActivity, TopologyChildWorkflow}
}

func (t Topology) Valid() bool { return slices.Contains(Topologies(), t) }

type Probe string

const (
	ProbeUnfaulted Probe = "unfaulted"
	ProbeUnsafe    Probe = "unsafe"
	ProbeProtected Probe = "protected"
)

func Probes() []Probe { return []Probe{ProbeUnfaulted, ProbeUnsafe, ProbeProtected} }

func (p Probe) Valid() bool { return slices.Contains(Probes(), p) }

type SuiteID string

const (
	SuiteOrchestrationSemantics SuiteID = "orchestration-semantics"
	SuiteRecoveryDynamics       SuiteID = "recovery-dynamics"
)

type CaseID string

const (
	CaseJoinBarrier                 CaseID = "join-barrier"
	CaseIncrementalPartialReduction CaseID = "incremental-partial-reduction"
	CaseQueuedExecutingSupersession CaseID = "queued-executing-supersession"
	CaseDestructiveTransition       CaseID = "destructive-transition"
	CaseCrashRecoveryBoundaries     CaseID = "crash-recovery-boundaries"
	CaseLayeredRetryAmplification   CaseID = "layered-retry-amplification"
	CaseOutageBacklogHerdRecovery   CaseID = "outage-backlog-herd-recovery"
	CaseBackpressureOverload        CaseID = "backpressure-overload"
	CasePoisonWorkIsolation         CaseID = "poison-work-isolation"
	CaseSilentProgress              CaseID = "silent-progress"
)

func Cases() []CaseID {
	return []CaseID{
		CaseJoinBarrier,
		CaseIncrementalPartialReduction,
		CaseQueuedExecutingSupersession,
		CaseDestructiveTransition,
		CaseCrashRecoveryBoundaries,
		CaseLayeredRetryAmplification,
		CaseOutageBacklogHerdRecovery,
		CaseBackpressureOverload,
		CasePoisonWorkIsolation,
		CaseSilentProgress,
	}
}

func (c CaseID) Valid() bool { return slices.Contains(Cases(), c) }

func (c CaseID) Suite() SuiteID {
	if !c.Valid() {
		return ""
	}
	if slices.Contains(Cases()[:4], c) {
		return SuiteOrchestrationSemantics
	}
	return SuiteRecoveryDynamics
}

func invalid(subject string) error {
	return fmt.Errorf("%w: %s", ErrInvalidEvidence, subject)
}
