package publication

import (
	"fmt"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

type WorkKind string

const (
	WorkRequest         WorkKind = "request"
	WorkFault           WorkKind = "fault"
	WorkRecovery        WorkKind = "recovery"
	WorkRejectAdmission WorkKind = "reject-admission"
	WorkSilentProgress  WorkKind = "silent-progress"
	WorkLegitimateWait  WorkKind = "legitimate-wait"
	WorkReplaceSilent   WorkKind = "replace-silent"
)

type WorkSpec struct {
	ID            string              `json:"id"`
	Kind          WorkKind            `json:"kind"`
	Item          int                 `json:"item"`
	Ordinal       int                 `json:"ordinal,omitempty"`
	ParentID      string              `json:"parent_id,omitempty"`
	RetryLayer    protocol.RetryLayer `json:"retry_layer,omitempty"`
	RetryCause    string              `json:"retry_cause,omitempty"`
	Outcome       string              `json:"outcome,omitempty"`
	DelayMillis   int                 `json:"delay_ms,omitempty"`
	ServiceMillis int                 `json:"service_ms,omitempty"`
}

type WorkRound struct {
	Sequence int        `json:"sequence"`
	Work     []WorkSpec `json:"work"`
}

type EpisodeItem struct {
	Index  int    `json:"index"`
	Role   string `json:"role"`
	Poison bool   `json:"poison"`
}

type EpisodePlan struct {
	Case   protocol.CaseID `json:"case"`
	Probe  protocol.Probe  `json:"probe"`
	Trial  int             `json:"trial"`
	Items  []EpisodeItem   `json:"items"`
	Rounds []WorkRound     `json:"rounds"`
}

func BuildEpisodePlan(request EpisodeRequest) (EpisodePlan, error) {
	if !request.Case.Valid() || !request.Probe.Valid() || request.Slot < 1 {
		return EpisodePlan{}, invalid("episode plan input")
	}
	plan := EpisodePlan{Case: request.Case, Probe: request.Probe, Trial: request.Slot}
	switch request.Case {
	case protocol.CaseABAReacquisition:
		plan.Items = items(1, "authority", nil)
		plan.Rounds = requestRounds(plan.Items, "ok", 1)
	case protocol.CaseLayeredRetryAmplification:
		plan.Items = items(1, "retry-target", nil)
		attempts := 1
		if request.Probe == protocol.ProbeProtected {
			attempts = 4
		} else if request.Probe == protocol.ProbeUnsafe {
			attempts = 16
		}
		if attempts == 1 {
			plan.Rounds = requestRounds(plan.Items, "ok", 1)
			break
		}
		plan.Rounds = append(plan.Rounds, round(requestWork(1, 1, "accepted_then_timeout_script_activated", 1, 0)))
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkFault, 1)))
		for ordinal := 2; ordinal < attempts; ordinal++ {
			outcome := "500"
			if ordinal%3 == 0 {
				outcome = "429"
			} else if ordinal%2 == 0 {
				outcome = "timeout"
			}
			plan.Rounds = append(plan.Rounds, round(requestWork(1, ordinal, outcome, 1, 1)))
		}
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkRecovery, 1)))
		plan.Rounds = append(plan.Rounds, round(requestWork(1, attempts, "ok", 1, 1)))
	case protocol.CaseOutageBacklogRecovery:
		count := 8
		if request.Probe != protocol.ProbeUnfaulted {
			count = 12
		}
		plan.Items = items(count, "outage-cohort", nil)
		if request.Probe == protocol.ProbeUnfaulted {
			work := make([]WorkSpec, 0, count)
			for index := 1; index <= count; index++ {
				work = append(work, requestWork(index, 1, "ok", 1, (index-1)*3))
			}
			plan.Rounds = append(plan.Rounds, round(work...))
			break
		}
		plan.Rounds = append(plan.Rounds, round(requestWork(1, 1, "ok", 2, 0)))
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkFault, 2)))
		failed := make([]WorkSpec, 0, count-1)
		for index := 2; index <= count; index++ {
			failed = append(failed, requestWork(index, 1, "outage", 2, 0))
		}
		plan.Rounds = append(plan.Rounds, round(failed...))
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkRecovery, 2)))
		recovered := make([]WorkSpec, 0, count-1)
		for index := 2; index <= count; index++ {
			delay, service := 0, 100
			if request.Probe == protocol.ProbeProtected {
				delay, service = (index-2)*20+((index*request.Slot)%3), 2
			}
			recovered = append(recovered, requestWork(index, 2, "ok", service, delay))
		}
		plan.Rounds = append(plan.Rounds, round(recovered...))
	case protocol.CaseBackpressureOverload:
		count := 10
		if request.Probe != protocol.ProbeUnfaulted {
			count = 100
		}
		plan.Items = items(count, "offered-load", nil)
		admitted := count
		if request.Probe == protocol.ProbeProtected {
			admitted = 20
		}
		if request.Probe != protocol.ProbeUnfaulted {
			plan.Rounds = append(plan.Rounds, round(controlWork(WorkFault, 1)))
		}
		work := make([]WorkSpec, 0, count)
		for index := 1; index <= count; index++ {
			if index > admitted {
				work = append(work, controlItemWork(WorkRejectAdmission, index))
				continue
			}
			work = append(work, requestWork(index, 1, "ok", 2, 0))
		}
		plan.Rounds = append(plan.Rounds, round(work...))
		if request.Probe != protocol.ProbeUnfaulted {
			plan.Rounds = append(plan.Rounds, round(controlWork(WorkRecovery, 1)))
		}
	case protocol.CasePoisonWorkIsolation:
		plan.Items = items(10, "mixed-cohort", map[int]bool{1: true, 2: true})
		if request.Probe == protocol.ProbeUnfaulted {
			plan.Rounds = requestRounds(plan.Items, "ok", 1)
			break
		}
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkFault, 1)))
		plan.Rounds = append(plan.Rounds, round(
			requestWork(1, 1, "deterministic_failure", 1, 0),
			requestWork(2, 1, "deterministic_failure", 1, 0),
		))
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkRecovery, 3)))
		healthy := make([]WorkSpec, 0, 10)
		for index := 3; index <= 10; index++ {
			healthy = append(healthy, requestWork(index, 1, "ok", 1, 0))
		}
		healthy = append(healthy,
			requestWork(1, 2, "deterministic_failure", 1, 0),
			requestWork(2, 2, "deterministic_failure", 1, 0),
		)
		plan.Rounds = append(plan.Rounds, round(healthy...))
		attempts := 3
		if request.Probe == protocol.ProbeUnsafe {
			attempts = 8
		}
		for ordinal := 3; ordinal <= attempts; ordinal++ {
			plan.Rounds = append(plan.Rounds, round(
				requestWork(1, ordinal, "deterministic_failure", 1, 0),
				requestWork(2, ordinal, "deterministic_failure", 1, 0),
			))
		}
	case protocol.CaseSilentProgress:
		plan.Items = []EpisodeItem{{Index: 1, Role: "wedged-executor"}, {Index: 2, Role: "legitimate-wait"}}
		if request.Probe == protocol.ProbeUnfaulted {
			plan.Rounds = requestRounds(plan.Items, "ok", 1)
			break
		}
		plan.Rounds = append(plan.Rounds, round(
			controlItemWork(WorkSilentProgress, 1), controlItemWork(WorkLegitimateWait, 2),
		))
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkFault, 1)))
		if request.Probe == protocol.ProbeProtected {
			plan.Rounds = append(plan.Rounds, round(controlItemWork(WorkReplaceSilent, 1)))
		}
		plan.Rounds = append(plan.Rounds, round(controlWork(WorkRecovery, 1)))
		plan.Rounds = append(plan.Rounds, round(requestWork(2, 1, "ok", 1, 0)))
	}
	for index := range plan.Rounds {
		plan.Rounds[index].Sequence = index + 1
	}
	if err := plan.Validate(); err != nil {
		return EpisodePlan{}, err
	}
	return plan, nil
}

func (p EpisodePlan) Validate() error {
	if !p.Case.Valid() || !p.Probe.Valid() || p.Trial < 1 || len(p.Items) == 0 || len(p.Rounds) == 0 {
		return invalid("episode plan")
	}
	seenItems := make(map[int]bool, len(p.Items))
	for _, item := range p.Items {
		if item.Index < 1 || item.Role == "" || seenItems[item.Index] {
			return invalid("episode item")
		}
		seenItems[item.Index] = true
	}
	seenWork := make(map[string]bool)
	faults, recoveries := 0, 0
	for roundIndex, round := range p.Rounds {
		if round.Sequence != roundIndex+1 || len(round.Work) == 0 {
			return invalid("episode round")
		}
		for _, work := range round.Work {
			if work.ID == "" || seenWork[work.ID] || !seenItems[work.Item] || work.DelayMillis < 0 || work.ServiceMillis < 0 {
				return invalid("episode work")
			}
			seenWork[work.ID] = true
			if work.Kind == WorkRequest && (work.Ordinal < 1 || !work.RetryLayer.Valid() || work.Outcome == "") {
				return invalid("episode request")
			}
			if work.Ordinal == 1 && work.ParentID != "" || work.Ordinal > 1 && work.ParentID == "" {
				return invalid("episode retry lineage")
			}
			if work.Kind == WorkFault {
				faults++
			}
			if work.Kind == WorkRecovery {
				recoveries++
			}
		}
	}
	if p.Probe == protocol.ProbeUnfaulted && (faults != 0 || recoveries != 0) || p.Probe != protocol.ProbeUnfaulted && p.Case != protocol.CaseABAReacquisition && (faults != 1 || recoveries != 1) {
		return invalid("episode fault controls")
	}
	return nil
}

func items(count int, role string, poison map[int]bool) []EpisodeItem {
	result := make([]EpisodeItem, count)
	for index := range result {
		result[index] = EpisodeItem{Index: index + 1, Role: role, Poison: poison[index+1]}
	}
	return result
}

func requestRounds(input []EpisodeItem, outcome string, serviceMillis int) []WorkRound {
	work := make([]WorkSpec, 0, len(input))
	for _, item := range input {
		work = append(work, requestWork(item.Index, 1, outcome, serviceMillis, 0))
	}
	return []WorkRound{round(work...)}
}

func requestWork(item, ordinal int, outcome string, serviceMillis, delayMillis int) WorkSpec {
	parent := ""
	cause := ""
	if ordinal > 1 {
		parent = attemptID(item, ordinal-1)
		cause = "dependency_failure"
	}
	return WorkSpec{
		ID: fmt.Sprintf("request-%03d-%02d", item, ordinal), Kind: WorkRequest, Item: item, Ordinal: ordinal,
		ParentID: parent, RetryLayer: protocol.RetryLayerActivity, RetryCause: cause, Outcome: outcome,
		ServiceMillis: serviceMillis, DelayMillis: delayMillis,
	}
}

func controlWork(kind WorkKind, item int) WorkSpec { return controlItemWork(kind, item) }

func controlItemWork(kind WorkKind, item int) WorkSpec {
	return WorkSpec{ID: fmt.Sprintf("%s-%03d", kind, item), Kind: kind, Item: item}
}

func round(work ...WorkSpec) WorkRound { return WorkRound{Work: work} }

func attemptID(item, ordinal int) string { return fmt.Sprintf("attempt-%03d-%02d", item, ordinal) }

func serviceDuration(work WorkSpec) time.Duration {
	return time.Duration(work.ServiceMillis) * time.Millisecond
}
