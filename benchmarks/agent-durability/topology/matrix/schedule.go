// Package matrix integrates the frozen topology schedule with deterministic
// apparatus fixtures and real Temporal sentinels. Its outputs are explicitly
// excluded from the pilot and publication populations.
package matrix

import (
	"fmt"
	"reflect"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

type ScheduleAudit struct {
	Strata                 int `json:"strata"`
	ScheduleBlocks         int `json:"schedule_blocks"`
	PrimaryPairs           int `json:"primary_pairs"`
	ReservePairs           int `json:"reserve_pairs"`
	PrimaryArmExecutions   int `json:"primary_arm_executions"`
	PrimaryDirectFirst     int `json:"primary_direct_first"`
	PrimaryChildFirst      int `json:"primary_child_first"`
	ReserveDirectFirst     int `json:"reserve_direct_first"`
	ReserveChildFirst      int `json:"reserve_child_first"`
	PrimaryPairsPerStratum int `json:"primary_pairs_per_stratum"`
	ReservePairsPerStratum int `json:"reserve_pairs_per_stratum"`
}

func AuditSchedule(registration protocol.Preregistration, schedule protocol.Schedule) (ScheduleAudit, []protocol.PairBlock, error) {
	if err := protocol.ValidateSchedule(registration, protocol.PhasePublication, schedule); err != nil {
		return ScheduleAudit{}, nil, err
	}
	wantStrata, err := protocol.BuildStrata(registration)
	if err != nil {
		return ScheduleAudit{}, nil, err
	}
	wantByID := make(map[string]protocol.Stratum, len(wantStrata))
	for _, stratum := range wantStrata {
		wantByID[stratum.ID] = stratum
	}
	type stratumCounts struct {
		primary, reserve            int
		directFirst, childFirst     int
		reserveDirect, reserveChild int
		slots                       map[int]bool
		selected                    *protocol.PairBlock
	}
	counts := make(map[string]*stratumCounts, len(wantStrata))
	seenBlocks := make(map[string]bool, len(schedule.Blocks))
	audit := ScheduleAudit{Strata: len(wantStrata), ScheduleBlocks: len(schedule.Blocks)}
	for index, block := range schedule.Blocks {
		want, exists := wantByID[block.Stratum.ID]
		if !exists || !reflect.DeepEqual(want, block.Stratum) || block.Index != index+1 || seenBlocks[block.ScheduleBlockID] {
			return ScheduleAudit{}, nil, fmt.Errorf("%w: schedule inventory identity", protocol.ErrInvalidEvidence)
		}
		seenBlocks[block.ScheduleBlockID] = true
		entry := counts[block.Stratum.ID]
		if entry == nil {
			entry = &stratumCounts{slots: make(map[int]bool)}
			counts[block.Stratum.ID] = entry
		}
		if entry.slots[block.Slot] || block.Slot > registration.Population.MaximumAttemptedPairsPerStratum {
			return ScheduleAudit{}, nil, fmt.Errorf("%w: duplicate or out-of-range stratum slot", protocol.ErrInvalidEvidence)
		}
		entry.slots[block.Slot] = true
		if block.Reserve {
			entry.reserve++
			audit.ReservePairs++
			if block.TopologyOrder[0] == protocol.TopologyDirectActivity {
				entry.reserveDirect++
				audit.ReserveDirectFirst++
			} else {
				entry.reserveChild++
				audit.ReserveChildFirst++
			}
		} else {
			entry.primary++
			audit.PrimaryPairs++
			if block.TopologyOrder[0] == protocol.TopologyDirectActivity {
				entry.directFirst++
				audit.PrimaryDirectFirst++
			} else {
				entry.childFirst++
				audit.PrimaryChildFirst++
			}
		}
		if block.Slot == 1 {
			copyBlock := block
			entry.selected = &copyBlock
		}
	}
	selected := make([]protocol.PairBlock, 0, len(wantStrata))
	wantPrimary := registration.Population.MinimumValidPairsPerStratum
	wantReserve := registration.Population.MaximumAttemptedPairsPerStratum - wantPrimary
	for _, stratum := range wantStrata {
		entry := counts[stratum.ID]
		if entry == nil || entry.primary != wantPrimary || entry.reserve != wantReserve || len(entry.slots) != wantPrimary+wantReserve ||
			entry.directFirst != wantPrimary/2 || entry.childFirst != wantPrimary/2 ||
			entry.reserveDirect != wantReserve/2 || entry.reserveChild != wantReserve/2 || entry.selected == nil || entry.selected.Reserve {
			return ScheduleAudit{}, nil, fmt.Errorf("%w: per-stratum schedule arithmetic", protocol.ErrInvalidEvidence)
		}
		selected = append(selected, *entry.selected)
	}
	audit.PrimaryArmExecutions = audit.PrimaryPairs * len(protocol.Topologies())
	audit.PrimaryPairsPerStratum = wantPrimary
	audit.ReservePairsPerStratum = wantReserve
	if audit.Strata != registration.Population.ExpectedStrata ||
		audit.PrimaryPairs != registration.Population.ExpectedPrimaryValidPairs ||
		audit.PrimaryArmExecutions != registration.Population.ExpectedPrimaryArmExecutions ||
		audit.ScheduleBlocks != registration.Population.ExpectedStrata*registration.Population.MaximumAttemptedPairsPerStratum {
		return ScheduleAudit{}, nil, fmt.Errorf("%w: global schedule arithmetic", protocol.ErrInvalidEvidence)
	}
	return audit, selected, nil
}
