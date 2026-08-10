package protocol

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

const UnfaultedBoundary = "unfaulted-baseline"

type Stratum struct {
	ID       string `json:"id"`
	Case     CaseID `json:"case"`
	Boundary string `json:"boundary"`
	Probe    Probe  `json:"probe"`
	Fanout   int    `json:"fanout"`
}

func (s Stratum) Validate() error {
	if s.ID == "" || !s.Case.Valid() || s.Boundary == "" || !s.Probe.Valid() || !slices.Contains([]int{8, 32, 128}, s.Fanout) ||
		s.ID != stratumID(s.Case, s.Boundary, s.Probe, s.Fanout) {
		return invalid("stratum")
	}
	if s.Probe == ProbeUnfaulted && s.Boundary != UnfaultedBoundary || s.Probe != ProbeUnfaulted && s.Boundary == UnfaultedBoundary {
		return invalid("stratum boundary and probe")
	}
	return nil
}

type PairBlock struct {
	Index           int        `json:"index"`
	ScheduleBlockID string     `json:"schedule_block_id"`
	PairID          string     `json:"pair_id"`
	Stratum         Stratum    `json:"stratum"`
	Slot            int        `json:"slot"`
	Reserve         bool       `json:"reserve"`
	TopologyOrder   []Topology `json:"topology_order"`
}

func (b PairBlock) Validate() error {
	if b.Index < 1 || b.ScheduleBlockID == "" || b.PairID == "" || b.Slot < 1 || b.ScheduleBlockID != "schedule-block/"+b.PairID {
		return invalid("pair block identity")
	}
	if err := b.Stratum.Validate(); err != nil {
		return err
	}
	if len(b.TopologyOrder) != 2 || !b.TopologyOrder[0].Valid() || !b.TopologyOrder[1].Valid() || b.TopologyOrder[0] == b.TopologyOrder[1] {
		return invalid("pair topology order")
	}
	return nil
}

type Schedule struct {
	ProtocolVersion string      `json:"protocol_version"`
	Phase           Phase       `json:"phase"`
	Seed            uint64      `json:"seed"`
	Algorithm       string      `json:"algorithm"`
	Blocks          []PairBlock `json:"blocks"`
}

func BuildStrata(registration Preregistration) ([]Stratum, error) {
	strata := make([]Stratum, 0, registration.Population.ExpectedStrata)
	appendStratum := func(benchmarkCase CaseID, boundary string, probe Probe, fanout int) {
		strata = append(strata, Stratum{
			ID: stratumID(benchmarkCase, boundary, probe, fanout), Case: benchmarkCase,
			Boundary: boundary, Probe: probe, Fanout: fanout,
		})
	}
	for _, benchmarkCase := range registration.Cases {
		for _, fanout := range registration.ScalePolicy.FanoutSizes {
			appendStratum(benchmarkCase, UnfaultedBoundary, ProbeUnfaulted, fanout)
		}
		primary := registration.PrimaryBoundaryByCase[benchmarkCase]
		for _, fanout := range registration.ScalePolicy.FanoutSizes {
			appendStratum(benchmarkCase, primary, ProbeProtected, fanout)
		}
		appendStratum(benchmarkCase, primary, ProbeUnsafe, registration.ScalePolicy.CanonicalFanout)
		for _, boundary := range registration.SecondaryBoundaries[benchmarkCase] {
			appendStratum(benchmarkCase, boundary, ProbeUnsafe, registration.ScalePolicy.CanonicalFanout)
			appendStratum(benchmarkCase, boundary, ProbeProtected, registration.ScalePolicy.CanonicalFanout)
		}
	}
	seen := make(map[string]bool, len(strata))
	for _, stratum := range strata {
		if err := stratum.Validate(); err != nil || seen[stratum.ID] {
			return nil, invalid("derived stratum identity")
		}
		seen[stratum.ID] = true
	}
	if len(strata) != registration.Population.ExpectedStrata {
		return nil, invalid("derived stratum count")
	}
	return strata, nil
}

func BuildSchedule(registration Preregistration, phase Phase) (Schedule, error) {
	if err := registration.Validate(); err != nil {
		return Schedule{}, err
	}
	if !phase.Valid() {
		return Schedule{}, invalid("schedule phase")
	}
	strata, err := BuildStrata(registration)
	if err != nil {
		return Schedule{}, err
	}
	seed := registration.Population.PublicationSeed
	slots := registration.Population.MaximumAttemptedPairsPerStratum
	if phase == PhasePilot {
		seed = registration.Population.PilotSeed
		slots = registration.Population.PilotPairsPerStratum
	}
	random := splitMix64{state: seed}
	var oddSlotFirsts []Topology
	if slots%2 != 0 {
		oddSlotFirsts = balancedTopologyOrders(len(strata), TopologyDirectActivity)
		shuffle(oddSlotFirsts, &random)
	}
	blocks := make([]PairBlock, 0, len(strata)*slots)
	for stratumIndex, stratum := range strata {
		first := TopologyDirectActivity
		if len(oddSlotFirsts) != 0 {
			first = oddSlotFirsts[stratumIndex]
		}
		orders := balancedTopologyOrders(slots, first)
		if phase == PhasePublication {
			primary := orders[:registration.Population.MinimumValidPairsPerStratum]
			reserve := orders[registration.Population.MinimumValidPairsPerStratum:]
			shuffle(primary, &random)
			shuffle(reserve, &random)
		} else {
			shuffle(orders, &random)
		}
		for slot, first := range orders {
			pairID := fmt.Sprintf("topology-%s-v1/%s/slot-%02d", phase, stratum.ID, slot+1)
			blocks = append(blocks, PairBlock{
				ScheduleBlockID: "schedule-block/" + pairID,
				PairID:          pairID,
				Stratum:         stratum,
				Slot:            slot + 1,
				Reserve:         phase == PhasePublication && slot >= registration.Population.MinimumValidPairsPerStratum,
				TopologyOrder:   []Topology{first, otherTopology(first)},
			})
		}
	}
	shuffle(blocks, &random)
	for index := range blocks {
		blocks[index].Index = index + 1
	}
	return Schedule{
		ProtocolVersion: PublicationProtocolVersion,
		Phase:           phase,
		Seed:            seed,
		Algorithm:       registration.Population.ScheduleAlgorithm,
		Blocks:          blocks,
	}, nil
}

func ValidateSchedule(registration Preregistration, phase Phase, schedule Schedule) error {
	expected, err := BuildSchedule(registration, phase)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, schedule) {
		return invalid("recorded schedule drift")
	}
	return nil
}

func (s Schedule) Clone() Schedule {
	clone := s
	clone.Blocks = make([]PairBlock, len(s.Blocks))
	for index, block := range s.Blocks {
		clone.Blocks[index] = block
		clone.Blocks[index].TopologyOrder = slices.Clone(block.TopologyOrder)
	}
	return clone
}

func stratumID(benchmarkCase CaseID, boundary string, probe Probe, fanout int) string {
	return fmt.Sprintf("%s/%s/%s/fanout-%03d", benchmarkCase, strings.TrimSpace(boundary), probe, fanout)
}

func balancedTopologyOrders(count int, first Topology) []Topology {
	result := make([]Topology, count)
	for index := range result {
		if index%2 == 0 {
			result[index] = first
		} else {
			result[index] = otherTopology(first)
		}
	}
	return result
}

func otherTopology(topology Topology) Topology {
	if topology == TopologyDirectActivity {
		return TopologyChildWorkflow
	}
	return TopologyDirectActivity
}

type splitMix64 struct{ state uint64 }

func (s *splitMix64) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	value := s.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func shuffle[T any](values []T, random *splitMix64) {
	for index := len(values) - 1; index > 0; index-- {
		other := int(random.next() % uint64(index+1))
		values[index], values[other] = values[other], values[index]
	}
}
