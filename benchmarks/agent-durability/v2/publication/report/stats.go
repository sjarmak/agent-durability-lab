package report

import (
	"fmt"
	"math"
	"sort"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

type Interval struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

type BootstrapEstimate struct {
	Estimate  float64 `json:"estimate"`
	Lower     float64 `json:"lower"`
	Upper     float64 `json:"upper"`
	Resamples int     `json:"resamples"`
}

type Distribution struct {
	Minimum float64 `json:"minimum"`
	P10     float64 `json:"p10"`
	P25     float64 `json:"p25"`
	Median  float64 `json:"median"`
	P75     float64 `json:"p75"`
	P90     float64 `json:"p90"`
	Maximum float64 `json:"maximum"`
	Mean    float64 `json:"mean"`
}

func Wilson95(successes, total int) Interval {
	if total <= 0 || successes < 0 || successes > total {
		return Interval{}
	}
	const z = 1.959963984540054
	n := float64(total)
	p := float64(successes) / n
	z2 := z * z
	denominator := 1 + z2/n
	center := (p + z2/(2*n)) / denominator
	half := z * math.Sqrt((p*(1-p)+z2/(4*n))/n) / denominator
	lower, upper := center-half, center+half
	if lower < 0 {
		lower = 0
	}
	if upper > 1 {
		upper = 1
	}
	if successes == 0 {
		lower = 0
	}
	if successes == total {
		upper = 1
	}
	return Interval{Lower: lower, Upper: upper}
}

func PairedMedianDifference(temporal, postgresql []float64, resamples int, seed uint64) (BootstrapEstimate, error) {
	if len(temporal) == 0 || len(temporal) != len(postgresql) || resamples < 1 || seed == 0 {
		return BootstrapEstimate{}, fmt.Errorf("%w: paired bootstrap input", protocol.ErrInvalidEvidence)
	}
	deltas := make([]float64, len(temporal))
	for index := range temporal {
		deltas[index] = temporal[index] - postgresql[index]
	}
	estimate := median(deltas)
	random := splitMix64{state: seed}
	bootstrap := make([]float64, resamples)
	sample := make([]float64, len(deltas))
	for iteration := range bootstrap {
		for index := range sample {
			sample[index] = deltas[int(random.next()%uint64(len(deltas)))]
		}
		bootstrap[iteration] = median(sample)
	}
	sort.Float64s(bootstrap)
	return BootstrapEstimate{
		Estimate: estimate, Lower: quantileSorted(bootstrap, 0.025), Upper: quantileSorted(bootstrap, 0.975), Resamples: resamples,
	}, nil
}

func Summarize(values []float64) Distribution {
	if len(values) == 0 {
		return Distribution{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mean := float64(0)
	for _, value := range sorted {
		mean += value
	}
	mean /= float64(len(sorted))
	return Distribution{
		Minimum: sorted[0], P10: quantileSorted(sorted, 0.10), P25: quantileSorted(sorted, 0.25),
		Median: medianSorted(sorted), P75: quantileSorted(sorted, 0.75), P90: quantileSorted(sorted, 0.90),
		Maximum: sorted[len(sorted)-1], Mean: mean,
	}
}

func PositivePairedRatio(numerator, denominator []float64) *Distribution {
	if len(numerator) == 0 || len(numerator) != len(denominator) {
		return nil
	}
	ratios := make([]float64, len(numerator))
	for index := range numerator {
		if numerator[index] <= 0 || denominator[index] <= 0 {
			return nil
		}
		ratios[index] = numerator[index] / denominator[index]
	}
	result := Summarize(ratios)
	return &result
}

func median(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return medianSorted(sorted)
}

func medianSorted(sorted []float64) float64 {
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

func quantileSorted(sorted []float64, quantile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * quantile)
	return sorted[index]
}

type splitMix64 struct{ state uint64 }

func (s *splitMix64) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	value := s.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
