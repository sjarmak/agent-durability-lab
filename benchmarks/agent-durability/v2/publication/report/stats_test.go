package report

import (
	"reflect"
	"testing"
)

func TestWilsonIntervalForThirtyOfThirty(t *testing.T) {
	interval := Wilson95(30, 30)
	if interval.Lower < 0.886 || interval.Lower > 0.887 || interval.Upper != 1 {
		t.Fatalf("interval = %+v", interval)
	}
}

func TestPairedBootstrapIsDeterministicAndUsesPairDifferences(t *testing.T) {
	temporal := []float64{10, 20, 30, 40, 50}
	postgres := []float64{7, 17, 27, 37, 47}
	first, err := PairedMedianDifference(temporal, postgres, 20_000, 2718281828)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PairedMedianDifference(temporal, postgres, 20_000, 2718281828)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first.Estimate != 3 || first.Lower != 3 || first.Upper != 3 {
		t.Fatalf("bootstrap = %+v / %+v", first, second)
	}
}

func TestPositiveRatioRequiresEveryPairToBePositive(t *testing.T) {
	if ratio := PositivePairedRatio([]float64{2, 4}, []float64{1, 2}); ratio == nil || ratio.Median != 2 {
		t.Fatalf("positive ratio = %+v", ratio)
	}
	if ratio := PositivePairedRatio([]float64{2, 0}, []float64{1, 2}); ratio != nil {
		t.Fatalf("zero-valued ratio = %+v, want nil", ratio)
	}
}
