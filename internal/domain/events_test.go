package domain

import (
	"math"
	"testing"
)

func TestMetricStatsCanBeMergedExactly(t *testing.T) {
	var left, right MetricStats
	for _, value := range []float64{1, 2} {
		left.Add(value)
	}
	for _, value := range []float64{3, 4} {
		right.Add(value)
	}
	left.Merge(right)

	if left.Count != 4 || left.Min != 1 || left.Max != 4 || left.Sum != 10 {
		t.Fatalf("merged stats = %+v", left)
	}
	if math.Abs(left.Mean()-2.5) > 1e-12 {
		t.Fatalf("mean = %v", left.Mean())
	}
	if math.Abs(left.PopulationStdDev()-math.Sqrt(1.25)) > 1e-12 {
		t.Fatalf("stddev = %v", left.PopulationStdDev())
	}
}
