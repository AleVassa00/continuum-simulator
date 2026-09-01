package model

import (
	"fmt"
	"math"
)

func ValidateMetricAggregate(
	name string,
	events uint64,
	metric MetricAggregate,
) error {
	if metric.Valid+metric.Invalid != events {
		return fmt.Errorf(
			"%s: valid(%d) + invalid(%d) != events(%d)",
			name,
			metric.Valid,
			metric.Invalid,
			events,
		)
	}

	if math.IsNaN(metric.Sum) || math.IsInf(metric.Sum, 0) {
		return fmt.Errorf("%s: sum non finita", name)
	}

	if metric.Valid == 0 {
		if metric.Sum != 0 {
			return fmt.Errorf(
				"%s: sum %.6f presente senza misure valide",
				name,
				metric.Sum,
			)
		}

		if metric.Average != nil || metric.Min != nil || metric.Max != nil {
			return fmt.Errorf(
				"%s: statistiche presenti senza misure valide",
				name,
			)
		}

		return nil
	}

	if metric.Average == nil || metric.Min == nil || metric.Max == nil {
		return fmt.Errorf(
			"%s: statistiche mancanti con %d misure valide",
			name,
			metric.Valid,
		)
	}

	if !isFinite(*metric.Average) ||
		!isFinite(*metric.Min) ||
		!isFinite(*metric.Max) {
		return fmt.Errorf("%s: statistiche non finite", name)
	}

	if *metric.Min > *metric.Max {
		return fmt.Errorf(
			"%s: min %.6f maggiore di max %.6f",
			name,
			*metric.Min,
			*metric.Max,
		)
	}

	tolerance := floatTolerance(
		*metric.Average,
		*metric.Min,
		*metric.Max,
	)
	if *metric.Average < *metric.Min-tolerance ||
		*metric.Average > *metric.Max+tolerance {
		return fmt.Errorf(
			"%s: average %.6f fuori dal range [%.6f, %.6f]",
			name,
			*metric.Average,
			*metric.Min,
			*metric.Max,
		)
	}

	expectedAverage := metric.Sum / float64(metric.Valid)
	if math.Abs(*metric.Average-expectedAverage) >
		floatTolerance(*metric.Average, expectedAverage) {
		return fmt.Errorf(
			"%s: average %.12f diversa da sum/valid %.12f",
			name,
			*metric.Average,
			expectedAverage,
		)
	}

	return nil
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func floatTolerance(values ...float64) float64 {
	scale := 1.0
	for _, value := range values {
		scale = max(scale, math.Abs(value))
	}
	return 1e-9 * scale
}
