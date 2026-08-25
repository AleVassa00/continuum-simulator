package cloudworker

import (
	"fmt"
	"math"
	"strings"
	"time"

	"continuum/internal/model"
)

func ValidateEdgeAggregate(
	aggregate model.EdgeAggregate,
) error {
	if aggregate.SchemaVersion != model.EdgeAggregateSchemaVersion {
		return fmt.Errorf(
			"schema_version EdgeAggregate non supportata: %d",
			aggregate.SchemaVersion,
		)
	}

	if err := validateAggregateHeader(
		aggregate.AggregateID,
		aggregate.EdgeID,
		aggregate.WindowStart,
		aggregate.WindowEnd,
		aggregate.Events,
	); err != nil {
		return err
	}

	if err := validateMetricAggregate(
		"temperature",
		aggregate.Events,
		aggregate.Temperature,
	); err != nil {
		return err
	}

	if err := validateMetricAggregate(
		"humidity",
		aggregate.Events,
		aggregate.Humidity,
	); err != nil {
		return err
	}

	return validateMetricAggregate(
		"pressure",
		aggregate.Events,
		aggregate.Pressure,
	)
}

func ValidateCloudEdgeAggregate(
	aggregate model.CloudEdgeAggregate,
) error {
	if aggregate.SchemaVersion != model.CloudEdgeAggregateSchemaVersion {
		return fmt.Errorf(
			"schema_version CloudEdgeAggregate non supportata: %d",
			aggregate.SchemaVersion,
		)
	}

	if aggregate.InputAggregates == 0 {
		return fmt.Errorf(
			"CloudEdgeAggregate senza aggregati di input",
		)
	}

	if err := validateAggregateHeader(
		aggregate.AggregateID,
		aggregate.EdgeID,
		aggregate.WindowStart,
		aggregate.WindowEnd,
		aggregate.Events,
	); err != nil {
		return err
	}

	if err := validateMetricAggregate(
		"temperature",
		aggregate.Events,
		aggregate.Temperature,
	); err != nil {
		return err
	}

	if err := validateMetricAggregate(
		"humidity",
		aggregate.Events,
		aggregate.Humidity,
	); err != nil {
		return err
	}

	return validateMetricAggregate(
		"pressure",
		aggregate.Events,
		aggregate.Pressure,
	)
}

func validateAggregateHeader(
	aggregateID string,
	edgeID string,
	windowStart time.Time,
	windowEnd time.Time,
	events uint64,
) error {
	if strings.TrimSpace(aggregateID) == "" {
		return fmt.Errorf("aggregate_id mancante")
	}

	if strings.TrimSpace(edgeID) == "" {
		return fmt.Errorf("edge_id mancante")
	}

	if windowStart.IsZero() {
		return fmt.Errorf("window_start mancante")
	}

	if windowEnd.IsZero() {
		return fmt.Errorf("window_end mancante")
	}

	if !windowEnd.After(windowStart) {
		return fmt.Errorf(
			"finestra non valida: start=%s end=%s",
			windowStart.Format(time.RFC3339),
			windowEnd.Format(time.RFC3339),
		)
	}

	if events == 0 {
		return fmt.Errorf("aggregato senza eventi")
	}

	return nil
}

func validateMetricAggregate(
	name string,
	events uint64,
	metric model.MetricAggregate,
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

	if math.IsNaN(metric.Sum) ||
		math.IsInf(metric.Sum, 0) {
		return fmt.Errorf(
			"%s: sum non finita",
			name,
		)
	}

	if metric.Valid == 0 {
		if metric.Sum != 0 {
			return fmt.Errorf(
				"%s: sum %.6f presente senza misure valide",
				name,
				metric.Sum,
			)
		}

		if metric.Average != nil ||
			metric.Min != nil ||
			metric.Max != nil {
			return fmt.Errorf(
				"%s: statistiche presenti senza misure valide",
				name,
			)
		}

		return nil
	}

	if metric.Average == nil ||
		metric.Min == nil ||
		metric.Max == nil {
		return fmt.Errorf(
			"%s: statistiche mancanti con %d misure valide",
			name,
			metric.Valid,
		)
	}

	if !isFinite(*metric.Average) ||
		!isFinite(*metric.Min) ||
		!isFinite(*metric.Max) {
		return fmt.Errorf(
			"%s: statistiche non finite",
			name,
		)
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

	expectedAverage := metric.Sum /
		float64(metric.Valid)

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
	return !math.IsNaN(value) &&
		!math.IsInf(value, 0)
}

func floatTolerance(values ...float64) float64 {
	scale := 1.0

	for _, value := range values {
		scale = max(
			scale,
			math.Abs(value),
		)
	}

	return 1e-9 * scale
}
