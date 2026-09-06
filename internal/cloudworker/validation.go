package cloudworker

import (
	"fmt"
	"strings"
	"time"

	"continuum/internal/model"
)

func ValidateEdgeAggregate(
	aggregate model.EdgeAggregate,
) error {
	if err := validateAggregateHeader(
		aggregate.AggregateID,
		aggregate.EdgeID,
		aggregate.WindowStart,
		aggregate.WindowEnd,
		aggregate.Events,
	); err != nil {
		return err
	}

	if err := model.ValidateMetricAggregate(
		"temperature",
		aggregate.Events,
		aggregate.Temperature,
	); err != nil {
		return err
	}

	if err := model.ValidateMetricAggregate(
		"humidity",
		aggregate.Events,
		aggregate.Humidity,
	); err != nil {
		return err
	}

	return model.ValidateMetricAggregate(
		"pressure",
		aggregate.Events,
		aggregate.Pressure,
	)
}

func ValidateCloudEdgeAggregate(
	aggregate model.CloudEdgeAggregate,
) error {
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

	if err := model.ValidateMetricAggregate(
		"temperature",
		aggregate.Events,
		aggregate.Temperature,
	); err != nil {
		return err
	}

	if err := model.ValidateMetricAggregate(
		"humidity",
		aggregate.Events,
		aggregate.Humidity,
	); err != nil {
		return err
	}

	return model.ValidateMetricAggregate(
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

func (
	aggregator *WindowAggregator,
) cloudWindowFor(
	input model.EdgeAggregate,
) (
	time.Time,
	time.Time,
	error,
) {
	edgeWindowSize := input.WindowEnd.Sub(
		input.WindowStart,
	)

	if edgeWindowSize <= 0 {
		return time.Time{},
			time.Time{},
			fmt.Errorf(
				"EdgeAggregate %q ha una finestra non valida",
				input.AggregateID,
			)
	}

	if aggregator.windowSize%edgeWindowSize != 0 {
		return time.Time{},
			time.Time{},
			fmt.Errorf(
				"finestra Cloud %s non multipla della finestra Edge %s per aggregate_id=%s",
				aggregator.windowSize,
				edgeWindowSize,
				input.AggregateID,
			)
	}

	windowStart := input.WindowStart.Truncate(
		aggregator.windowSize,
	)

	windowEnd := windowStart.Add(
		aggregator.windowSize,
	)

	if input.WindowStart.Before(windowStart) ||
		input.WindowEnd.After(windowEnd) {
		return time.Time{},
			time.Time{},
			fmt.Errorf(
				"finestra Edge [%s,%s) attraversa il confine della finestra Cloud [%s,%s)",
				input.WindowStart.Format(time.RFC3339),
				input.WindowEnd.Format(time.RFC3339),
				windowStart.Format(time.RFC3339),
				windowEnd.Format(time.RFC3339),
			)
	}

	return windowStart, windowEnd, nil
}
