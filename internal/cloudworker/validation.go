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
