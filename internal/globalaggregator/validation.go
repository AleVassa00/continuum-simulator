package globalaggregator

import (
	"fmt"
	"strings"

	"continuum/internal/model"
)

func ValidateGlobalAggregate(aggregate model.GlobalAggregate) error {
	if aggregate.SchemaVersion != model.GlobalAggregateSchemaVersion {
		return fmt.Errorf(
			"schema_version GlobalAggregate non supportata: %d",
			aggregate.SchemaVersion,
		)
	}

	if strings.TrimSpace(aggregate.AggregateID) == "" {
		return fmt.Errorf("aggregate_id GlobalAggregate mancante")
	}
	if aggregate.WindowStart.IsZero() {
		return fmt.Errorf("window_start GlobalAggregate mancante")
	}
	if aggregate.WindowEnd.IsZero() {
		return fmt.Errorf("window_end GlobalAggregate mancante")
	}
	if !aggregate.WindowEnd.After(aggregate.WindowStart) {
		return fmt.Errorf(
			"finestra GlobalAggregate non valida: start=%s end=%s",
			aggregate.WindowStart,
			aggregate.WindowEnd,
		)
	}
	if aggregate.ExpectedEdges == 0 {
		return fmt.Errorf("GlobalAggregate senza Edge attesi")
	}
	if aggregate.ContributingEdges == 0 {
		return fmt.Errorf("GlobalAggregate senza Edge contribuenti")
	}
	if aggregate.ContributingEdges > aggregate.ExpectedEdges {
		return fmt.Errorf(
			"Edge contribuenti (%d) maggiori degli attesi (%d)",
			aggregate.ContributingEdges,
			aggregate.ExpectedEdges,
		)
	}
	if aggregate.Events == 0 {
		return fmt.Errorf("GlobalAggregate senza eventi")
	}
	if aggregate.EmittedAt.IsZero() {
		return fmt.Errorf("emitted_at GlobalAggregate mancante")
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
