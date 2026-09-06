package globalaggregator

import (
	"fmt"
	"strings"

	"continuum/internal/model"
)

func (aggregator *Aggregator) validateWindow(
	input model.CloudEdgeAggregate,
) error {
	duration := input.WindowEnd.Sub(input.WindowStart)
	if duration != aggregator.windowSize {
		return fmt.Errorf(
			"CloudEdgeAggregate %q ha finestra %s, attesa GLOBAL_WINDOW_SIZE %s",
			input.AggregateID,
			duration,
			aggregator.windowSize,
		)
	}
	start := input.WindowStart.UTC()
	if !start.Equal(start.Truncate(aggregator.windowSize)) {
		return fmt.Errorf(
			"CloudEdgeAggregate %q non allineato a GLOBAL_WINDOW_SIZE %s",
			input.AggregateID,
			aggregator.windowSize,
		)
	}
	return nil
}

func ValidateGlobalAggregate(aggregate model.GlobalAggregate) error {
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
