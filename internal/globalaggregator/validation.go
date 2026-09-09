package globalaggregator

import (
	"fmt"
	"strings"

	"continuum/internal/model"
)

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
	if aggregate.ExpectedPartitions == 0 {
		return fmt.Errorf("GlobalAggregate senza partition attesi")
	}
	if aggregate.ContributingPartitions == 0 {
		return fmt.Errorf("GlobalAggregate senza partition contribuenti")
	}
	if aggregate.ContributingPartitions != aggregate.ExpectedPartitions {
		return fmt.Errorf(
			"partition contribuenti (%d) diverse dalle attese (%d)",
			aggregate.ContributingPartitions,
			aggregate.ExpectedPartitions,
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
