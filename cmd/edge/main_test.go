package main

import (
	"testing"
	"time"

	"continuum/internal/model"
)

func TestBuildMetricAggregateIncludesComposableSum(t *testing.T) {
	state := MetricState{
		Valid:   2,
		Invalid: 1,
		Sum:     7,
		Min:     2,
		Max:     5,
	}

	aggregate := buildMetricAggregate(state)

	if aggregate.Valid != 2 ||
		aggregate.Invalid != 1 ||
		aggregate.Sum != 7 {
		t.Fatalf("conteggi o somma inattesi: %#v", aggregate)
	}

	if aggregate.Average == nil ||
		*aggregate.Average != 3.5 {
		t.Fatalf("average inattesa: %v", aggregate.Average)
	}

	if aggregate.Min == nil || *aggregate.Min != 2 {
		t.Fatalf("min inatteso: %v", aggregate.Min)
	}

	if aggregate.Max == nil || *aggregate.Max != 5 {
		t.Fatalf("max inatteso: %v", aggregate.Max)
	}
}

func TestBuildMetricAggregateWithoutValidValues(t *testing.T) {
	aggregate := buildMetricAggregate(
		MetricState{
			Invalid: 3,
		},
	)

	if aggregate.Valid != 0 ||
		aggregate.Invalid != 3 ||
		aggregate.Sum != 0 ||
		aggregate.Average != nil ||
		aggregate.Min != nil ||
		aggregate.Max != nil {
		t.Fatalf("aggregato senza valori validi inatteso: %#v", aggregate)
	}
}

func TestBuildEdgeAggregateUsesCurrentSchema(t *testing.T) {
	start := time.Date(
		2026,
		time.January,
		1,
		10,
		0,
		0,
		0,
		time.UTC,
	)

	aggregate := buildEdgeAggregate(
		"edge-0",
		&WindowState{
			Start:  start,
			End:    start.Add(5 * time.Minute),
			Events: 1,
			Temperature: MetricState{
				Valid: 1,
				Sum:   20,
				Min:   20,
				Max:   20,
			},
			Humidity: MetricState{
				Valid: 1,
				Sum:   50,
				Min:   50,
				Max:   50,
			},
			Pressure: MetricState{
				Valid: 1,
				Sum:   100000,
				Min:   100000,
				Max:   100000,
			},
		},
	)

	if aggregate.SchemaVersion != model.EdgeAggregateSchemaVersion {
		t.Fatalf(
			"schema_version=%d, attesa %d",
			aggregate.SchemaVersion,
			model.EdgeAggregateSchemaVersion,
		)
	}
}
