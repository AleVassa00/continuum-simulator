package cloudworker

import (
	"math"
	"testing"
	"time"

	"continuum/internal/model"
)

func TestWindowAggregatorComposesEdgeAggregates(t *testing.T) {
	aggregator := newTestAggregator(t)
	start := testTime(10, 0)

	inputs := []model.EdgeAggregate{
		newEdgeAggregate(
			"edge-3",
			start,
			metricAggregate(10, 0, 100, 5, 15),
		),
		newEdgeAggregate(
			"edge-3",
			start.Add(5*time.Minute),
			metricAggregate(2, 0, 100, 40, 60),
		),
		newEdgeAggregate(
			"edge-3",
			start.Add(10*time.Minute),
			metricAggregate(3, 1, 60, 10, 30),
		),
	}

	for _, input := range inputs {
		output, err := aggregator.Add(input)
		if err != nil {
			t.Fatalf("Add() ha restituito un errore: %v", err)
		}

		if output != nil {
			t.Fatal("la finestra e stata emessa prima dell'arrivo della finestra successiva")
		}
	}

	output, err := aggregator.Add(
		newEdgeAggregate(
			"edge-3",
			start.Add(15*time.Minute),
			metricAggregate(1, 0, 30, 30, 30),
		),
	)
	if err != nil {
		t.Fatalf("Add() ha restituito un errore: %v", err)
	}

	if output == nil {
		t.Fatal("la finestra precedente non e stata emessa")
	}

	if !output.WindowStart.Equal(start) ||
		!output.WindowEnd.Equal(start.Add(15*time.Minute)) {
		t.Fatalf(
			"finestra inattesa: [%s,%s)",
			output.WindowStart,
			output.WindowEnd,
		)
	}

	if output.EdgeID != "edge-3" {
		t.Fatalf("edge_id inatteso: %s", output.EdgeID)
	}

	if output.InputAggregates != 3 {
		t.Fatalf("InputAggregates=%d, atteso 3", output.InputAggregates)
	}

	if output.Events != 16 {
		t.Fatalf("Events=%d, atteso 16", output.Events)
	}

	assertMetric(
		t,
		output.Temperature,
		15,
		1,
		260,
		260.0/15.0,
		5,
		60,
	)

	if err := ValidateCloudEdgeAggregate(*output); err != nil {
		t.Fatalf("CloudEdgeAggregate emesso non valido: %v", err)
	}
}

func TestWindowAggregatorKeepsEdgesSeparate(t *testing.T) {
	aggregator := newTestAggregator(t)
	start := testTime(10, 0)

	for _, edgeID := range []string{"edge-0", "edge-1"} {
		_, err := aggregator.Add(
			newEdgeAggregate(
				edgeID,
				start,
				metricAggregate(2, 0, 20, 5, 15),
			),
		)
		if err != nil {
			t.Fatalf("Add(%s) ha restituito un errore: %v", edgeID, err)
		}
	}

	edge0Output, err := aggregator.Add(
		newEdgeAggregate(
			"edge-0",
			start.Add(15*time.Minute),
			metricAggregate(1, 0, 10, 10, 10),
		),
	)
	if err != nil {
		t.Fatalf("Add(edge-0) ha restituito un errore: %v", err)
	}

	if edge0Output == nil || edge0Output.EdgeID != "edge-0" {
		t.Fatalf("output edge-0 inatteso: %#v", edge0Output)
	}

	edge1Output, err := aggregator.Add(
		newEdgeAggregate(
			"edge-1",
			start.Add(15*time.Minute),
			metricAggregate(1, 0, 20, 20, 20),
		),
	)
	if err != nil {
		t.Fatalf("Add(edge-1) ha restituito un errore: %v", err)
	}

	if edge1Output == nil || edge1Output.EdgeID != "edge-1" {
		t.Fatalf("output edge-1 inatteso: %#v", edge1Output)
	}
}

func TestWindowAggregatorIgnoresDuplicateAggregateID(t *testing.T) {
	aggregator := newTestAggregator(t)
	start := testTime(10, 0)
	input := newEdgeAggregate(
		"edge-2",
		start,
		metricAggregate(4, 1, 40, 5, 15),
	)

	for index := 0; index < 2; index++ {
		output, err := aggregator.Add(input)
		if err != nil {
			t.Fatalf("Add() ha restituito un errore: %v", err)
		}

		if output != nil {
			t.Fatal("output inatteso durante la stessa finestra")
		}
	}

	output, err := aggregator.Add(
		newEdgeAggregate(
			"edge-2",
			start.Add(15*time.Minute),
			metricAggregate(1, 0, 10, 10, 10),
		),
	)
	if err != nil {
		t.Fatalf("Add() ha restituito un errore: %v", err)
	}

	if output.InputAggregates != 1 ||
		output.Events != 5 ||
		output.Temperature.Sum != 40 {
		t.Fatalf("duplicato incorporato nell'output: %#v", output)
	}
}

func TestWindowAggregatorRejectsPreviousWindow(t *testing.T) {
	aggregator := newTestAggregator(t)
	start := testTime(10, 0)

	_, err := aggregator.Add(
		newEdgeAggregate(
			"edge-4",
			start.Add(15*time.Minute),
			metricAggregate(1, 0, 10, 10, 10),
		),
	)
	if err != nil {
		t.Fatalf("primo Add() ha restituito un errore: %v", err)
	}

	_, err = aggregator.Add(
		newEdgeAggregate(
			"edge-4",
			start,
			metricAggregate(1, 0, 10, 10, 10),
		),
	)
	if err == nil {
		t.Fatal("EdgeAggregate temporalmente precedente accettato")
	}
}

func TestWindowAggregatorRejectsCrossingWindow(t *testing.T) {
	aggregator := newTestAggregator(t)
	start := testTime(10, 13)
	input := newEdgeAggregate(
		"edge-5",
		start,
		metricAggregate(1, 0, 10, 10, 10),
	)

	_, err := aggregator.Add(input)
	if err == nil {
		t.Fatal("finestra Edge che attraversa un confine Cloud accettata")
	}
}

func TestWindowAggregatorFlushesPartialWindowsDeterministically(t *testing.T) {
	aggregator := newTestAggregator(t)
	start := testTime(10, 0)

	for _, edgeID := range []string{"edge-9", "edge-1"} {
		_, err := aggregator.Add(
			newEdgeAggregate(
				edgeID,
				start,
				metricAggregate(1, 0, 10, 10, 10),
			),
		)
		if err != nil {
			t.Fatalf("Add(%s) ha restituito un errore: %v", edgeID, err)
		}
	}

	outputs := aggregator.Flush()

	if len(outputs) != 2 {
		t.Fatalf("Flush() ha restituito %d output, attesi 2", len(outputs))
	}

	if outputs[0].EdgeID != "edge-1" ||
		outputs[1].EdgeID != "edge-9" {
		t.Fatalf("ordine Flush() non deterministico: %#v", outputs)
	}

	if len(aggregator.Flush()) != 0 {
		t.Fatal("il secondo Flush() non e vuoto")
	}
}

func TestCloudAggregateIDIsDeterministic(t *testing.T) {
	start := testTime(10, 0)
	input := newEdgeAggregate(
		"edge-7",
		start,
		metricAggregate(1, 0, 10, 10, 10),
	)

	first := newTestAggregator(t)
	second := newTestAggregator(t)

	_, _ = first.Add(input)
	_, _ = second.Add(input)

	firstID := first.Flush()[0].AggregateID
	secondID := second.Flush()[0].AggregateID

	if firstID != secondID {
		t.Fatalf("AggregateID non deterministico: %q != %q", firstID, secondID)
	}

	expected := "cloud:edge-7:2026-01-01T10:00:00Z:2026-01-01T10:15:00Z"
	if firstID != expected {
		t.Fatalf("AggregateID=%q, atteso %q", firstID, expected)
	}
}

func TestValidateMetricAggregateRejectsInconsistentAverage(t *testing.T) {
	aggregate := newEdgeAggregate(
		"edge-8",
		testTime(10, 0),
		metricAggregate(2, 0, 20, 5, 15),
	)

	wrongAverage := 30.0
	aggregate.Temperature.Average = &wrongAverage

	if err := ValidateEdgeAggregate(aggregate); err == nil {
		t.Fatal("average incoerente con Sum/Valid accettata")
	}
}

func newTestAggregator(t *testing.T) *WindowAggregator {
	t.Helper()

	aggregator, err := NewWindowAggregator(15 * time.Minute)
	if err != nil {
		t.Fatalf("NewWindowAggregator() ha restituito un errore: %v", err)
	}

	return aggregator
}

func newEdgeAggregate(
	edgeID string,
	start time.Time,
	metric model.MetricAggregate,
) model.EdgeAggregate {
	end := start.Add(5 * time.Minute)
	events := metric.Valid + metric.Invalid

	return model.EdgeAggregate{
		SchemaVersion: model.EdgeAggregateSchemaVersion,
		AggregateID: edgeID + ":" +
			start.UTC().Format(time.RFC3339) + ":" +
			end.UTC().Format(time.RFC3339),
		EdgeID:      edgeID,
		WindowStart: start,
		WindowEnd:   end,
		Events:      events,
		Temperature: metric,
		Humidity:    metric,
		Pressure:    metric,
		EmittedAt:   testTime(12, 0),
	}
}

func metricAggregate(
	valid uint64,
	invalid uint64,
	sum float64,
	minimum float64,
	maximum float64,
) model.MetricAggregate {
	if valid == 0 {
		return model.MetricAggregate{
			Valid:   0,
			Invalid: invalid,
			Sum:     0,
		}
	}

	average := sum / float64(valid)

	return model.MetricAggregate{
		Valid:   valid,
		Invalid: invalid,
		Sum:     sum,
		Average: &average,
		Min:     &minimum,
		Max:     &maximum,
	}
}

func assertMetric(
	t *testing.T,
	actual model.MetricAggregate,
	valid uint64,
	invalid uint64,
	sum float64,
	average float64,
	minimum float64,
	maximum float64,
) {
	t.Helper()

	if actual.Valid != valid ||
		actual.Invalid != invalid {
		t.Fatalf(
			"conteggi metrici inattesi: valid=%d invalid=%d",
			actual.Valid,
			actual.Invalid,
		)
	}

	if math.Abs(actual.Sum-sum) > 1e-9 {
		t.Fatalf("Sum=%.12f, attesa %.12f", actual.Sum, sum)
	}

	if actual.Average == nil ||
		math.Abs(*actual.Average-average) > 1e-9 {
		t.Fatalf("Average=%v, attesa %.12f", actual.Average, average)
	}

	if actual.Min == nil || *actual.Min != minimum {
		t.Fatalf("Min=%v, atteso %.12f", actual.Min, minimum)
	}

	if actual.Max == nil || *actual.Max != maximum {
		t.Fatalf("Max=%v, atteso %.12f", actual.Max, maximum)
	}
}

func testTime(
	hour int,
	minute int,
) time.Time {
	return time.Date(
		2026,
		time.January,
		1,
		hour,
		minute,
		0,
		0,
		time.UTC,
	)
}
