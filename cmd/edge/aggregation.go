package main

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"continuum/internal/model"
)

type EdgeOutputKind byte

const (
	EdgeOutputAggregate EdgeOutputKind = iota
	EdgeOutputEndOfReplay
)

type EdgeOutputRecord struct {
	Kind        EdgeOutputKind
	Aggregate   model.EdgeAggregate
	EndOfReplay model.EndOfReplay
}

type MetricValue struct {
	Value float64
	Valid bool
}

type EdgeMeasurement struct {
	Temperature MetricValue
	Humidity    MetricValue
	Pressure    MetricValue
}

type MetricState struct {
	Valid   uint64
	Invalid uint64

	Sum float64
	Min float64
	Max float64
}

type WindowState struct {
	Start time.Time
	End   time.Time

	Events uint64

	Temperature MetricState
	Humidity    MetricState
	Pressure    MetricState
}

type WindowAggregator struct {
	mu sync.Mutex

	edgeID     string
	windowSize time.Duration
	current    *WindowState
	ended      bool

	output        chan<- EdgeOutputRecord
	egressStopped <-chan struct{}
}

var (
	errEdgeWindowClosed = errors.New("evento appartenente a finestra Edge gia chiusa")
	errEdgeReplayEnded  = errors.New("replay Edge gia terminato")
)

func newWindowState(
	start time.Time,
	end time.Time,
) *WindowState {
	return &WindowState{
		Start: start,
		End:   end,
	}
}

func validateSensorEvent(
	event model.SensorEvent,
) error {
	if strings.TrimSpace(
		event.EventID,
	) == "" {
		return fmt.Errorf(
			"event_id mancante",
		)
	}

	if strings.TrimSpace(
		event.SensorID,
	) == "" {
		return fmt.Errorf(
			"sensor_id mancante",
		)
	}

	if event.EventTime.IsZero() {
		return fmt.Errorf(
			"event_time mancante",
		)
	}

	return nil
}

func parseMeasurements(
	event model.SensorEvent,
) EdgeMeasurement {
	return EdgeMeasurement{
		Temperature: parseMetric(
			event.Measurements,
			"temperature",
			-40,
			85,
		),

		Humidity: parseMetric(
			event.Measurements,
			"humidity",
			0,
			100,
		),

		Pressure: parseMetric(
			event.Measurements,
			"pressure",
			30000,
			110000,
		),
	}
}

func parseMetric(
	measurements map[string]model.NullableFloat64,
	name string,
	minValue float64,
	maxValue float64,
) MetricValue {
	measurement, found := measurements[name]

	if !found {
		return MetricValue{
			Valid: false,
		}
	}

	if !measurement.Valid {
		return MetricValue{
			Valid: false,
		}
	}

	value := measurement.Value

	if math.IsNaN(value) ||
		math.IsInf(value, 0) {
		return MetricValue{
			Valid: false,
		}
	}

	if value < minValue ||
		value > maxValue {
		return MetricValue{
			Valid: false,
		}
	}

	return MetricValue{
		Value: value,
		Valid: true,
	}
}

func (
	aggregator *WindowAggregator,
) Add(
	eventID string,
	eventTime time.Time,
	measurement EdgeMeasurement,
) error {
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	if aggregator.ended {
		return fmt.Errorf(
			"%w: edge=%s event_id=%s",
			errEdgeReplayEnded,
			aggregator.edgeID,
			eventID,
		)
	}

	windowStart := eventTime.Truncate(
		aggregator.windowSize,
	)

	windowEnd := windowStart.Add(
		aggregator.windowSize,
	)

	if aggregator.current == nil {
		aggregator.current = newWindowState(
			windowStart,
			windowEnd,
		)
	}

	if windowStart.Before(
		aggregator.current.Start,
	) {
		return fmt.Errorf(
			"%w: event_id=%s event_time=%s current_window=%s",
			errEdgeWindowClosed,
			eventID,
			eventTime.Format(time.RFC3339),
			aggregator.current.Start.Format(time.RFC3339),
		)
	}

	if !windowStart.Equal(
		aggregator.current.Start,
	) {
		err := aggregator.emitCurrentWindow()
		if err != nil {
			return err
		}

		aggregator.current = newWindowState(
			windowStart,
			windowEnd,
		)
	}

	aggregator.current.Add(
		measurement,
	)

	return nil
}

func (
	window *WindowState,
) Add(
	measurement EdgeMeasurement,
) {
	window.Events++

	window.Temperature.Add(
		measurement.Temperature,
	)

	window.Humidity.Add(
		measurement.Humidity,
	)

	window.Pressure.Add(
		measurement.Pressure,
	)
}

func (
	metric *MetricState,
) Add(
	value MetricValue,
) {
	if !value.Valid {
		metric.Invalid++

		return
	}

	if metric.Valid == 0 {
		metric.Min = value.Value
		metric.Max = value.Value
	} else {
		metric.Min = min(
			metric.Min,
			value.Value,
		)

		metric.Max = max(
			metric.Max,
			value.Value,
		)
	}

	metric.Sum += value.Value
	metric.Valid++
}

func (
	aggregator *WindowAggregator,
) emitCurrentWindow() error {
	if aggregator.current == nil {
		return nil
	}

	if aggregator.current.Events == 0 {
		return nil
	}

	aggregate := buildEdgeAggregate(
		aggregator.edgeID,
		aggregator.current,
	)

	select {
	case aggregator.output <- EdgeOutputRecord{
		Kind:      EdgeOutputAggregate,
		Aggregate: aggregate,
	}:
		return nil
	case <-aggregator.egressStopped:
		return fmt.Errorf("Kafka egress terminato")
	}
}

func (
	aggregator *WindowAggregator,
) Flush() error {
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	if err := aggregator.emitCurrentWindow(); err != nil {
		return err
	}

	aggregator.current = nil

	return nil
}

func (
	aggregator *WindowAggregator,
) EndReplay(
	record model.EndOfReplay,
) error {
	if err := model.ValidateEndOfReplay(record); err != nil {
		return err
	}

	if record.EdgeID != aggregator.edgeID {
		return fmt.Errorf(
			"EndOfReplay edge_id=%s non coerente con Edge %s",
			record.EdgeID,
			aggregator.edgeID,
		)
	}

	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	if aggregator.ended {
		fmt.Printf(
			"%s: EndOfReplay duplicato ignorato\n",
			aggregator.edgeID,
		)
		return nil
	}

	if err := aggregator.emitCurrentWindow(); err != nil {
		return fmt.Errorf(
			"flush finestra finale Edge %s fallito: %w",
			aggregator.edgeID,
			err,
		)
	}
	aggregator.current = nil

	forwarded := record
	forwarded.EmittedAt = forwarded.EmittedAt.UTC()

	select {
	case aggregator.output <- EdgeOutputRecord{
		Kind:        EdgeOutputEndOfReplay,
		EndOfReplay: forwarded,
	}:
	case <-aggregator.egressStopped:
		return fmt.Errorf("Kafka egress terminato")
	}

	aggregator.ended = true

	return nil
}

func buildMetricAggregate(
	state MetricState,
) model.MetricAggregate {
	if state.Valid == 0 {
		return model.MetricAggregate{
			Valid:   0,
			Invalid: state.Invalid,
			Sum:     0,
			Average: nil,
			Min:     nil,
			Max:     nil,
		}
	}

	average := state.Sum /
		float64(state.Valid)

	minimum := state.Min
	maximum := state.Max

	return model.MetricAggregate{
		Valid:   state.Valid,
		Invalid: state.Invalid,
		Sum:     state.Sum,
		Average: &average,
		Min:     &minimum,
		Max:     &maximum,
	}
}

func buildAggregateID(
	edgeID string,
	windowStart time.Time,
	windowEnd time.Time,
) string {
	return fmt.Sprintf(
		"%s:%s:%s",
		edgeID,
		windowStart.UTC().Format(
			time.RFC3339,
		),
		windowEnd.UTC().Format(
			time.RFC3339,
		),
	)
}

func buildEdgeAggregate(
	edgeID string,
	window *WindowState,
) model.EdgeAggregate {
	return model.EdgeAggregate{
		SchemaVersion: model.EdgeAggregateSchemaVersion,

		AggregateID: buildAggregateID(
			edgeID,
			window.Start,
			window.End,
		),

		EdgeID: edgeID,

		WindowStart: window.Start,
		WindowEnd:   window.End,

		Events: window.Events,

		Temperature: buildMetricAggregate(
			window.Temperature,
		),

		Humidity: buildMetricAggregate(
			window.Humidity,
		),

		Pressure: buildMetricAggregate(
			window.Pressure,
		),

		EmittedAt: time.Now().UTC(),
	}
}
