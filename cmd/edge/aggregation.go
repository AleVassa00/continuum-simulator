package main

import (
	"errors"
	"fmt"
	"time"

	"continuum/internal/model"
)

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
	edgeID     string
	windowSize time.Duration
	current    *WindowState
	ended      bool
}

var (
	errEdgeWindowClosed = errors.New("evento appartenente a finestra Edge gia chiusa")
	errEdgeReplayEnded  = errors.New("replay Edge gia terminato")
)

func newWindowState(start time.Time, end time.Time) *WindowState {
	return &WindowState{
		Start: start,
		End:   end,
	}
}

func (
	aggregator *WindowAggregator,
) Add(
	eventID string,
	eventTime time.Time,
	measurement EdgeMeasurement,
) (*model.EdgeAggregate, error) {

	if aggregator.ended {
		return nil, fmt.Errorf(
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
		return nil, fmt.Errorf(
			"%w: event_id=%s event_time=%s current_window=%s",
			errEdgeWindowClosed,
			eventID,
			eventTime.Format(time.RFC3339),
			aggregator.current.Start.Format(time.RFC3339),
		)
	}

	var aggregate *model.EdgeAggregate
	if !windowStart.Equal(
		aggregator.current.Start,
	) {
		aggregate = aggregator.Flush()

		aggregator.current = newWindowState(
			windowStart,
			windowEnd,
		)
	}

	aggregator.current.Add(
		measurement,
	)

	return aggregate, nil
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
) currentAggregate() *model.EdgeAggregate {
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

	return &aggregate
}

func (
	aggregator *WindowAggregator,
) Flush() *model.EdgeAggregate {
	aggregate := aggregator.currentAggregate()
	aggregator.current = nil

	return aggregate
}

func (
	aggregator *WindowAggregator,
) EndReplay() (*model.EdgeAggregate, error) {
	if aggregator.ended {
		return nil, errEdgeReplayEnded
	}

	aggregate := aggregator.Flush()
	aggregator.ended = true

	return aggregate, nil
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
