package cloudworker

import (
	"fmt"
	"time"

	"continuum/internal/model"
)

type cloudWindowState struct {
	edgeID string
	start  time.Time
	end    time.Time

	inputAggregates  uint64
	events           uint64
	seenAggregateIDs map[string]struct{}

	temperature metricState
	humidity    metricState
	pressure    metricState
}

type metricState struct {
	valid   uint64
	invalid uint64
	sum     float64
	min     float64
	max     float64
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

func newCloudWindowState(
	edgeID string,
	start time.Time,
	end time.Time,
) *cloudWindowState {
	return &cloudWindowState{
		edgeID: edgeID,
		start:  start,
		end:    end,
		// Gli ID vivono quanto la finestra: sostituzione e flush liberano il set.
		seenAggregateIDs: make(map[string]struct{}),
	}
}

func (
	state *cloudWindowState,
) add(
	input model.EdgeAggregate,
) {
	state.seenAggregateIDs[input.AggregateID] = struct{}{}
	state.inputAggregates++
	state.events += input.Events

	state.temperature.add(input.Temperature)
	state.humidity.add(input.Humidity)
	state.pressure.add(input.Pressure)
}

func (
	state *metricState,
) add(
	input model.MetricAggregate,
) {
	if input.Valid > 0 {
		if state.valid == 0 {
			state.min = *input.Min
			state.max = *input.Max
		} else {
			state.min = min(
				state.min,
				*input.Min,
			)

			state.max = max(
				state.max,
				*input.Max,
			)
		}
	}

	state.valid += input.Valid
	state.invalid += input.Invalid
	state.sum += input.Sum
}

func (
	state *cloudWindowState,
) buildAggregate(
	emittedAt time.Time,
) model.CloudEdgeAggregate {
	return model.CloudEdgeAggregate{
		AggregateID: buildCloudAggregateID(
			state.edgeID,
			state.start,
			state.end,
		),
		EdgeID:          state.edgeID,
		WindowStart:     state.start,
		WindowEnd:       state.end,
		InputAggregates: state.inputAggregates,
		Events:          state.events,
		Temperature:     state.temperature.buildAggregate(),
		Humidity:        state.humidity.buildAggregate(),
		Pressure:        state.pressure.buildAggregate(),
		EmittedAt:       emittedAt,
	}
}

func (
	state metricState,
) buildAggregate() model.MetricAggregate {
	if state.valid == 0 {
		return model.MetricAggregate{
			Valid:   0,
			Invalid: state.invalid,
			Sum:     0,
			Average: nil,
			Min:     nil,
			Max:     nil,
		}
	}

	average := state.sum /
		float64(state.valid)

	minimum := state.min
	maximum := state.max

	return model.MetricAggregate{
		Valid:   state.valid,
		Invalid: state.invalid,
		Sum:     state.sum,
		Average: &average,
		Min:     &minimum,
		Max:     &maximum,
	}
}

func buildCloudAggregateID(
	edgeID string,
	windowStart time.Time,
	windowEnd time.Time,
) string {
	return fmt.Sprintf(
		"cloud:%s:%s:%s",
		edgeID,
		windowStart.UTC().Format(time.RFC3339),
		windowEnd.UTC().Format(time.RFC3339),
	)
}
