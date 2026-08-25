package cloudworker

import (
	"fmt"
	"sort"
	"time"

	"continuum/internal/model"
)

type metricState struct {
	valid   uint64
	invalid uint64
	sum     float64
	min     float64
	max     float64
}

type cloudWindowState struct {
	edgeID string
	start  time.Time
	end    time.Time

	inputAggregates uint64
	events          uint64
	duplicateEvents uint64

	seenAggregateIDs map[string]struct{}

	temperature metricState
	humidity    metricState
	pressure    metricState
}

type WindowAggregator struct {
	windowSize time.Duration
	states     map[string]*cloudWindowState
	now        func() time.Time
}

func NewWindowAggregator(
	windowSize time.Duration,
) (*WindowAggregator, error) {
	if windowSize <= 0 {
		return nil,
			fmt.Errorf(
				"dimensione finestra Cloud deve essere maggiore di zero",
			)
	}

	return &WindowAggregator{
		windowSize: windowSize,
		states: make(
			map[string]*cloudWindowState,
		),
		now: time.Now,
	}, nil
}

func (
	aggregator *WindowAggregator,
) Add(
	input model.EdgeAggregate,
) (*model.CloudEdgeAggregate, error) {
	if err := ValidateEdgeAggregate(input); err != nil {
		return nil,
			fmt.Errorf(
				"EdgeAggregate %q non valido: %w",
				input.AggregateID,
				err,
			)
	}

	windowStart, windowEnd, err :=
		aggregator.cloudWindowFor(input)
	if err != nil {
		return nil, err
	}

	current := aggregator.states[input.EdgeID]

	if current == nil {
		current = newCloudWindowState(
			input.EdgeID,
			windowStart,
			windowEnd,
		)

		aggregator.states[input.EdgeID] = current

		current.add(input)

		return nil, nil
	}

	if windowStart.Before(current.start) {
		return nil,
			fmt.Errorf(
				"EdgeAggregate fuori ordine: edge_id=%s aggregate_id=%s window_start=%s current_window=%s",
				input.EdgeID,
				input.AggregateID,
				windowStart.Format(time.RFC3339),
				current.start.Format(time.RFC3339),
			)
	}

	if windowStart.Equal(current.start) {
		current.add(input)

		return nil, nil
	}

	emitted := current.buildAggregate(
		aggregator.now().UTC(),
	)

	next := newCloudWindowState(
		input.EdgeID,
		windowStart,
		windowEnd,
	)

	next.add(input)

	aggregator.states[input.EdgeID] = next

	return &emitted, nil
}

func (
	aggregator *WindowAggregator,
) Flush() []model.CloudEdgeAggregate {
	edgeIDs := make(
		[]string,
		0,
		len(aggregator.states),
	)

	for edgeID := range aggregator.states {
		edgeIDs = append(
			edgeIDs,
			edgeID,
		)
	}

	sort.Strings(edgeIDs)

	emittedAt := aggregator.now().UTC()
	outputs := make(
		[]model.CloudEdgeAggregate,
		0,
		len(edgeIDs),
	)

	for _, edgeID := range edgeIDs {
		state := aggregator.states[edgeID]

		if state.inputAggregates == 0 {
			continue
		}

		outputs = append(
			outputs,
			state.buildAggregate(emittedAt),
		)
	}

	clear(aggregator.states)

	return outputs
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
		seenAggregateIDs: make(
			map[string]struct{},
		),
	}
}

func (
	state *cloudWindowState,
) add(
	input model.EdgeAggregate,
) bool {
	if _, found :=
		state.seenAggregateIDs[input.AggregateID]; found {
		return false
	}

	state.seenAggregateIDs[input.AggregateID] =
		struct{}{}

	state.inputAggregates++
	state.events += input.Events
	state.duplicateEvents += input.DuplicateEvents

	state.temperature.add(input.Temperature)
	state.humidity.add(input.Humidity)
	state.pressure.add(input.Pressure)

	return true
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
		SchemaVersion: model.CloudEdgeAggregateSchemaVersion,
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
		DuplicateEvents: state.duplicateEvents,
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
