package cloudworker

import (
	"fmt"
	"sort"
	"time"

	"continuum/internal/model"
)

type WindowAggregator struct {
	windowSize time.Duration
	states     map[string]*cloudWindowState
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
	if current != nil {
		if _, seen := current.seenAggregateIDs[input.AggregateID]; seen {
			return nil, nil
		}
	}

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
		time.Now().UTC(),
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

	emittedAt := time.Now().UTC()
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
) FlushEdge(
	edgeID string,
) (*model.CloudEdgeAggregate, bool) {
	state, found := aggregator.states[edgeID]
	if !found {
		return nil, false
	}

	delete(aggregator.states, edgeID)
	if state.inputAggregates == 0 {
		return nil, false
	}

	output := state.buildAggregate(
		time.Now().UTC(),
	)

	return &output, true
}
