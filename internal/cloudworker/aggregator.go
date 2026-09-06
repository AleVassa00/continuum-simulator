package cloudworker

import (
	"fmt"
	"sort"
	"time"

	"continuum/internal/model"
)

// WindowAggregator aggrega EdgeAggregate in finestre Cloud per singolo Edge.
//
// Limitazione architetturale: lo stato delle finestre vive esclusivamente in
// RAM, mentre gli offset Kafka vengono committati dopo l'elaborazione di ciascun
// record. Un crash del processo può quindi perdere stato derivato da record il
// cui offset è già stato committato: al riavvio tali record non vengono
// riconsumati e la finestra parziale non è ricostruibile. Una soluzione completa
// richiederebbe un checkpoint coordinato tra offset e stato applicativo.
type WindowAggregator struct {
	windowSize time.Duration
	states     map[string]*cloudWindowState
}

func NewWindowAggregator(windowSize time.Duration) *WindowAggregator {
	return &WindowAggregator{
		windowSize: windowSize,
		states:     make(map[string]*cloudWindowState),
	}
}

// Add incorpora un EdgeAggregate già validato al confine Kafka.
func (aggregator *WindowAggregator) Add(input model.EdgeAggregate) (*model.CloudEdgeAggregate, error) {
	windowStart, windowEnd, err := aggregator.cloudWindowFor(input)
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
		current = newCloudWindowState(input.EdgeID, windowStart, windowEnd)
		aggregator.states[input.EdgeID] = current
		current.add(input)
		return nil, nil
	}

	if windowStart.Before(current.start) {
		return nil, fmt.Errorf("EdgeAggregate fuori ordine: edge_id=%s aggregate_id=%s window_start=%s current_window=%s", input.EdgeID, input.AggregateID, windowStart.Format(time.RFC3339), current.start.Format(time.RFC3339))
	}

	if windowStart.Equal(current.start) {
		current.add(input)

		return nil, nil
	}

	emitted := current.buildAggregate(time.Now().UTC())

	next := newCloudWindowState(input.EdgeID, windowStart, windowEnd)

	next.add(input)

	aggregator.states[input.EdgeID] = next

	return &emitted, nil
}

func (aggregator *WindowAggregator) Flush() []model.CloudEdgeAggregate {
	edgeIDs := make([]string, 0, len(aggregator.states))

	for edgeID := range aggregator.states {
		edgeIDs = append(edgeIDs, edgeID)
	}

	sort.Strings(edgeIDs)

	emittedAt := time.Now().UTC()
	outputs := make([]model.CloudEdgeAggregate, 0, len(edgeIDs))

	for _, edgeID := range edgeIDs {
		state := aggregator.states[edgeID]

		if state.inputAggregates == 0 {
			continue
		}

		outputs = append(outputs, state.buildAggregate(emittedAt))
	}

	clear(aggregator.states)

	return outputs
}

func (aggregator *WindowAggregator) FlushEdge(edgeID string) (*model.CloudEdgeAggregate, bool) {
	state, found := aggregator.states[edgeID]
	if !found {
		return nil, false
	}

	delete(aggregator.states, edgeID)
	if state.inputAggregates == 0 {
		return nil, false
	}

	output := state.buildAggregate(time.Now().UTC())

	return &output, true
}
