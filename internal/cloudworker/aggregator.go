package cloudworker

import (
	"fmt"
	"sort"
	"time"

	"continuum/internal/model"
)

// WindowAggregator aggrega EdgeAggregate in finestre Cloud per singolo Edge.
//
// Limitazione architetturale: lo stato delle finestre vive esclusivamente in RAM
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
	// Determiniamo a quale finestra Cloud appartiene l'aggregato Edge
	windowStart, windowEnd, err := aggregator.cloudWindowFor(input)
	if err != nil {
		return nil, err
	}

	current := aggregator.states[input.EdgeID]
	if current != nil {
		// Verifico se l'aggregato è già stato elaborato, dato il delivery at-least-once
		if _, seen := current.seenAggregateIDs[input.AggregateID]; seen {
			return nil, nil
		}
	}
	// se ancora non ho uno stato per quell'edge lo creo
	if current == nil {
		current = newCloudWindowState(input.EdgeID, windowStart, windowEnd)
		// salvo lo stato nella mappa
		aggregator.states[input.EdgeID] = current
		// aggiungo l'aggregato appena arrivato
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
	// Se l'aggregato appartiene a una finestra Cloud successiva, emetto l'aggregato costruito dalla finestra corrente e apro il nuovo stato
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

func (aggregator *WindowAggregator) FlushEdge(edgeID string) *model.CloudEdgeAggregate {
	state := aggregator.states[edgeID]
	if state == nil {
		return nil
	}

	delete(aggregator.states, edgeID)

	output := state.buildAggregate(time.Now().UTC())

	return &output
}
