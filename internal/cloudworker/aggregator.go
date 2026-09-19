package cloudworker

import (
	"continuum/internal/model"
	"fmt"
	"reflect"
	"sort"
	"time"
)

type edgeProgress struct {
	completeThrough time.Time
	ended           bool
	last            *model.EdgeAggregate
}

// Ogni partizione possiede il proprio stato volatile.
type PartitionAggregator struct {
	partition            int
	windowSize           time.Duration
	maxEdgeWatermarkSkew time.Duration
	edges                map[string]*edgeProgress
	windows              map[time.Time]*cloudWindowState
	completeThrough      time.Time
	ended                bool
}

type Output struct {
	SourcePartition int
	Aggregates      []model.CloudPartitionAggregate
	Late            *LateAggregate
	End             bool
}

type LateAggregate struct {
	Aggregate          model.EdgeAggregate
	CloudWindowEnd     time.Time
	PartitionWatermark time.Time
}

func NewPartitionAggregator(partition, count int, members []string, windowSize, maxEdgeWatermarkSkew time.Duration) (*PartitionAggregator, error) {
	if err := model.ValidateSourcePartition(partition, count); err != nil {
		return nil, err
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf("Cloud window must be positive")
	}
	if maxEdgeWatermarkSkew <= 0 {
		return nil, fmt.Errorf("maximum Edge watermark skew must be positive")
	}
	a := &PartitionAggregator{
		partition:            partition,
		windowSize:           windowSize,
		maxEdgeWatermarkSkew: maxEdgeWatermarkSkew,
		edges:                make(map[string]*edgeProgress, len(members)),
		windows:              make(map[time.Time]*cloudWindowState),
	}
	for _, id := range members {
		if id == "" || a.edges[id] != nil {
			return nil, fmt.Errorf("invalid or duplicate Edge membership %q for partition %d", id, partition)
		}
		a.edges[id] = &edgeProgress{}
	}
	return a, nil
}

func (a *PartitionAggregator) Initialize() Output {
	return a.advance()
}

func (a *PartitionAggregator) Add(partition int, input model.EdgeAggregate) (Output, error) {
	out := Output{SourcePartition: a.partition}
	if partition != a.partition {
		return out, fmt.Errorf("message partition %d does not match owner %d", partition, a.partition)
	}
	if err := ValidateEdgeAggregate(input); err != nil {
		return out, err
	}
	edge := a.edges[input.EdgeID]
	if edge == nil {
		return out, fmt.Errorf("unexpected Edge %q in source partition %d", input.EdgeID, partition)
	}
	if a.ended || edge.ended {
		return out, fmt.Errorf("EdgeAggregate after Edge end-of-input edge=%s", input.EdgeID)
	}

	start, end := input.WindowStart.UTC(), input.WindowEnd.UTC()
	through := input.CompleteThrough.UTC()
	size := end.Sub(start)
	if a.windowSize%size != 0 || !start.Equal(start.Truncate(size)) || !through.Equal(through.Truncate(size)) {
		return out, fmt.Errorf("Edge window is not an aligned divisor of Cloud window")
	}
	cloudStart := start.Truncate(a.windowSize)
	cloudEnd := cloudStart.Add(a.windowSize)
	if end.After(cloudEnd) {
		return out, fmt.Errorf("Edge window crosses Cloud boundary")
	}

	for _, state := range a.windows {
		if old, ok := state.inputs[input.AggregateID]; ok {
			if !sameEdgeAggregate(old, input) {
				return out, fmt.Errorf("conflicting Edge duplicate %q", input.AggregateID)
			}
			return out, nil
		}
	}
	if edge.last != nil && edge.last.AggregateID == input.AggregateID {
		if !sameEdgeAggregate(*edge.last, input) {
			return out, fmt.Errorf("conflicting Edge duplicate %q", input.AggregateID)
		}
		return out, nil
	}

	// Un record late non riapre una finestra già emessa.
	if !a.completeThrough.IsZero() && !cloudEnd.After(a.completeThrough) {
		if through.After(edge.completeThrough) {
			edge.completeThrough = through
		}
		if edge.last == nil || !start.Before(edge.last.WindowEnd) {
			copy := input
			edge.last = &copy
		}
		out = a.advance()
		out.Late = &LateAggregate{Aggregate: input, CloudWindowEnd: cloudEnd, PartitionWatermark: a.completeThrough}
		return out, nil
	}
	if edge.last != nil && start.Before(edge.last.WindowEnd) {
		return out, fmt.Errorf("out-of-order/overlapping EdgeAggregate edge=%s", input.EdgeID)
	}
	if through.Before(edge.completeThrough) {
		return out, fmt.Errorf("embedded progress regressed for Edge %s", input.EdgeID)
	}
	if !edge.completeThrough.IsZero() && !end.After(edge.completeThrough) {
		return out, fmt.Errorf("EdgeAggregate edge=%s behind its certified progress %s", input.EdgeID, edge.completeThrough.Format(time.RFC3339Nano))
	}

	state := a.windows[cloudStart]
	if state == nil {
		state = &cloudWindowState{start: cloudStart, end: cloudEnd, inputs: make(map[string]model.EdgeAggregate)}
		a.windows[cloudStart] = state
	}
	state.add(input)
	copy := input
	edge.last = &copy
	edge.completeThrough = through
	return a.advance(), nil
}

func (a *PartitionAggregator) EndEdge(partition int, edgeID string) (Output, error) {
	out := Output{SourcePartition: a.partition}
	if partition != a.partition {
		return out, fmt.Errorf("Edge end partition %d does not match owner %d", partition, a.partition)
	}
	edge := a.edges[edgeID]
	if edge == nil {
		return out, fmt.Errorf("end-of-input from unexpected Edge %q in partition %d", edgeID, partition)
	}
	if edge.ended {
		return out, nil
	}
	if a.ended {
		return out, fmt.Errorf("Edge end-of-input after partition completion edge=%s", edgeID)
	}
	edge.ended = true
	return a.advance(), nil
}

func (a *PartitionAggregator) advance() Output {
	out := Output{SourcePartition: a.partition}
	if a.ended {
		return out
	}

	allEnded := true
	var minimum, maximum time.Time
	for _, edge := range a.edges {
		if edge.ended {
			continue
		}
		allEnded = false
		if edge.completeThrough.IsZero() {
			return out
		}
		if minimum.IsZero() || edge.completeThrough.Before(minimum) {
			minimum = edge.completeThrough
		}
		if maximum.IsZero() || edge.completeThrough.After(maximum) {
			maximum = edge.completeThrough
		}
	}

	frontier := minimum
	if !allEnded {
		// Lo skew limita quanto una sorgente lenta può trattenere la frontiera.
		boundedFrontier := maximum.Add(-a.maxEdgeWatermarkSkew)
		if boundedFrontier.After(frontier) {
			frontier = boundedFrontier
		}
	}
	out.Aggregates = a.flush(allEnded, frontier)
	if allEnded {
		a.ended = true
		out.End = true
		return out
	}
	if frontier.After(a.completeThrough) {
		a.completeThrough = frontier
	}
	return out
}

func (a *PartitionAggregator) flush(all bool, frontier time.Time) []model.CloudPartitionAggregate {
	keys := make([]time.Time, 0, len(a.windows))
	for key, state := range a.windows {
		if all || !state.end.After(frontier) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Before(keys[j]) })
	var aggregates []model.CloudPartitionAggregate
	for index, key := range keys {
		state := a.windows[key]
		// Solo l'ultimo output può certificare l'intera frontiera.
		through := state.end
		if index == len(keys)-1 && frontier.After(through) {
			through = frontier
		}
		aggregates = append(aggregates, state.buildAggregate(a.partition, through))
		delete(a.windows, key)
	}
	return aggregates
}

func sameEdgeAggregate(a, b model.EdgeAggregate) bool {
	a.EmittedAt = time.Time{}
	b.EmittedAt = time.Time{}
	a.WindowStart = a.WindowStart.UTC()
	b.WindowStart = b.WindowStart.UTC()
	a.WindowEnd = a.WindowEnd.UTC()
	b.WindowEnd = b.WindowEnd.UTC()
	a.CompleteThrough = a.CompleteThrough.UTC()
	b.CompleteThrough = b.CompleteThrough.UTC()
	return reflect.DeepEqual(a, b)
}
