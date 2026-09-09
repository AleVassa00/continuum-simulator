package cloudworker

import (
	"continuum/internal/model"
	"fmt"
	"reflect"
	"sort"
	"time"
)

type edgeProgress struct {
	ended bool
	last  *model.EdgeAggregate
}

// PartitionAggregator is owned by an input partition, never a worker.
// Single-threaded and volatile: a mid-run ownership change invalidates the run.
type PartitionAggregator struct {
	partition       int
	windowSize      time.Duration
	edges           map[string]*edgeProgress
	windows         map[int64]*cloudWindowState
	completeThrough time.Time
	ended           bool
}
type Output struct {
	SourcePartition int
	Aggregates      []model.CloudPartitionAggregate
	Progress        *model.PartitionProgress
	End             bool
}

func NewPartitionAggregator(partition int, members []string, windowSize time.Duration) (*PartitionAggregator, error) {
	if err := model.ValidateSourcePartition(partition); err != nil {
		return nil, err
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf("Cloud window must be positive")
	}
	a := &PartitionAggregator{partition: partition, windowSize: windowSize, edges: make(map[string]*edgeProgress), windows: make(map[int64]*cloudWindowState)}
	for _, id := range members {
		if id == "" || a.edges[id] != nil || PartitionForEdge(id) != partition {
			return nil, fmt.Errorf("invalid membership %q for partition %d", id, partition)
		}
		a.edges[id] = &edgeProgress{}
	}
	return a, nil
}

// Initialize certifies a partition without configured producers as terminal.
// Called only by its consumer group owner.
func (a *PartitionAggregator) Initialize() Output { return a.advance() }
func (a *PartitionAggregator) Add(partition int, input model.EdgeAggregate) (Output, error) {
	empty := Output{SourcePartition: a.partition}
	if partition != a.partition {
		return empty, fmt.Errorf("message partition %d does not match owner %d", partition, a.partition)
	}
	if err := ValidateEdgeAggregate(input); err != nil {
		return empty, err
	}
	edge := a.edges[input.EdgeID]
	if edge == nil {
		return empty, fmt.Errorf("unexpected Edge %q in actual partition %d", input.EdgeID, partition)
	}
	start, end := input.WindowStart.UTC(), input.WindowEnd.UTC()
	size := end.Sub(start)
	if a.windowSize%size != 0 || !start.Equal(start.Truncate(size)) {
		return empty, fmt.Errorf("Edge window is not an aligned divisor of Cloud window")
	}
	cloudStart := start.Truncate(a.windowSize)
	if end.After(cloudStart.Add(a.windowSize)) {
		return empty, fmt.Errorf("Edge window crosses Cloud boundary")
	}
	// Dedup lives with pending windows, plus one last record per Edge.
	if state := a.windows[cloudStart.UnixNano()]; state != nil {
		if old, ok := state.inputs[input.AggregateID]; ok {
			if !sameEdgeAggregate(old, input) {
				return empty, fmt.Errorf("conflicting Edge duplicate %q", input.AggregateID)
			}
			return empty, nil
		}
	}
	if edge.last != nil && edge.last.AggregateID == input.AggregateID {
		if !sameEdgeAggregate(*edge.last, input) {
			return empty, fmt.Errorf("conflicting Edge duplicate %q", input.AggregateID)
		}
		return empty, nil
	}
	if edge.ended || a.ended {
		return empty, fmt.Errorf("EdgeAggregate after EOS edge=%s", input.EdgeID)
	}
	if edge.last != nil && start.Before(edge.last.WindowEnd) {
		return empty, fmt.Errorf("late/overlapping EdgeAggregate edge=%s: invalid run", input.EdgeID)
	}
	if !a.completeThrough.IsZero() && !end.After(a.completeThrough) {
		return empty, fmt.Errorf("EdgeAggregate behind partition progress")
	}
	state := a.windows[cloudStart.UnixNano()]
	if state == nil {
		state = &cloudWindowState{start: cloudStart, end: cloudStart.Add(a.windowSize), inputs: make(map[string]model.EdgeAggregate)}
		a.windows[cloudStart.UnixNano()] = state
	}
	state.add(input)
	copy := input
	edge.last = &copy
	return a.advance(), nil
}
func (a *PartitionAggregator) EndEdge(partition int, edgeID string) (Output, error) {
	empty := Output{SourcePartition: a.partition}
	if partition != a.partition {
		return empty, fmt.Errorf("EOS partition differs from actual owner")
	}
	edge := a.edges[edgeID]
	if edge == nil {
		return empty, fmt.Errorf("EOS from unexpected Edge %q in partition %d", edgeID, partition)
	}
	if edge.ended {
		return empty, nil
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
	var frontier time.Time
	for _, edge := range a.edges {
		if edge.ended {
			continue
		}
		allEnded = false
		// Silence and another Edge's data cannot certify this Edge's progress.
		if edge.last == nil {
			return out
		}
		bound := edge.last.WindowEnd.UTC().Truncate(a.windowSize)
		if frontier.IsZero() || bound.Before(frontier) {
			frontier = bound
		}
	}
	keys := make([]int64, 0, len(a.windows))
	for key, state := range a.windows {
		if allEnded || !state.end.After(frontier) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		out.Aggregates = append(out.Aggregates, a.windows[key].buildAggregate(a.partition))
		delete(a.windows, key)
	}
	if allEnded {
		a.ended = true
		out.End = true
	} else if frontier.After(a.completeThrough) {
		a.completeThrough = frontier
		out.Progress = &model.PartitionProgress{SourcePartition: a.partition, CompleteThrough: frontier}
	}
	return out
}
func sameEdgeAggregate(a, b model.EdgeAggregate) bool {
	a.EmittedAt = time.Time{}
	b.EmittedAt = time.Time{}
	a.WindowStart = a.WindowStart.UTC()
	b.WindowStart = b.WindowStart.UTC()
	a.WindowEnd = a.WindowEnd.UTC()
	b.WindowEnd = b.WindowEnd.UTC()
	return reflect.DeepEqual(a, b)
}
