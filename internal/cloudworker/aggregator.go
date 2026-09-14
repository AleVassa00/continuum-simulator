package cloudworker

import (
	"continuum/internal/model"
	"fmt"
	"reflect"
	"sort"
	"time"
)

type edgeProgress struct {
	watermark time.Time
	ended     bool
	last      *model.EdgeAggregate
}

// PartitionAggregator is owned by an input partition, never a worker.
// Single-threaded and volatile: a mid-run ownership change invalidates the run.
type PartitionAggregator struct {
	partition       int
	windowSize      time.Duration
	edges           map[string]*edgeProgress
	windows         map[time.Time]*cloudWindowState
	completeThrough time.Time
	ended           bool
}

type Output struct {
	SourcePartition int
	Aggregates      []model.CloudPartitionAggregate
	Progress        *model.PartitionProgress
	End             bool
}

func NewPartitionAggregator(partition, count int, members []string, windowSize time.Duration) (*PartitionAggregator, error) {
	if err := model.ValidateSourcePartition(partition, count); err != nil {
		return nil, err
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf("Cloud window must be positive")
	}
	a := &PartitionAggregator{
		partition:  partition,
		windowSize: windowSize,
		edges:      make(map[string]*edgeProgress, len(members)),
		windows:    make(map[time.Time]*cloudWindowState),
	}
	for _, id := range members {
		if id == "" || a.edges[id] != nil {
			return nil, fmt.Errorf("invalid or duplicate Edge membership %q for partition %d", id, partition)
		}
		a.edges[id] = &edgeProgress{}
	}
	return a, nil
}

// Initialize certifies a partition with no configured producers as terminal.
// It is called exactly by the consumer-group owner of that partition.
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
	size := end.Sub(start)
	if a.windowSize%size != 0 || !start.Equal(start.Truncate(size)) {
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
	if edge.last != nil && start.Before(edge.last.WindowEnd) {
		return out, fmt.Errorf("out-of-order/overlapping EdgeAggregate edge=%s", input.EdgeID)
	}
	if !edge.watermark.IsZero() && !end.After(edge.watermark) {
		return out, fmt.Errorf("EdgeAggregate edge=%s behind its certified watermark %s", input.EdgeID, edge.watermark.Format(time.RFC3339Nano))
	}
	if !a.completeThrough.IsZero() && !cloudEnd.After(a.completeThrough) {
		return out, fmt.Errorf("EdgeAggregate edge=%s behind partition watermark %s", input.EdgeID, a.completeThrough.Format(time.RFC3339Nano))
	}

	state := a.windows[cloudStart]
	if state == nil {
		state = &cloudWindowState{start: cloudStart, end: cloudEnd, inputs: make(map[string]model.EdgeAggregate)}
		a.windows[cloudStart] = state
	}
	state.add(input)
	copy := input
	edge.last = &copy
	return out, nil
}

func (a *PartitionAggregator) AdvanceEdgeWatermark(partition int, input model.EdgeWatermark) (Output, error) {
	out := Output{SourcePartition: a.partition}
	if partition != a.partition {
		return out, fmt.Errorf("watermark partition %d does not match owner %d", partition, a.partition)
	}
	if err := model.ValidateEdgeWatermark(input); err != nil {
		return out, err
	}
	edge := a.edges[input.EdgeID]
	if edge == nil {
		return out, fmt.Errorf("watermark from unexpected Edge %q in partition %d", input.EdgeID, partition)
	}
	if a.ended || edge.ended {
		return out, fmt.Errorf("watermark after Edge end-of-input edge=%s", input.EdgeID)
	}
	through := input.CompleteThrough.UTC()
	if through.Before(edge.watermark) {
		return out, fmt.Errorf("watermark regressed for Edge %s", input.EdgeID)
	}
	if through.Equal(edge.watermark) {
		return out, nil
	}
	edge.watermark = through
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
	var frontier time.Time
	for _, edge := range a.edges {
		if edge.ended {
			continue
		}
		allEnded = false
		if edge.watermark.IsZero() {
			return out
		}
		if frontier.IsZero() || edge.watermark.Before(frontier) {
			frontier = edge.watermark
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
		out.Progress = &model.PartitionProgress{SourcePartition: a.partition, CompleteThrough: frontier}
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
	for _, key := range keys {
		aggregates = append(aggregates, a.windows[key].buildAggregate(a.partition))
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
	return reflect.DeepEqual(a, b)
}
