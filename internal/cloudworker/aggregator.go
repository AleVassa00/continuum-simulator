package cloudworker

import (
	"continuum/internal/model"
	"fmt"
	"reflect"
	"sort"
	"time"
)

// PartitionAggregator is owned by an input partition, never a worker.
// Single-threaded and volatile: a mid-run ownership change invalidates the run.
type PartitionAggregator struct {
	partition                int
	windowSize               time.Duration
	watermarkDelay           time.Duration
	windows                  map[time.Time]*cloudWindowState
	maxEventTimeObserved     time.Time
	watermark                time.Time
	hasObserved              bool
	ended                    bool
}

// LateRecord describes an input dropped because its Cloud window is finalized.
// Watermark is the watermark BEFORE observing this input.
// Events counts this input's events, not unique lost events: a retry of an
// already finalized input is also late and has no retained duplicate state.
type LateRecord struct {
	AggregateID    string
	EdgeID         string
	Events         uint64
	CloudWindowEnd time.Time
	Watermark      time.Time
}

type Output struct {
	SourcePartition int
	Aggregates      []model.CloudPartitionAggregate
	Progress        *model.PartitionProgress
	End             bool
	Late            *LateRecord
}

func NewPartitionAggregator(partition int, windowSize, watermarkDelay time.Duration) (*PartitionAggregator, error) {
	if err := model.ValidateSourcePartition(partition); err != nil {
		return nil, err
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf("Cloud window must be positive")
	}
	if watermarkDelay < 0 {
		return nil, fmt.Errorf("Cloud watermark delay must be nonnegative")
	}
	return &PartitionAggregator{partition: partition, windowSize: windowSize, watermarkDelay: watermarkDelay, windows: make(map[time.Time]*cloudWindowState)}, nil
}

func (a *PartitionAggregator) Add(partition int, input model.EdgeAggregate) (Output, error) {
	out := Output{SourcePartition: a.partition}
	if partition != a.partition {
		return out, fmt.Errorf("message partition %d does not match owner %d", partition, a.partition)
	}
	if a.ended {
		return out, fmt.Errorf("EdgeAggregate after source partition EOS edge=%s", input.EdgeID)
	}
	if err := ValidateEdgeAggregate(input); err != nil {
		return out, err
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
	// Check the Cloud window against the previous watermark. Delay zero admits
	// the record that advances the watermark and closes its own window.
	if a.hasObserved && !cloudEnd.After(a.watermark) {
		out.Late = &LateRecord{AggregateID: input.AggregateID, EdgeID: input.EdgeID, Events: input.Events, CloudWindowEnd: cloudEnd, Watermark: a.watermark}
		return out, nil
	}
	// Duplicate state exists only for pending windows. Searching all pending
	// windows also detects an ID reused with different window boundaries.
	for _, state := range a.windows {
		if old, ok := state.inputs[input.AggregateID]; ok {
			if !sameEdgeAggregate(old, input) {
				return out, fmt.Errorf("conflicting Edge duplicate %q", input.AggregateID)
			}
			return out, nil
		}
	}
	state := a.windows[cloudStart]
	if state == nil {
		state = &cloudWindowState{start: cloudStart, end: cloudEnd, inputs: make(map[string]model.EdgeAggregate)}
		a.windows[cloudStart] = state
	}
	state.add(input)
	if !a.hasObserved || end.After(a.maxEventTimeObserved) {
		a.maxEventTimeObserved, a.hasObserved = end, true
		a.watermark = a.maxEventTimeObserved.Add(-a.watermarkDelay)
		out.Aggregates = a.flush(false)
		out.Progress = &model.PartitionProgress{SourcePartition: a.partition, CompleteThrough: a.watermark}
	}
	return out, nil
}

// EndPartitionInput certifies that no further source inputs exist. Empty
// partitions also need this control; duplicate controls produce no output.
func (a *PartitionAggregator) EndPartitionInput(partition int) (Output, error) {
	out := Output{SourcePartition: a.partition}
	if partition != a.partition {
		return out, fmt.Errorf("EOS partition %d does not match owner %d", partition, a.partition)
	}
	if a.ended {
		return out, nil
	}
	a.ended = true
	out.Aggregates = a.flush(true)
	out.End = true
	return out, nil
}

func (a *PartitionAggregator) flush(all bool) []model.CloudPartitionAggregate {
	keys := make([]time.Time, 0, len(a.windows))
	for key, state := range a.windows {
		if all || (a.hasObserved && !state.end.After(a.watermark)) {
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
