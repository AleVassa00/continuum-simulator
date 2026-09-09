package globalaggregator

import (
	"context"
	"continuum/internal/model"
	"fmt"
	"reflect"
	"time"
)

type GlobalAggregateSink func(context.Context, model.GlobalAggregate) error
type partitionState struct {
	through time.Time
	ended   bool
	last    *model.CloudPartitionAggregate
}

// Exact-window reducer: no Edge membership, timers, or event-time window policy.
type Aggregator struct {
	partitions [model.SourcePartitionCount]partitionState
	windows    map[windowKey]*windowState
	sink       GlobalAggregateSink
	complete   bool
}

func New(sink GlobalAggregateSink) (*Aggregator, error) {
	if sink == nil {
		return nil, fmt.Errorf("Global sink is required")
	}
	return &Aggregator{windows: make(map[windowKey]*windowState), sink: sink}, nil
}
func (a *Aggregator) Add(ctx context.Context, input model.CloudPartitionAggregate) error {
	if err := model.ValidateCloudPartitionAggregate(input); err != nil {
		return err
	}
	p := &a.partitions[input.SourcePartition]
	key := makeWindowKey(input.WindowStart, input.WindowEnd)
	if s := a.windows[key]; s != nil {
		if old, ok := s.contributors[input.SourcePartition]; ok {
			if !samePartial(old, input) {
				return fmt.Errorf("conflicting partial %q", input.AggregateID)
			}
			return a.emitReady(ctx)
		}
	}
	if p.last != nil && p.last.AggregateID == input.AggregateID {
		if !samePartial(*p.last, input) {
			return fmt.Errorf("conflicting partial %q", input.AggregateID)
		}
		return a.emitReady(ctx)
	}
	if p.ended || a.complete {
		return fmt.Errorf("partition partial after EOS")
	}
	if !p.through.IsZero() && !input.WindowEnd.After(p.through) {
		return fmt.Errorf("partial behind certified partition progress")
	}
	if p.last != nil && input.WindowStart.Before(p.last.WindowEnd) {
		return fmt.Errorf("out-of-order/overlapping partition partial")
	}
	for k := range a.windows {
		if k != key && key.start < k.end && k.start < key.end {
			return fmt.Errorf("overlapping nonidentical Cloud windows")
		}
	}
	s := a.windows[key]
	if s == nil {
		s = &windowState{start: input.WindowStart.UTC(), end: input.WindowEnd.UTC(), contributors: make(map[int]model.CloudPartitionAggregate)}
		a.windows[key] = s
	}
	s.add(input)
	copy := input
	p.last = &copy
	return a.emitReady(ctx)
}
func (a *Aggregator) Progress(ctx context.Context, progress model.PartitionProgress) error {
	if err := model.ValidatePartitionProgress(progress); err != nil {
		return err
	}
	p := &a.partitions[progress.SourcePartition]
	if p.ended {
		return fmt.Errorf("partition progress after EOS")
	}
	if progress.CompleteThrough.Before(p.through) {
		return fmt.Errorf("partition progress regressed")
	}
	p.through = progress.CompleteThrough.UTC()
	return a.emitReady(ctx)
}
func (a *Aggregator) EndPartition(ctx context.Context, partition int) (bool, error) {
	if err := model.ValidateSourcePartition(partition); err != nil {
		return false, err
	}
	a.partitions[partition].ended = true
	if err := a.emitReady(ctx); err != nil {
		return false, err
	}
	for _, p := range a.partitions {
		if !p.ended {
			return false, nil
		}
	}
	if len(a.windows) != 0 {
		return false, fmt.Errorf("unresolved windows after all partition EOS")
	}
	a.complete = true
	return true, nil
}
func (a *Aggregator) IsComplete() bool { return a.complete }
func (a *Aggregator) emitReady(ctx context.Context) error {
	for _, key := range a.sortedOpenWindowKeys() {
		s := a.windows[key]
		ready := true
		for id, p := range a.partitions {
			_, contributed := s.contributors[id]
			if !contributed && !p.ended && (p.through.IsZero() || p.through.Before(s.end)) {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		// Missing partials count as zero ONLY after ordered progress/EOS certificates.
		out := s.buildAggregate(model.SourcePartitionCount, time.Now().UTC())
		if err := ValidateGlobalAggregate(out); err != nil {
			return err
		}
		if err := a.sink(ctx, out); err != nil {
			return err
		}
		delete(a.windows, key)
	}
	return nil
}
func samePartial(a, b model.CloudPartitionAggregate) bool {
	a.EmittedAt = time.Time{}
	b.EmittedAt = time.Time{}
	a.WindowStart = a.WindowStart.UTC()
	b.WindowStart = b.WindowStart.UTC()
	a.WindowEnd = a.WindowEnd.UTC()
	b.WindowEnd = b.WindowEnd.UTC()
	return reflect.DeepEqual(a, b)
}
