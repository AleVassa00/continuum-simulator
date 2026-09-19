package globalaggregator

import (
	"context"
	"continuum/internal/model"
	"fmt"
	"reflect"
	"time"
)

type GlobalAggregateSink func(context.Context, model.GlobalAggregate) error

const DefaultMaxPartitionWatermarkSkew = 30 * time.Minute

type partitionState struct {
	through time.Time
	ended   bool
	last    *model.CloudPartitionAggregate
}

// Riduce le finestre prodotte dalle partizioni Cloud.
type Aggregator struct {
	partitions                []partitionState
	windows                   map[windowKey]*windowState
	sink                      GlobalAggregateSink
	maxPartitionWatermarkSkew time.Duration
	completeThrough           time.Time
	complete                  bool
}

func New(count int, sink GlobalAggregateSink) (*Aggregator, error) {
	return NewWithMaxPartitionWatermarkSkew(count, DefaultMaxPartitionWatermarkSkew, sink)
}

func NewWithMaxPartitionWatermarkSkew(count int, maxPartitionWatermarkSkew time.Duration, sink GlobalAggregateSink) (*Aggregator, error) {
	if count <= 0 {
		return nil, fmt.Errorf("source partition count must be positive")
	}
	if maxPartitionWatermarkSkew <= 0 {
		return nil, fmt.Errorf("maximum partition watermark skew must be positive")
	}
	if sink == nil {
		return nil, fmt.Errorf("Global sink is required")
	}
	return &Aggregator{
		partitions:                make([]partitionState, count),
		windows:                   make(map[windowKey]*windowState),
		sink:                      sink,
		maxPartitionWatermarkSkew: maxPartitionWatermarkSkew,
	}, nil
}

type LatePartitionAggregate struct {
	Aggregate       model.CloudPartitionAggregate
	GlobalWatermark time.Time
}

func (a *Aggregator) Add(ctx context.Context, input model.CloudPartitionAggregate) error {
	_, err := a.AddWithResult(ctx, input)
	return err
}

// AddWithResult unisce un partial oppure lo segnala come late
func (a *Aggregator) AddWithResult(ctx context.Context, input model.CloudPartitionAggregate) (*LatePartitionAggregate, error) {
	if err := model.ValidateCloudPartitionAggregate(input); err != nil {
		return nil, err
	}
	if err := model.ValidateSourcePartition(input.SourcePartition, len(a.partitions)); err != nil {
		return nil, err
	}
	p := &a.partitions[input.SourcePartition]
	key := makeWindowKey(input.WindowStart, input.WindowEnd)
	if s := a.windows[key]; s != nil {
		if old, ok := s.contributors[input.SourcePartition]; ok {
			if !samePartial(old, input) {
				return nil, fmt.Errorf("conflicting partial %q", input.AggregateID)
			}
			return nil, a.emitReady(ctx)
		}
	}
	if p.last != nil && p.last.AggregateID == input.AggregateID {
		if !samePartial(*p.last, input) {
			return nil, fmt.Errorf("conflicting partial %q", input.AggregateID)
		}
		return nil, a.emitReady(ctx)
	}
	if p.ended || a.complete {
		return nil, fmt.Errorf("partition partial after EOS")
	}
	// Una finestra già emessa non viene riaperta.
	if !a.completeThrough.IsZero() && !input.WindowEnd.After(a.completeThrough) {
		if input.CompleteThrough.Before(p.through) {
			return nil, fmt.Errorf("partition progress regressed")
		}
		if input.CompleteThrough.After(p.through) {
			p.through = input.CompleteThrough.UTC()
		}
		if p.last == nil || !input.WindowStart.Before(p.last.WindowEnd) {
			copy := input
			p.last = &copy
		}
		a.advanceWatermark()
		if err := a.emitReady(ctx); err != nil {
			return nil, err
		}
		return &LatePartitionAggregate{Aggregate: input, GlobalWatermark: a.completeThrough}, nil
	}
	if !p.through.IsZero() && !input.WindowEnd.After(p.through) {
		return nil, fmt.Errorf("partial behind certified partition progress")
	}
	if input.CompleteThrough.Before(p.through) {
		return nil, fmt.Errorf("partition progress regressed")
	}
	if p.last != nil && input.WindowStart.Before(p.last.WindowEnd) {
		return nil, fmt.Errorf("out-of-order/overlapping partition partial")
	}
	for k := range a.windows {
		if k != key && key.start < k.end && k.start < key.end {
			return nil, fmt.Errorf("overlapping nonidentical Cloud windows")
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
	if input.CompleteThrough.After(p.through) {
		p.through = input.CompleteThrough.UTC()
	}
	a.advanceWatermark()
	return nil, a.emitReady(ctx)
}

// L'EOS esclude la partizione dal calcolo della frontiera
func (a *Aggregator) EndPartition(ctx context.Context, partition int) (bool, error) {
	if err := model.ValidateSourcePartition(partition, len(a.partitions)); err != nil {
		return false, err
	}
	a.partitions[partition].ended = true
	a.advanceWatermark()
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

// Usa lo stesso criterio bounded applicato tra Edge e Cloud
func (a *Aggregator) advanceWatermark() {
	var minimum, maximum time.Time
	active := false
	for _, partition := range a.partitions {
		if partition.ended {
			continue
		}
		active = true
		if partition.through.IsZero() {
			return
		}
		if minimum.IsZero() || partition.through.Before(minimum) {
			minimum = partition.through
		}
		if maximum.IsZero() || partition.through.After(maximum) {
			maximum = partition.through
		}
	}
	if !active {
		return
	}
	frontier := minimum
	boundedFrontier := maximum.Add(-a.maxPartitionWatermarkSkew)
	if boundedFrontier.After(frontier) {
		frontier = boundedFrontier
	}
	if frontier.After(a.completeThrough) {
		a.completeThrough = frontier
	}
}

// Emette le finestre complete in ordine temporale
func (a *Aggregator) emitReady(ctx context.Context) error {
	for _, key := range a.sortedOpenWindowKeys() {
		s := a.windows[key]
		ready := true
		for id, p := range a.partitions {
			_, contributed := s.contributors[id]
			globallyComplete := !a.completeThrough.IsZero() && !a.completeThrough.Before(s.end)
			if !contributed && !p.ended && !globallyComplete && (p.through.IsZero() || p.through.Before(s.end)) {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		// Il progresso certifica le partizioni senza contributi per la finestra.
		out := s.buildAggregate(uint64(len(a.partitions)), time.Now().UTC())
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
	a.CompleteThrough = a.CompleteThrough.UTC()
	b.CompleteThrough = b.CompleteThrough.UTC()
	return reflect.DeepEqual(a, b)
}
