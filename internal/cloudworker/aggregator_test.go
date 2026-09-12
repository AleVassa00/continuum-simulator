package cloudworker

import (
	"continuum/internal/model"
	"fmt"
	"math"
	"testing"
	"time"
)

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func fixtureEdge(id string, minute int) model.EdgeAggregate {
	start := epoch.Add(time.Duration(minute) * time.Minute)
	v := 10.0
	m := model.MetricAggregate{Valid: 1, Sum: v, Average: &v, Min: &v, Max: &v}
	return model.EdgeAggregate{AggregateID: fmt.Sprintf("%s:%d", id, minute), EdgeID: id, WindowStart: start, WindowEnd: start.Add(5 * time.Minute), Events: 1, Temperature: m, Humidity: m, Pressure: m, EmittedAt: epoch}
}

func newAggregator(t *testing.T, partition int, delay time.Duration) *PartitionAggregator {
	t.Helper()
	a, err := NewPartitionAggregator(partition, DefaultWindowSize, delay)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func add(t *testing.T, a *PartitionAggregator, input model.EdgeAggregate) Output {
	t.Helper()
	out, err := a.Add(a.partition, input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWatermarkUsesMaxInputEndAndAcceptsOutOfOrder(t *testing.T) {
	a := newAggregator(t, 0, DefaultWatermarkDelay)
	first := fixtureEdge("fast", 10)
	first.WindowStart = first.WindowStart.In(time.FixedZone("offset", 3600))
	first.WindowEnd = first.WindowEnd.In(time.FixedZone("offset", 3600))
	out := add(t, a, first)
	if out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(10*time.Minute)) || out.Progress.CompleteThrough.Location() != time.UTC || len(out.Aggregates) != 0 {
		t.Fatalf("initial watermark: %+v", out)
	}
	// The input end is behind the watermark, but its CLOUD end is ahead.
	// Both a previously unseen edge and the same edge may arrive out of order.
	for _, input := range []model.EdgeAggregate{fixtureEdge("slow", 0), fixtureEdge("fast", 5), fixtureEdge("fast", 0)} {
		out = add(t, a, input)
		if out.Progress != nil || out.Late != nil || len(out.Aggregates) != 0 {
			t.Fatalf("pending out-of-order input changed watermark or was lost: %+v", out)
		}
	}
	out = add(t, a, fixtureEdge("fast", 15))
	if len(out.Aggregates) != 1 || out.Aggregates[0].Events != 4 || out.Aggregates[0].InputAggregates != 4 || out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(15*time.Minute)) {
		t.Fatalf("watermark boundary: %+v", out)
	}
	if len(a.windows) != 1 {
		t.Fatal("finalized state retained or next window lost")
	}
	// A slow edge cannot hold back another edge's partition watermark.
	out = add(t, a, fixtureEdge("slow", 10))
	if out.Late == nil || out.Late.EdgeID != "slow" || out.Late.Events != 1 || !out.Late.CloudWindowEnd.Equal(epoch.Add(15*time.Minute)) || !out.Late.Watermark.Equal(a.watermark) || out.Progress != nil || out.End || len(out.Aggregates) != 0 {
		t.Fatalf("expected explicit late-drop signal: %+v", out)
	}
}

func TestDelayZeroAdmitsBoundaryThenDropsClosedRetries(t *testing.T) {
	a := newAggregator(t, 2, 0)
	add(t, a, fixtureEdge("arbitrary", 0))
	boundary := fixtureEdge("arbitrary", 10)
	out := add(t, a, boundary)
	if out.Late != nil || len(out.Aggregates) != 1 || out.Aggregates[0].Events != 2 || out.Progress == nil || !out.Progress.CompleteThrough.Equal(boundary.WindowEnd) {
		t.Fatalf("boundary input must be included before flush: %+v", out)
	}
	for _, input := range []model.EdgeAggregate{boundary, fixtureEdge("other", 5)} {
		if out = add(t, a, input); out.Late == nil || out.Progress != nil || len(out.Aggregates) != 0 {
			t.Fatalf("closed-window record must drop: %+v", out)
		}
	}
	if len(a.windows) != 0 {
		t.Fatal("late record recreated a closed window")
	}
	if out = add(t, a, fixtureEdge("other", 15)); out.Late != nil || out.Progress == nil {
		t.Fatal("late input prevented continued processing")
	}
}

func TestWatermarkIsUnroundedAndIndependentPerPartition(t *testing.T) {
	a := newAggregator(t, 0, 2*time.Minute)
	b := newAggregator(t, 1, 2*time.Minute)
	out := add(t, a, fixtureEdge("edge", 10))
	if out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(13*time.Minute)) || len(out.Aggregates) != 0 {
		t.Fatalf("watermark was rounded: %+v", out)
	}
	out = add(t, b, fixtureEdge("edge", 0))
	if out.Progress == nil || out.Progress.SourcePartition != 1 || !out.Progress.CompleteThrough.Equal(epoch.Add(3*time.Minute)) {
		t.Fatalf("partitions share progress: %+v", out)
	}
	out = add(t, a, fixtureEdge("edge", 60))
	if out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(63*time.Minute)) || len(out.Aggregates) != 1 {
		t.Fatalf("gap progress or flush incorrect: %+v", out)
	}
}

func TestPendingDuplicatesAndConflicts(t *testing.T) {
	a := newAggregator(t, 0, time.Hour)
	input := fixtureEdge("edge", 0)
	add(t, a, input)
	add(t, a, fixtureEdge("edge", 15))
	retry := input
	retry.EmittedAt = epoch.Add(time.Hour)
	retry.WindowStart = retry.WindowStart.In(time.FixedZone("offset", 3600))
	retry.WindowEnd = retry.WindowEnd.In(time.FixedZone("offset", 3600))
	if out := add(t, a, retry); out.Progress != nil || out.Late != nil || len(out.Aggregates) != 0 {
		t.Fatalf("non-last pending duplicate changed state: %+v", out)
	}
	for _, change := range []func(*model.EdgeAggregate){
		func(a *model.EdgeAggregate) { a.EdgeID = "different" },
		func(a *model.EdgeAggregate) {
			a.WindowStart = a.WindowStart.Add(5 * time.Minute)
			a.WindowEnd = a.WindowEnd.Add(5 * time.Minute)
		},
		func(a *model.EdgeAggregate) {
			a.WindowStart = a.WindowStart.Add(30 * time.Minute)
			a.WindowEnd = a.WindowEnd.Add(30 * time.Minute)
		},
		func(a *model.EdgeAggregate) {
			a.Events++
			a.Temperature.Invalid++
			a.Humidity.Invalid++
			a.Pressure.Invalid++
		},
	} {
		bad := input
		change(&bad)
		if _, err := a.Add(0, bad); err == nil {
			t.Fatal("conflicting pending duplicate accepted")
		}
	}
	out, err := a.EndPartitionInput(0)
	if err != nil || len(out.Aggregates) != 2 || out.Aggregates[0].Events != 1 || out.Aggregates[1].Events != 1 {
		t.Fatalf("duplicates changed counts: %+v %v", out, err)
	}
}

func TestSourceEOSFlushesSortedAndRejectsAllSubsequentData(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			a := newAggregator(t, 3, time.Hour)
			if a.ended || a.hasObserved {
				t.Fatal("new partition inferred completion or progress")
			}
			if !empty {
				for _, minute := range []int{30, 0, 15} {
					add(t, a, fixtureEdge("edge", minute))
				}
			}
			if _, err := a.EndPartitionInput(2); err == nil || a.ended {
				t.Fatal("wrong-partition EOS accepted")
			}
			out, err := a.EndPartitionInput(3)
			if err != nil || !out.End || out.Progress != nil || out.SourcePartition != 3 || len(a.windows) != 0 {
				t.Fatalf("source EOS: %+v %v", out, err)
			}
			want := 3
			if empty {
				want = 0
			}
			if len(out.Aggregates) != want {
				t.Fatalf("flushed %d, want %d", len(out.Aggregates), want)
			}
			for i, aggregate := range out.Aggregates {
				if !aggregate.WindowStart.Equal(epoch.Add(time.Duration(i) * 15 * time.Minute)) {
					t.Fatal("EOS flush not sorted")
				}
			}
			out, err = a.EndPartitionInput(3)
			if err != nil || out.End || out.Progress != nil || len(out.Aggregates) != 0 {
				t.Fatal("duplicate EOS produced output")
			}
			for _, minute := range []int{0, 15, 30, 90} {
				if _, err := a.Add(3, fixtureEdge("edge", minute)); err == nil {
					t.Fatal("aggregate, including duplicate, accepted after EOS")
				}
			}
		})
	}
}

func TestWatermarkFlushesMultipleWindowsInOrder(t *testing.T) {
	a := newAggregator(t, 0, time.Hour)
	for _, minute := range []int{30, 0, 15} {
		add(t, a, fixtureEdge("edge", minute))
	}
	out := add(t, a, fixtureEdge("edge", 120))
	if len(out.Aggregates) != 3 || out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(65*time.Minute)) {
		t.Fatalf("flush: %+v", out)
	}
	for i, aggregate := range out.Aggregates {
		if !aggregate.WindowStart.Equal(epoch.Add(time.Duration(i) * 15 * time.Minute)) {
			t.Fatal("watermark flush not sorted")
		}
	}
}

func TestConstructorValidation(t *testing.T) {
	for _, tc := range []struct {
		partition   int
		size, delay time.Duration
	}{
		{-1, time.Minute, 0}, {6, time.Minute, 0}, {0, 0, 0}, {0, -time.Minute, 0}, {0, time.Minute, -time.Nanosecond},
	} {
		if _, err := NewPartitionAggregator(tc.partition, tc.size, tc.delay); err == nil {
			t.Fatalf("invalid config accepted: %+v", tc)
		}
	}
	for _, delay := range []time.Duration{0, time.Hour} {
		if _, err := NewPartitionAggregator(0, time.Minute, delay); err != nil {
			t.Fatal(err)
		}
	}
	if DefaultWindowSize != 15*time.Minute || DefaultWatermarkDelay != 5*time.Minute {
		t.Fatal("unexpected defaults")
	}
}

func TestObservedWatermarkCrossesZeroWithoutSentinel(t *testing.T) {
	a := newAggregator(t, 0, 25*time.Minute)
	zero := time.Time{}
	for _, minute := range []int{15, 20, 10, 25} {
		input := fixtureEdge("edge", minute)
		input.WindowStart = zero.Add(time.Duration(minute)*time.Minute)
		input.WindowEnd = input.WindowStart.Add(5*time.Minute)
		out := add(t, a, input)
		if minute == 10 {
			if out.Progress != nil || !a.watermark.IsZero() || !a.hasObserved || !a.maxEventTimeObserved.Equal(zero.Add(25*time.Minute)) {
				t.Fatal("out-of-order input reset the observed zero watermark")
			}
			continue
		}
		want := input.WindowEnd.Add(-25*time.Minute)
		if out.Progress == nil || !out.Progress.CompleteThrough.Equal(want) || !a.hasObserved || !a.maxEventTimeObserved.Equal(input.WindowEnd) {
			t.Fatalf("watermark crossing zero: %+v want %v", out, want)
		}
	}
	// EOS ordering uses time.Time rather than UnixNano, whose supported range
	// excludes the zero time and other otherwise representable dates.
	out, err := a.EndPartitionInput(0)
	if err != nil || len(out.Aggregates) != 2 || !out.Aggregates[0].WindowStart.Before(out.Aggregates[1].WindowStart) {
		t.Fatalf("early historical windows: %+v %v", out, err)
	}
}

func TestInputValidationDoesNotAdvanceState(t *testing.T) {
	for name, change := range map[string]func(*model.EdgeAggregate){
		"id":         func(a *model.EdgeAggregate) { a.AggregateID = " " },
		"edge":       func(a *model.EdgeAggregate) { a.EdgeID = " " },
		"start":      func(a *model.EdgeAggregate) { a.WindowStart = time.Time{} },
		"end":        func(a *model.EdgeAggregate) { a.WindowEnd = time.Time{} },
		"reversed":   func(a *model.EdgeAggregate) { a.WindowEnd = a.WindowStart },
		"events":     func(a *model.EdgeAggregate) { a.Events = 0 },
		"counts":     func(a *model.EdgeAggregate) { a.Humidity.Invalid++ },
		"statistics": func(a *model.EdgeAggregate) { a.Pressure.Min = nil },
		"nan":        func(a *model.EdgeAggregate) { a.Temperature.Sum = math.NaN() },
		"divisor":    func(a *model.EdgeAggregate) { a.WindowEnd = a.WindowStart.Add(4 * time.Minute) },
		"alignment": func(a *model.EdgeAggregate) {
			a.WindowStart = a.WindowStart.Add(time.Minute)
			a.WindowEnd = a.WindowEnd.Add(time.Minute)
		},
		"oversize": func(a *model.EdgeAggregate) { a.WindowEnd = a.WindowStart.Add(30 * time.Minute) },
	} {
		t.Run(name, func(t *testing.T) {
			a := newAggregator(t, 0, 0)
			input := fixtureEdge("edge", 0)
			change(&input)
			if _, err := a.Add(0, input); err == nil {
				t.Fatal("malformed input accepted")
			}
			if a.hasObserved || len(a.windows) != 0 {
				t.Fatal("invalid input changed state")
			}
		})
	}
	a := newAggregator(t, 0, 0)
	if _, err := a.Add(1, fixtureEdge("edge", 0)); err == nil || a.hasObserved {
		t.Fatal("wrong actual partition accepted")
	}
}
