package globalaggregator

import (
	"context"
	"continuum/internal/model"
	"errors"
	"testing"
	"time"
)

var testStart = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func partial(p int, start time.Time) model.CloudPartitionAggregate {
	end := start.Add(15 * time.Minute)
	v := float64(p + 1)
	n := uint64(p + 1)
	m := model.MetricAggregate{Valid: n, Sum: v * float64(n), Average: &v, Min: &v, Max: &v}
	return model.CloudPartitionAggregate{SourcePartition: p, AggregateID: model.PartitionAggregateID(p, start, end), WindowStart: start, WindowEnd: end, CompleteThrough: end, InputAggregates: 1, Events: n, Temperature: m, Humidity: m, Pressure: m, EmittedAt: testStart}
}
func TestOnlyCertifiedPartitionCompleteness(t *testing.T) {
	ctx := context.Background()
	var outputs []model.GlobalAggregate
	a, _ := New(model.DefaultSourcePartitionCount, func(_ context.Context, out model.GlobalAggregate) error { outputs = append(outputs, out); return nil })
	for p := 0; p < 5; p++ {
		if err := a.Add(ctx, partial(p, testStart)); err != nil {
			t.Fatal(err)
		}
	}
	if len(outputs) != 0 {
		t.Fatal("five partials cannot complete six partitions")
	}
	// Partition 5 sends a partial for a later window, advancing its CompleteThrough
	p5Next := partial(5, testStart.Add(15*time.Minute))
	if err := a.Add(ctx, p5Next); err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 1 || outputs[0].Events != 15 || outputs[0].ContributingPartitions != 6 {
		t.Fatalf("certified zero: %+v", outputs)
	}
	if err := a.Add(ctx, partial(5, testStart)); err == nil {
		t.Fatal("data after progress accepted")
	}
}

func TestMaximumPartitionSkewAdvancesGlobalWatermarkAndDropsLatePartial(t *testing.T) {
	ctx := context.Background()
	var outputs []model.GlobalAggregate
	a, err := NewWithMaxPartitionWatermarkSkew(2, 15*time.Minute, func(_ context.Context, out model.GlobalAggregate) error {
		outputs = append(outputs, out)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Add(ctx, partial(0, testStart)); err != nil {
		t.Fatal(err)
	}
	second := partial(0, testStart.Add(15*time.Minute))
	second.CompleteThrough = testStart.Add(45 * time.Minute)
	if err := a.Add(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := a.Add(ctx, partial(1, testStart)); err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 2 || !a.completeThrough.Equal(testStart.Add(30*time.Minute)) {
		t.Fatalf("bounded global watermark did not advance: outputs=%d watermark=%s", len(outputs), a.completeThrough)
	}
	late, err := a.AddWithResult(ctx, partial(1, testStart.Add(15*time.Minute)))
	if err != nil || late == nil || late.Aggregate.SourcePartition != 1 {
		t.Fatalf("late partial not dropped explicitly: %+v %v", late, err)
	}
}
func TestNoWindowPolicyAndNoTimeout(t *testing.T) {
	ctx := context.Background()
	var output model.GlobalAggregate
	a, _ := New(model.DefaultSourcePartitionCount, func(_ context.Context, out model.GlobalAggregate) error { output = out; return nil })
	start := testStart.Add(2 * time.Minute)
	for p := 0; p < 6; p++ {
		v := partial(p, start)
		// Global trusts exact window boundaries finalized by Cloud; no 15m rewindowing.
		v.WindowEnd = start.Add(7 * time.Minute)
		v.AggregateID = model.PartitionAggregateID(p, v.WindowStart, v.WindowEnd)
		if err := a.Add(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	if !output.WindowStart.Equal(start) || !output.WindowEnd.Equal(start.Add(7*time.Minute)) {
		t.Fatal("Global changed Cloud window")
	}
}
func TestDuplicateConflictAndSinkFailureRetry(t *testing.T) {
	ctx := context.Background()
	fail := true
	emissions := 0
	a, _ := New(model.DefaultSourcePartitionCount, func(context.Context, model.GlobalAggregate) error {
		if fail {
			return errors.New("sink unavailable")
		}
		emissions++
		return nil
	})
	for p := 0; p < 5; p++ {
		if err := a.Add(ctx, partial(p, testStart)); err != nil {
			t.Fatal(err)
		}
		if err := a.Add(ctx, partial(p, testStart)); err != nil {
			t.Fatal(err)
		}
	}
	conflict := partial(0, testStart)
	conflict.InputAggregates++
	if err := a.Add(ctx, conflict); err == nil {
		t.Fatal("same ID changed payload")
	}
	if err := a.Add(ctx, partial(5, testStart)); err == nil {
		t.Fatal("sink failure not propagated")
	}
	fail = false
	if err := a.Add(ctx, partial(5, testStart)); err != nil {
		t.Fatal(err)
	}
	if err := a.Add(ctx, partial(5, testStart)); err != nil {
		t.Fatal(err)
	}
	if emissions != 1 {
		t.Fatalf("retry emitted %d times", emissions)
	}
}
func TestPartitionEOSHandlesEmptyAndEarlyTerminatingPartitions(t *testing.T) {
	ctx := context.Background()
	emissions := 0
	a, _ := New(model.DefaultSourcePartitionCount, func(context.Context, model.GlobalAggregate) error { emissions++; return nil })
	for p := 0; p < 5; p++ {
		done, err := a.EndPartition(ctx, p)
		if done || err != nil {
			t.Fatal("premature completion")
		}
		done, err = a.EndPartition(ctx, p)
		if done || err != nil {
			t.Fatal("duplicate EOS changed completion")
		}
	}
	for _, minute := range []int{0, 30} {
		if err := a.Add(ctx, partial(5, testStart.Add(time.Duration(minute)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if emissions != 2 {
		t.Fatal("EOS did not certify zero contributions for later windows")
	}
	done, err := a.EndPartition(ctx, 5)
	if err != nil || !done {
		t.Fatal("not complete")
	}
	if err := a.Add(ctx, partial(5, testStart.Add(time.Hour))); err == nil {
		t.Fatal("new data after EOS")
	}
	empty, _ := New(model.DefaultSourcePartitionCount, func(context.Context, model.GlobalAggregate) error { t.Fatal("empty run emitted data"); return nil })
	for p := 0; p < 6; p++ {
		_, err := empty.EndPartition(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !empty.IsComplete() {
		t.Fatal("empty run cannot complete")
	}
}
func TestDifferentWindowBoundsNeverMerged(t *testing.T) {
	a, _ := New(model.DefaultSourcePartitionCount, func(context.Context, model.GlobalAggregate) error { return nil })
	a.Add(context.Background(), partial(0, testStart))
	if err := a.Add(context.Background(), partial(1, testStart.Add(time.Minute))); err == nil {
		t.Fatal("merged overlapping nonidentical windows")
	}
}
