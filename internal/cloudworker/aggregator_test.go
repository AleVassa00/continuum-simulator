package cloudworker

import (
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"fmt"
	"testing"
	"time"
)

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func fixtureEdge(id string, minute int) model.EdgeAggregate {
	start := epoch.Add(time.Duration(minute) * time.Minute)
	v := 10.0
	m := model.MetricAggregate{Valid: 1, Sum: v, Average: &v, Min: &v, Max: &v}
	end := start.Add(5 * time.Minute)
	return model.EdgeAggregate{AggregateID: fmt.Sprintf("%s:%d", id, minute), EdgeID: id, WindowStart: start, WindowEnd: end, CompleteThrough: end, Events: 1, Temperature: m, Humidity: m, Pressure: m, EmittedAt: epoch}
}

func newAggregator(t *testing.T, members ...string) *PartitionAggregator {
	t.Helper()
	a, err := NewPartitionAggregator(0, model.DefaultSourcePartitionCount, members, DefaultWindowSize, DefaultMaxEdgeWatermarkSkew)
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

func TestPartitionWatermarkUsesMinimumEdgeProgress(t *testing.T) {
	a := newAggregator(t, "fast", "slow")
	for minute := 0; minute < 15; minute += 5 {
		if out := add(t, a, fixtureEdge("fast", minute)); out.Progress != nil || len(out.Aggregates) != 0 {
			t.Fatalf("one Edge certified the partition: %+v", out)
		}
	}
	if out := add(t, a, fixtureEdge("slow", 0)); out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(5*time.Minute)) || len(out.Aggregates) != 0 {
		t.Fatalf("wrong minimum watermark: %+v", out)
	}
	if out := add(t, a, fixtureEdge("slow", 5)); out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(10*time.Minute)) || len(out.Aggregates) != 0 {
		t.Fatalf("wrong minimum watermark: %+v", out)
	}
	out := add(t, a, fixtureEdge("slow", 10))
	if out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(15*time.Minute)) || len(out.Aggregates) != 1 || out.Aggregates[0].InputAggregates != 6 {
		t.Fatalf("window not finalized by minimum watermark: %+v", out)
	}
	if len(a.windows) != 0 {
		t.Fatal("finalized window retained")
	}
}

func TestEndedEdgeStopsHoldingBackPartition(t *testing.T) {
	a := newAggregator(t, "fast", "slow")
	fast := fixtureEdge("fast", 0)
	fast.CompleteThrough = epoch.Add(30 * time.Minute)
	add(t, a, fast)
	add(t, a, fixtureEdge("slow", 0))
	out, err := a.EndEdge(0, "slow")
	if err != nil || out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(30*time.Minute)) || len(out.Aggregates) != 1 || out.End {
		t.Fatalf("completed Edge still held frontier: %+v %v", out, err)
	}
	out, err = a.EndEdge(0, "fast")
	if err != nil || !out.End || out.Progress != nil || len(out.Aggregates) != 0 || !a.ended {
		t.Fatalf("partition did not terminate: %+v %v", out, err)
	}
	if duplicate, err := a.EndEdge(0, "fast"); err != nil || duplicate.End {
		t.Fatalf("duplicate Edge end changed output: %+v %v", duplicate, err)
	}
}

func TestAllEdgeEndsFlushPendingWindowsInOrder(t *testing.T) {
	a := newAggregator(t, "edge", "blocker")
	for _, minute := range []int{0, 15, 30} {
		add(t, a, fixtureEdge("edge", minute))
	}
	out, err := a.EndEdge(0, "edge")
	if err != nil || out.End || len(out.Aggregates) != 0 {
		t.Fatalf("one ended Edge completed the partition: %+v %v", out, err)
	}
	out, err = a.EndEdge(0, "blocker")
	if err != nil || !out.End || len(out.Aggregates) != 3 {
		t.Fatalf("terminal flush: %+v %v", out, err)
	}
	for i, aggregate := range out.Aggregates {
		if !aggregate.WindowStart.Equal(epoch.Add(time.Duration(i) * 15 * time.Minute)) {
			t.Fatal("terminal flush is not ordered")
		}
	}
}

func TestEmptyPartitionEndsAtInitialization(t *testing.T) {
	a := newAggregator(t)
	out := a.Initialize()
	if !out.End || !a.ended || len(out.Aggregates) != 0 {
		t.Fatalf("empty partition not completed: %+v", out)
	}
}

func TestEmbeddedProgressAndMembershipProtocolViolations(t *testing.T) {
	a := newAggregator(t, "edge")
	if _, err := a.Add(0, fixtureEdge("other", 0)); err == nil {
		t.Fatal("unexpected Edge accepted")
	}
	add(t, a, fixtureEdge("edge", 0))
	invalidProgress := fixtureEdge("edge", 5)
	invalidProgress.CompleteThrough = invalidProgress.WindowEnd.Add(-time.Minute)
	if _, err := a.Add(0, invalidProgress); err == nil {
		t.Fatal("progress before aggregate window accepted")
	}
	if _, err := a.EndEdge(1, "edge"); err == nil {
		t.Fatal("wrong partition end accepted")
	}
	if _, err := a.EndEdge(0, "unknown"); err == nil {
		t.Fatal("unknown Edge end accepted")
	}
}

func TestMaximumSkewAdvancesWatermarkAndDropsLateAggregate(t *testing.T) {
	a, err := NewPartitionAggregator(0, model.DefaultSourcePartitionCount, []string{"fast", "slow"}, DefaultWindowSize, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fast := fixtureEdge("fast", 0)
	fast.CompleteThrough = epoch.Add(30 * time.Minute)
	add(t, a, fast)
	out := add(t, a, fixtureEdge("slow", 0))
	if out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(20*time.Minute)) || len(out.Aggregates) != 1 {
		t.Fatalf("bounded watermark did not advance: %+v", out)
	}
	late := add(t, a, fixtureEdge("slow", 5))
	if late.Late == nil || late.Late.Aggregate.AggregateID != "slow:5" || len(a.windows) != 0 {
		t.Fatalf("late aggregate reopened a finalized window: %+v", late)
	}
}

func TestPendingDuplicateIsIdempotent(t *testing.T) {
	a := newAggregator(t, "edge")
	input := fixtureEdge("edge", 0)
	add(t, a, input)
	retry := input
	retry.EmittedAt = epoch.Add(time.Hour)
	if out := add(t, a, retry); out.Progress != nil || len(out.Aggregates) != 0 {
		t.Fatalf("duplicate changed output: %+v", out)
	}
	conflict := input
	conflict.Events = 2
	conflict.Temperature.Valid = 2
	conflict.Humidity.Valid = 2
	conflict.Pressure.Valid = 2
	if _, err := a.Add(0, conflict); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
}

func TestMembershipUsesProducerPartitioner(t *testing.T) {
	ids := []string{"edge-0", "edge-1", "edge-2"}
	membership, err := BuildMembership(ids, model.DefaultSourcePartitionCount)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		partition := kafkautil.PartitionForEdge(id, model.DefaultSourcePartitionCount)
		found := false
		for _, member := range membership[partition] {
			found = found || member == id
		}
		if !found {
			t.Fatalf("%s missing from partition %d", id, partition)
		}
	}
	if _, err := BuildMembership([]string{"edge-0", "edge-0"}, 6); err == nil {
		t.Fatal("duplicate membership accepted")
	}
	if _, err := NewPartitionAggregator(6, 6, nil, time.Minute, time.Minute); err == nil {
		t.Fatal("invalid owner accepted")
	}
}
