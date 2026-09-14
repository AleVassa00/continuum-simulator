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
	return model.EdgeAggregate{AggregateID: fmt.Sprintf("%s:%d", id, minute), EdgeID: id, WindowStart: start, WindowEnd: start.Add(5 * time.Minute), Events: 1, Temperature: m, Humidity: m, Pressure: m, EmittedAt: epoch}
}

func newAggregator(t *testing.T, members ...string) *PartitionAggregator {
	t.Helper()
	a, err := NewPartitionAggregator(0, model.DefaultSourcePartitionCount, members, DefaultWindowSize)
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

func advance(t *testing.T, a *PartitionAggregator, edgeID string, minute int) Output {
	t.Helper()
	out, err := a.AdvanceEdgeWatermark(a.partition, model.EdgeWatermark{EdgeID: edgeID, CompleteThrough: epoch.Add(time.Duration(minute) * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPartitionWatermarkUsesMinimumEdgeProgress(t *testing.T) {
	a := newAggregator(t, "fast", "slow")
	for _, id := range []string{"fast", "slow"} {
		for minute := 0; minute < 15; minute += 5 {
			add(t, a, fixtureEdge(id, minute))
		}
	}
	if out := advance(t, a, "fast", 30); out.Progress != nil || len(out.Aggregates) != 0 {
		t.Fatalf("one Edge certified the partition: %+v", out)
	}
	if out := advance(t, a, "slow", 10); out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(10*time.Minute)) || len(out.Aggregates) != 0 {
		t.Fatalf("wrong minimum watermark: %+v", out)
	}
	out := advance(t, a, "slow", 15)
	if out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(15*time.Minute)) || len(out.Aggregates) != 1 || out.Aggregates[0].InputAggregates != 6 {
		t.Fatalf("window not finalized by minimum watermark: %+v", out)
	}
	if len(a.windows) != 0 {
		t.Fatal("finalized window retained")
	}
}

func TestEndedEdgeStopsHoldingBackPartition(t *testing.T) {
	a := newAggregator(t, "fast", "slow")
	add(t, a, fixtureEdge("fast", 0))
	add(t, a, fixtureEdge("slow", 0))
	advance(t, a, "fast", 30)
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
	a := newAggregator(t, "edge")
	for _, minute := range []int{0, 15, 30} {
		add(t, a, fixtureEdge("edge", minute))
	}
	out, err := a.EndEdge(0, "edge")
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

func TestWatermarkAndMembershipProtocolViolations(t *testing.T) {
	a := newAggregator(t, "edge")
	if _, err := a.Add(0, fixtureEdge("other", 0)); err == nil {
		t.Fatal("unexpected Edge accepted")
	}
	add(t, a, fixtureEdge("edge", 0))
	advance(t, a, "edge", 15)
	if _, err := a.AdvanceEdgeWatermark(0, model.EdgeWatermark{EdgeID: "edge", CompleteThrough: epoch.Add(10 * time.Minute)}); err == nil {
		t.Fatal("regressing watermark accepted")
	}
	if _, err := a.Add(0, fixtureEdge("edge", 5)); err == nil {
		t.Fatal("aggregate behind certified watermark accepted")
	}
	if _, err := a.EndEdge(1, "edge"); err == nil {
		t.Fatal("wrong partition end accepted")
	}
	if _, err := a.EndEdge(0, "unknown"); err == nil {
		t.Fatal("unknown Edge end accepted")
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
	if _, err := NewPartitionAggregator(6, 6, nil, time.Minute); err == nil {
		t.Fatal("invalid owner accepted")
	}
}
