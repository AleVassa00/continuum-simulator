package cloudworker

import (
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
func colocated(n int) []string {
	var ids []string
	for i := 0; len(ids) < n; i++ {
		id := fmt.Sprintf("edge-%d", i)
		if PartitionForEdge(id) == 0 {
			ids = append(ids, id)
		}
	}
	return ids
}
func TestPartitionWaitsForEverySource(t *testing.T) {
	ids := colocated(2)
	a, err := NewPartitionAggregator(0, ids, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, minute := range []int{0, 5, 10, 15, 20, 25} {
		out, err := a.Add(0, fixtureEdge(ids[0], minute))
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Aggregates) != 0 || out.Progress != nil || out.End {
			t.Fatal("fast Edge finalized unseen slow Edge")
		}
	}
	if len(a.windows) != 2 {
		t.Fatal("state must be partition+window, not a single current window")
	}
	for _, minute := range []int{0, 5} {
		out, err := a.Add(0, fixtureEdge(ids[1], minute))
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Aggregates) != 0 {
			t.Fatal("slow Edge has not crossed boundary")
		}
	}
	out, err := a.Add(0, fixtureEdge(ids[1], 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Aggregates) != 1 || out.Aggregates[0].Events != 6 || out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(15*time.Minute)) {
		t.Fatalf("wrong finalized window: %+v", out)
	}
	// All later data from fast Edge was preserved.
	out, err = a.EndEdge(0, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Aggregates) != 1 || out.Aggregates[0].Events != 3 || out.End {
		t.Fatalf("unexpected terminal progress: %+v", out)
	}
	out, err = a.EndEdge(0, ids[0])
	if err != nil || !out.End {
		t.Fatalf("EOS: %+v %v", out, err)
	}
}
func TestEmptyEdgeAndEmptyPartition(t *testing.T) {
	ids := colocated(2)
	a, _ := NewPartitionAggregator(0, ids, 15*time.Minute)
	a.Add(0, fixtureEdge(ids[0], 0))
	out, err := a.EndEdge(0, ids[0])
	if err != nil || len(out.Aggregates) != 0 || out.End {
		t.Fatal("one Edge EOS closed partition")
	}
	out, err = a.EndEdge(0, ids[1])
	if err != nil || !out.End || len(out.Aggregates) != 1 {
		t.Fatalf("empty Edge EOS: %+v %v", out, err)
	}
	out, err = a.EndEdge(0, ids[1])
	if err != nil || out.End || len(out.Aggregates) != 0 {
		t.Fatal("duplicate EOS")
	}
	empty, _ := NewPartitionAggregator(1, nil, 15*time.Minute)
	if out := empty.Initialize(); !out.End || len(out.Aggregates) != 0 {
		t.Fatal("empty partition must certify termination")
	}
	if empty.Initialize().End {
		t.Fatal("duplicate initialize")
	}
}
func TestDedupLateConflictsAndActualPartition(t *testing.T) {
	id := colocated(1)[0]
	a, _ := NewPartitionAggregator(0, []string{id}, 15*time.Minute)
	input := fixtureEdge(id, 0)
	if _, err := a.Add(1, input); err == nil {
		t.Fatal("used hash instead of actual partition")
	}
	if _, err := a.Add(0, input); err != nil {
		t.Fatal(err)
	}
	if out, err := a.Add(0, input); err != nil || len(out.Aggregates) != 0 {
		t.Fatal("duplicate changed state")
	}
	bad := input
	bad.Events = 2
	bad.Temperature.Invalid = 1
	bad.Humidity.Invalid = 1
	bad.Pressure.Invalid = 1
	if _, err := a.Add(0, bad); err == nil {
		t.Fatal("conflict accepted")
	}
	a.Add(0, fixtureEdge(id, 10))
	if out, err := a.Add(0, fixtureEdge(id, 10)); err != nil || len(out.Aggregates) != 0 {
		t.Fatal("last closed input retry")
	}
	if _, err := a.Add(0, fixtureEdge(id, 5)); err == nil {
		t.Fatal("late unrecognized record accepted")
	}
	a.EndEdge(0, id)
	if _, err := a.Add(0, fixtureEdge(id, 15)); err == nil {
		t.Fatal("post EOS data accepted")
	}
	if _, err := a.EndEdge(0, "unknown"); err == nil {
		t.Fatal("unknown EOS accepted")
	}
}
func TestGapsAreCertifiedOnlyAfterProgress(t *testing.T) {
	id := colocated(1)[0]
	a, _ := NewPartitionAggregator(0, []string{id}, 15*time.Minute)
	out, err := a.Add(0, fixtureEdge(id, 60))
	if err != nil || len(out.Aggregates) != 0 || out.Progress == nil || !out.Progress.CompleteThrough.Equal(epoch.Add(time.Hour)) {
		t.Fatalf("gap certificate: %+v %v", out, err)
	}
	out, err = a.EndEdge(0, id)
	if err != nil || !out.End || len(out.Aggregates) != 1 {
		t.Fatal("tail EOS did not flush exact Cloud window")
	}
}
