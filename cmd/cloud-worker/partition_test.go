package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/globalaggregator"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

var testEpoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func edgeInput(t *testing.T, id string, minute int, n uint64) kafka.Message {
	t.Helper()
	start := testEpoch.Add(time.Duration(minute) * time.Minute)
	v := float64(n)
	m := model.MetricAggregate{Valid: n, Sum: v * float64(n), Average: &v, Min: &v, Max: &v}
	a := model.EdgeAggregate{AggregateID: fmt.Sprintf("%s:%d", id, minute), EdgeID: id, WindowStart: start, WindowEnd: start.Add(5 * time.Minute), Events: n, Temperature: m, Humidity: m, Pressure: m, EmittedAt: testEpoch}
	payload, err := avrocodec.EncodeEdgeAggregate(a)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Partition: cloudworker.PartitionForEdge(id), Key: []byte(id), Value: payload, Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypeEdgeAggregate)}}}
}
func edgeEOS(id string) kafka.Message {
	return kafka.Message{Partition: cloudworker.PartitionForEdge(id), Key: []byte(id), Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypeEndOfReplay)}}}
}
func feedGlobal(ctx context.Context, g *globalaggregator.Aggregator, m kafka.Message) error {
	kind, err := kafkautil.ParseRecordType(m.Headers)
	if err != nil {
		return err
	}
	p, err := model.ParsePartitionKey(m.Key)
	if err != nil {
		return err
	}
	switch kind {
	case model.RecordTypeCloudPartitionAggregate:
		a, err := avrocodec.DecodeCloudPartitionAggregate(m.Value)
		if err != nil {
			return err
		}
		if p != a.SourcePartition {
			return fmt.Errorf("wrong source key")
		}
		return g.Add(ctx, a)
	case model.RecordTypePartitionProgress:
		progress, err := avrocodec.DecodePartitionProgress(m.Value)
		if err != nil {
			return err
		}
		if p != progress.SourcePartition {
			return fmt.Errorf("wrong progress key")
		}
		return g.Progress(ctx, progress)
	case model.RecordTypePartitionEndOfReplay:
		_, err := g.EndPartition(ctx, p)
		return err
	}
	return fmt.Errorf("unknown control")
}

// Uses the actual kafka-go RangeGroupBalancer and Hash balancer, the actual
// message processor and Avro codecs, and the Global reducer. No worker bypass.
func TestW1W2W4W6HaveIdenticalPartitionSemantics(t *testing.T) {
	var baselinePartials []model.CloudPartitionAggregate
	var baselineGlobals []model.GlobalAggregate
	for _, workers := range []int{1, 2, 4, 6} {
		t.Run(fmt.Sprintf("W%d", workers), func(t *testing.T) {
			ctx := context.Background()
			var ids []string
			for i := 0; i < 13; i++ {
				ids = append(ids, fmt.Sprintf("edge-%d", i))
			}
			membership, err := cloudworker.BuildMembership(ids)
			if err != nil {
				t.Fatal(err)
			}
			var members []kafka.GroupMember
			var partitions []kafka.Partition
			for w := 0; w < workers; w++ {
				members = append(members, kafka.GroupMember{ID: fmt.Sprintf("executor-%d", w), Topics: []string{"edge-aggregates"}})
			}
			for p := 0; p < 6; p++ {
				partitions = append(partitions, kafka.Partition{Topic: "edge-aggregates", ID: p})
			}
			assignments := (kafka.RangeGroupBalancer{}).AssignGroups(members, partitions)
			var wire []kafka.Message
			processors := map[int]*CloudMessageProcessor{}
			workerPartitions := make([][]int, workers)
			for w, m := range members {
				workerPartitions[w] = assignments[m.ID]["edge-aggregates"]
				for _, p := range workerPartitions[w] {
					a, err := cloudworker.NewPartitionAggregator(p, membership[p], 15*time.Minute)
					if err != nil {
						t.Fatal(err)
					}
					processors[p] = &CloudMessageProcessor{aggregator: a, workerID: m.ID, outputTopic: "cloud-partition-aggregates", publishMessage: func(_ context.Context, msg kafka.Message) error { wire = append(wire, msg); return nil }}
					if err := processors[p].Initialize(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			if len(processors) != 6 {
				t.Fatal("missing partition ownership")
			}
			streams := make(map[int][]kafka.Message)
			for i, id := range ids {
				p := cloudworker.PartitionForEdge(id)
				for minute := 0; minute < 35; minute += 5 {
					m := edgeInput(t, id, minute, uint64(i+1))
					streams[p] = append(streams[p], m, m)
				}
				streams[p] = append(streams[p], edgeEOS(id), edgeEOS(id))
			}
			// Different inter-partition scheduling per worker count, same partition logs.
			for pending := true; pending; {
				pending = false
				for w := workers - 1; w >= 0; w-- {
					for _, p := range workerPartitions[w] {
						if len(streams[p]) == 0 {
							continue
						}
						pending = true
						m := streams[p][0]
						streams[p] = streams[p][1:]
						if err := processors[p].Process(ctx, m); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			var partials []model.CloudPartitionAggregate
			byPartition := make(map[int][]kafka.Message)
			for _, m := range wire {
				p, _ := model.ParsePartitionKey(m.Key)
				byPartition[p] = append(byPartition[p], m)
				kind, _ := kafkautil.ParseRecordType(m.Headers)
				if kind == model.RecordTypeCloudPartitionAggregate {
					a, err := avrocodec.DecodeCloudPartitionAggregate(m.Value)
					if err != nil {
						t.Fatal(err)
					}
					a.EmittedAt = time.Time{}
					partials = append(partials, a)
				}
			}
			sort.Slice(partials, func(i, j int) bool { return partials[i].AggregateID < partials[j].AggregateID })
			occupied := 0
			for _, edges := range membership {
				if len(edges) > 0 {
					occupied++
				}
			}
			if occupied != 6 {
				t.Fatalf("current 13-Edge topology does not cover all six partitions: %v", membership)
			}
			if len(partials) != 18 {
				t.Fatalf("want 6 partials x 3 windows, got %d", len(partials))
			}
			var globals []model.GlobalAggregate
			g, _ := globalaggregator.New(func(_ context.Context, a model.GlobalAggregate) error {
				a.EmittedAt = time.Time{}
				globals = append(globals, a)
				return nil
			})
			rng := rand.New(rand.NewSource(int64(workers)))
			for len(byPartition) > 0 {
				keys := make([]int, 0, len(byPartition))
				for p := range byPartition {
					keys = append(keys, p)
				}
				sort.Ints(keys)
				p := keys[rng.Intn(len(keys))]
				msg := byPartition[p][0]
				byPartition[p] = byPartition[p][1:]
				if len(byPartition[p]) == 0 {
					delete(byPartition, p)
				}
				if err := feedGlobal(ctx, g, msg); err != nil {
					t.Fatal(err)
				}
			}
			if !g.IsComplete() {
				t.Fatal("all six partition EOS did not complete Global")
			}
			sort.Slice(globals, func(i, j int) bool { return globals[i].WindowStart.Before(globals[j].WindowStart) })
			if len(globals) != 3 {
				t.Fatalf("global windows: %d", len(globals))
			}
			var events uint64
			for _, a := range globals {
				events += a.Events
				if a.ExpectedPartitions != 6 || a.ContributingPartitions != 6 {
					t.Fatal("incomplete reduction")
				}
				if *a.Temperature.Average != 9 {
					t.Fatalf("mean of means or invalid weighted sum/count: %v", *a.Temperature.Average)
				}
			}
			if events != 7*91 {
				t.Fatalf("dedup lost/doubled events: %d", events)
			}
			if workers == 1 {
				baselinePartials = partials
				baselineGlobals = globals
			} else if !reflect.DeepEqual(partials, baselinePartials) || !reflect.DeepEqual(globals, baselineGlobals) {
				t.Fatal("worker count changed logical output")
			}
		})
	}
}
func TestPublishBeforeCommitAndNoCommitOnFailure(t *testing.T) {
	id := "edge-0"
	partition := cloudworker.PartitionForEdge(id)
	for _, failAt := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			a, _ := cloudworker.NewPartitionAggregator(partition, []string{id}, 15*time.Minute)
			var order []string
			calls := 0
			committed := false
			armed := false
			p := &CloudMessageProcessor{aggregator: a, publishMessage: func(_ context.Context, m kafka.Message) error {
				calls++
				kind, _ := kafkautil.ParseRecordType(m.Headers)
				order = append(order, kind)
				if armed && calls == failAt {
					return errors.New("broker write failed")
				}
				return nil
			}}
			if err := p.Process(context.Background(), edgeInput(t, id, 0, 1)); err != nil {
				t.Fatal(err)
			}
			// Initial progress precedes any data for this incomplete window. Reset log.
			calls = 0
			order = nil
			armed = true
			err := processAndCommitMessage(context.Background(), edgeEOS(id), p, func(kafka.Message) error { committed = true; order = append(order, "commit"); return nil })
			if failAt == 0 {
				want := []string{model.RecordTypeCloudPartitionAggregate, model.RecordTypePartitionEndOfReplay, "commit"}
				if err != nil || !reflect.DeepEqual(order, want) {
					t.Fatalf("order=%v err=%v", order, err)
				}
			} else if err == nil || committed {
				t.Fatal("commit after failed publication")
			}
		})
	}
}
func TestWorkerConfigRequiresRealMembershipAndSixPartitions(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "unused:9092")
	t.Setenv("CLOUD_EXPECTED_EDGE_IDS", "edge-0,edge-1")
	t.Setenv("SOURCE_PARTITION_COUNT", "6")
	t.Setenv("CLOUD_WINDOW_SIZE", "15m")
	cfg, err := loadCloudWorkerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Membership) != 6 {
		t.Fatal("membership missing for valid config")
	}
	for _, value := range []string{"5", "7", "invalid"} {
		t.Setenv("SOURCE_PARTITION_COUNT", value)
		if _, err := loadCloudWorkerConfig(); err == nil {
			t.Fatal("partition count changed")
		}
	}
	t.Setenv("SOURCE_PARTITION_COUNT", "6")
	t.Setenv("CLOUD_EXPECTED_EDGE_IDS", "")
	if _, err := loadCloudWorkerConfig(); err == nil {
		t.Fatal("empty membership accepted")
	}
}
