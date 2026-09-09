package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/globalaggregator"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

// Opt-in: KAFKA_INTEGRATION_BROKER must point to a disposable, single-broker
// Kafka. Unique test topics/groups are created and left there for inspection.
// Exercises the real worker consumer group and Kafka on BOTH pipeline hops;
// the real Global reducer is connected to an in-memory assertion sink.
func TestKafkaPartitionPipeline(t *testing.T) {
	broker := os.Getenv("KAFKA_INTEGRATION_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_INTEGRATION_BROKER to a disposable Kafka broker")
	}
	for _, edgeCount := range []int{13, 1} {
		var baseline []model.GlobalAggregate
		for _, workers := range []int{1, 2, 4, 6} {
			t.Run(fmt.Sprintf("edges%d/W%d", edgeCount, workers), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				prefix := fmt.Sprintf("partition-test-%d", time.Now().UnixNano())
				input, output, group := prefix+"-input", prefix+"-output", prefix+"-workers"
				conn, err := kafka.DialContext(ctx, "tcp", broker)
				if err != nil {
					t.Fatal(err)
				}
				err = conn.CreateTopics(kafka.TopicConfig{Topic: input, NumPartitions: 6, ReplicationFactor: 1}, kafka.TopicConfig{Topic: output, NumPartitions: 1, ReplicationFactor: 1})
				conn.Close()
				if err != nil {
					t.Fatal(err)
				}
				pollKafka(t, ctx, func() bool {
					return validateKafkaTopology(ctx, broker, input, 6) == nil && validateKafkaTopology(ctx, broker, output, 1) == nil
				})
				// Each deployment is a new process in production. Do not reuse the
				// default process-wide metadata cache across newly created test topics.
				transport := &kafka.Transport{}
				defer transport.CloseIdleConnections()
				client := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 5 * time.Second, Transport: transport}
				var ids []string
				for e := 0; e < edgeCount; e++ {
					ids = append(ids, fmt.Sprintf("edge-%d", e))
				}
				membership, err := cloudworker.BuildMembership(ids)
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, workers)
				for w := 0; w < workers; w++ {
					cfg := CloudWorkerConfig{KafkaBroker: broker, InputTopic: input, OutputTopic: output, GroupID: group, WorkerID: fmt.Sprintf("executor-%d", w), WindowSize: 15 * time.Minute, Membership: membership}
					go func() {
						writer := newKafkaWriter(broker, output)
						writer.Transport = transport
						defer writer.Close()
						err := consume(ctx, cfg, func(ctx context.Context, m kafka.Message) error { return writer.WriteMessages(ctx, m) })
						done <- err
						if err != nil {
							cancel()
						}
					}()
				}
				defer func() {
					cancel()
					for w := 0; w < workers; w++ {
						select {
						case err := <-done:
							if err != nil {
								t.Errorf("worker: %v", err)
							}
						case <-time.After(15 * time.Second):
							t.Error("worker did not stop")
							return
						}
					}
				}()
				// Readiness comes from broker membership + assignment, NOT elapsed
				// silence. No input is offered before the requested group is stable.
				pollKafka(t, ctx, func() bool {
					r, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{group}})
					if err != nil || len(r.Groups) != 1 {
						return false
					}
					g := r.Groups[0]
					if g.Error != nil || g.GroupState != "Stable" || len(g.Members) != workers {
						return false
					}
					seen := map[int]bool{}
					for _, m := range g.Members {
						for _, a := range m.MemberAssignments.Topics {
							if a.Topic == input {
								for _, p := range a.Partitions {
									if seen[p] {
										t.Fatal("duplicate assignment")
									}
									seen[p] = true
								}
							}
						}
					}
					return len(seen) == 6
				})
				producer := newKafkaWriter(broker, input) // Same Hash/key and synchronous ordering as Edge.
				producer.Transport = transport
				defer producer.Close()
				wantOffsets := make([]int64, 6)
				for e, id := range ids {
					for minute := 0; minute < 35; minute += 5 {
						m := edgeInput(t, id, minute, uint64(e+1))
						if err := producer.WriteMessages(ctx, m); err != nil {
							t.Fatal(err)
						}
						wantOffsets[m.Partition]++
					}
					m := edgeEOS(id)
					if err := producer.WriteMessages(ctx, m); err != nil {
						t.Fatal(err)
					}
					wantOffsets[m.Partition]++
				}
				var globals []model.GlobalAggregate
				g, err := globalaggregator.New(func(_ context.Context, a model.GlobalAggregate) error {
					a.EmittedAt = time.Time{}
					globals = append(globals, a)
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: output, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 100 * time.Millisecond})
				defer reader.Close()
				partials := map[string]bool{}
				for !g.IsComplete() {
					m, err := reader.FetchMessage(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if string(m.Headers[0].Value) == model.RecordTypeCloudPartitionAggregate {
						a, err := avrocodec.DecodeCloudPartitionAggregate(m.Value)
						if err != nil {
							t.Fatal(err)
						}
						if partials[a.AggregateID] {
							t.Fatal("duplicate final partial")
						}
						partials[a.AggregateID] = true
					}
					if err := feedGlobal(ctx, g, m); err != nil {
						t.Fatal(err)
					}
				}
				activePartitions := 0
				for _, members := range membership {
					if len(members) > 0 {
						activePartitions++
					}
				}
				if len(partials) != 3*activePartitions || len(globals) != 3 {
					t.Fatalf("partials=%d globals=%d", len(partials), len(globals))
				}
				var events uint64
				for _, a := range globals {
					events += a.Events
				}
				if events != uint64(7*edgeCount*(edgeCount+1)/2) {
					t.Fatalf("lost/duplicated events: %d", events)
				}
				sort.Slice(globals, func(i, j int) bool { return globals[i].WindowStart.Before(globals[j].WindowStart) })
				if baseline == nil {
					baseline = globals
				} else if !reflect.DeepEqual(baseline, globals) {
					t.Fatal("Global results depend on worker count")
				}
				// EOS publication precedes commit: wait for the broker to confirm
				// the exact NEXT offset of every populated input partition.
				pollKafka(t, ctx, func() bool {
					r, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{input: {0, 1, 2, 3, 4, 5}}})
					if err != nil || r.Error != nil || len(r.Topics[input]) != 6 {
						return false
					}
					for _, p := range r.Topics[input] {
						if p.Error != nil {
							return false
						}
						want := wantOffsets[p.Partition]
						if want > 0 && p.CommittedOffset != want {
							return false
						}
					}
					return true
				})
			})
		}
	}
}

func pollKafka(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatal("Kafka condition not reached:", ctx.Err())
		case <-ticker.C:
		}
	}
}
