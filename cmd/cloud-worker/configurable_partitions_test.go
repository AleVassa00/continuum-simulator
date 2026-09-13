package main

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"continuum/internal/cloudworker"
	"continuum/internal/globalaggregator"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"continuum/internal/partitioncompletion"
	"github.com/segmentio/kafka-go"
)

func TestConfiguredPartitionPipeline(t *testing.T) {
	for _, count := range []int{1, 3, 6, 8} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var outputs []model.GlobalAggregate
			g, err := globalaggregator.New(count, func(_ context.Context, a model.GlobalAggregate) error {
				outputs = append(outputs, a)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			processors := make([]*CloudMessageProcessor, count)
			for p := range processors {
				a, err := cloudworker.NewPartitionAggregator(p, count, cloudworker.DefaultWindowSize, cloudworker.DefaultWatermarkDelay)
				if err != nil {
					t.Fatal(err)
				}
				processors[p] = &CloudMessageProcessor{aggregator: a, publishMessage: func(ctx context.Context, m kafka.Message) error { return feedGlobal(ctx, g, m) }}
			}
			ids := make([]string, 13) // Producer count is independent of the source partition count.
			for e := range ids {
				ids[e] = fmt.Sprintf("edge-%d", e)
			}
			markers := make(chan int, count)
			c, err := partitioncompletion.New(ctx, count, ids, func(_ context.Context, p int) error { markers <- p; return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { cancel(); c.Wait() }()
			if err := c.Start(); err != nil {
				t.Fatal(err)
			}
			for minute := 0; minute < 35; minute += 5 {
				for _, id := range ids {
					m := edgeInput(t, id, minute, 1)
					m.Partition = kafkautil.PartitionForEdge(id, count)
					if err := processors[m.Partition].Process(ctx, m); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, id := range ids {
				if err := c.Complete(id); err != nil {
					t.Fatal(err)
				}
			}
			c.Wait()
			seen := make(map[int]bool)
			for range count {
				p := <-markers
				if seen[p] {
					t.Fatal("duplicate partition EOS")
				}
				seen[p] = true
				if err := processors[p].Process(ctx, sourceEOS(p)); err != nil {
					t.Fatal(err)
				}
			}
			if !g.IsComplete() || len(outputs) != 3 {
				t.Fatalf("incomplete output: %+v", outputs)
			}
			var events uint64
			for _, a := range outputs {
				events += a.Events
				if a.ExpectedPartitions != uint64(count) || a.ContributingPartitions != uint64(count) {
					t.Fatalf("wrong partition counts: %+v", a)
				}
			}
			if events != 7*13 {
				t.Fatalf("event conservation: %d", events)
			}
			if _, err := g.EndPartition(ctx, count); err == nil {
				t.Fatal("out-of-range EOS accepted")
			}
			if err := g.Progress(ctx, model.PartitionProgress{SourcePartition: count, CompleteThrough: testEpoch}); err == nil {
				t.Fatal("out-of-range progress accepted")
			}
			if _, err := cloudworker.NewPartitionAggregator(count, count, cloudworker.DefaultWindowSize, 0); err == nil {
				t.Fatal("out-of-range owner accepted")
			}
		})
	}
}

func TestWorkerPartitionCountConfiguration(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "unused:9092")
	for _, tc := range []struct {
		value string
		want  int
	}{{"", model.DefaultSourcePartitionCount}, {"1", 1}, {"8", 8}} {
		t.Setenv("SOURCE_PARTITION_COUNT", tc.value)
		cfg, err := loadCloudWorkerConfig()
		if err != nil || cfg.SourcePartitionCount != tc.want {
			t.Fatalf("value=%q config=%+v err=%v", tc.value, cfg, err)
		}
	}
}
