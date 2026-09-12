package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

func processorFixture(t *testing.T, partition int, delay time.Duration) (*CloudMessageProcessor, *[]kafka.Message) {
	t.Helper()
	a, err := cloudworker.NewPartitionAggregator(partition, cloudworker.DefaultWindowSize, delay)
	if err != nil {
		t.Fatal(err)
	}
	var wire []kafka.Message
	p := &CloudMessageProcessor{aggregator: a, workerID: "test-worker", publishMessage: func(_ context.Context, m kafka.Message) error { wire = append(wire, m); return nil }}
	return p, &wire
}

func TestSourceEOSValidationAndEmptyCompletion(t *testing.T) {
	for name, change := range map[string]func(*kafka.Message){
		"payload":          func(m *kafka.Message) { m.Value = []byte("unexpected") },
		"edge-key":         func(m *kafka.Message) { m.Key = []byte("edge-0") },
		"empty-key":        func(m *kafka.Message) { m.Key = nil },
		"different-key":    func(m *kafka.Message) { m.Key = []byte("1") },
		"noncanonical-key": func(m *kafka.Message) { m.Key = []byte("00") },
		"out-of-range":     func(m *kafka.Message) { m.Key = []byte("6"); m.Partition = 6 },
		"wrong-owner":      func(m *kafka.Message) { m.Key = []byte("1"); m.Partition = 1 },
		"old-edge-eos":     func(m *kafka.Message) { m.Headers[0].Value = []byte("end_of_replay") },
		"output-eos":       func(m *kafka.Message) { m.Headers[0].Value = []byte(model.RecordTypePartitionEndOfReplay) },
		"missing-type":     func(m *kafka.Message) { m.Headers = nil },
	} {
		t.Run(name, func(t *testing.T) {
			p, wire := processorFixture(t, 0, 0)
			m := sourceEOS(0)
			change(&m)
			committed := false
			if err := processAndCommitMessage(context.Background(), m, p, func(kafka.Message) error { committed = true; return nil }); err == nil || committed || len(*wire) != 0 {
				t.Fatal("invalid EOS changed output or committed")
			}
			if err := p.Process(context.Background(), sourceEOS(0)); err != nil {
				t.Fatal(err)
			}
			if len(*wire) != 1 {
				t.Fatal("empty source EOS must explicitly publish exactly one terminal record")
			}
			kind, err := kafkautil.ParseRecordType((*wire)[0].Headers)
			if err != nil || kind != model.RecordTypePartitionEndOfReplay || string((*wire)[0].Key) != "0" || len((*wire)[0].Value) != 0 {
				t.Fatalf("bad terminal record: %+v", *wire)
			}
			if err := p.Process(context.Background(), sourceEOS(0)); err != nil || len(*wire) != 1 {
				t.Fatal("duplicate EOS changed output")
			}
			input := edgeInput(t, "any-edge", 0, 1)
			input.Partition = 0
			if err := p.Process(context.Background(), input); err == nil {
				t.Fatal("input accepted after empty EOS")
			}
		})
	}
}

func TestAggregateKeyAndActualPartitionValidation(t *testing.T) {
	input := edgeInput(t, "edge-0", 0, 1)
	// Ownership follows the actual Kafka partition, even if it differs from Hash.
	input.Partition = (input.Partition + 1) % model.SourcePartitionCount
	p, wire := processorFixture(t, input.Partition, cloudworker.DefaultWatermarkDelay)
	bad := input
	bad.Key = []byte("wrong-edge")
	if err := p.Process(context.Background(), bad); err == nil || len(*wire) != 0 {
		t.Fatal("mismatched Edge key accepted")
	}
	bad = input
	bad.Partition = (input.Partition + 1) % model.SourcePartitionCount
	if err := p.Process(context.Background(), bad); err == nil || len(*wire) != 0 {
		t.Fatal("wrong owner accepted")
	}
	bad = input
	bad.Value = []byte("malformed avro")
	if err := p.Process(context.Background(), bad); err == nil {
		t.Fatal("malformed payload accepted")
	}
	if err := p.Process(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if err := p.Process(context.Background(), sourceEOS(input.Partition)); err != nil {
		t.Fatal(err)
	}
	if err := p.Process(context.Background(), input); err == nil {
		t.Fatal("duplicate accepted after EOS")
	}
}

func TestLateWarningIncludesInputEventsAndStillCommits(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	input := edgeInput(t, "edge-0", 10, 7)
	p, wire := processorFixture(t, input.Partition, 0)
	if err := p.Process(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	count := len(*wire)
	// A retry of an already counted aggregate is late too. Its events field
	// measures this dropped attempt, not unique event loss.
	input.Offset = 42
	committed := false
	if err := processAndCommitMessage(context.Background(), input, p, func(kafka.Message) error { committed = true; return nil }); err != nil || !committed || len(*wire) != count {
		t.Fatalf("late input did not commit cleanly: %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{"level": "WARN", "msg": "CLOUD_LATE_RECORD_DROPPED", "worker": "test-worker", "source_partition": float64(input.Partition), "offset": float64(42), "aggregate_id": "edge-0:10", "edge_id": "edge-0", "events": float64(7), "cloud_window_end": testEpoch.Add(15 * time.Minute).Format(time.RFC3339), "watermark": testEpoch.Add(15 * time.Minute).Format(time.RFC3339)} {
		if entry[key] != want {
			t.Fatalf("warning %s=%v, want %v", key, entry[key], want)
		}
	}
	if err := p.Process(context.Background(), edgeInput(t, "edge-0", 15, 1)); err != nil {
		t.Fatal("late drop prevented subsequent input:", err)
	}
}

func TestFinalizedPartialsBeforeActualWatermarkAndCommit(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		input := edgeInput(t, "edge-0", 10, 1)
		p, _ := processorFixture(t, input.Partition, 2*time.Minute)
		if err := p.Process(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		var order []string
		p.publishMessage = func(_ context.Context, m kafka.Message) error {
			kind, err := kafkautil.ParseRecordType(m.Headers)
			if err != nil {
				return err
			}
			order = append(order, kind)
			if string(m.Key) != model.PartitionKey(input.Partition) {
				t.Fatal("wrong egress source key")
			}
			if kind == model.RecordTypePartitionProgress {
				progress, err := avrocodec.DecodePartitionProgress(m.Value)
				if err != nil || !progress.CompleteThrough.Equal(testEpoch.Add(18*time.Minute)) {
					t.Fatalf("progress must be actual unrounded watermark: %+v %v", progress, err)
				}
			}
			if failAt == len(order) {
				return errors.New("publish failed")
			}
			return nil
		}
		committed := false
		err := processAndCommitMessage(context.Background(), edgeInput(t, "edge-0", 15, 1), p, func(kafka.Message) error { committed = true; order = append(order, "commit"); return nil })
		if failAt == 0 {
			if err != nil || !reflect.DeepEqual(order, []string{model.RecordTypeCloudPartitionAggregate, model.RecordTypePartitionProgress, "commit"}) {
				t.Fatalf("publication order: %v %v", order, err)
			}
		} else if err == nil || committed {
			t.Fatal("committed after failed data/progress publication")
		}
	}
}

func TestDurationConfiguration(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "unused:9092")
	t.Setenv("SOURCE_PARTITION_COUNT", "6")
	t.Setenv("CLOUD_WINDOW_SIZE", "15m")
	t.Setenv("CLOUD_WATERMARK_DELAY", "5m")
	for _, value := range []string{"-1ns", "invalid", "999999999999999999h"} {
		t.Setenv("CLOUD_WATERMARK_DELAY", value)
		if _, err := loadCloudWorkerConfig(); err == nil {
			t.Fatalf("bad delay %q accepted", value)
		}
	}
	for _, value := range []string{"0", "0s", "1ns", "20m"} {
		t.Setenv("CLOUD_WATERMARK_DELAY", value)
		cfg, err := loadCloudWorkerConfig()
		want, _ := time.ParseDuration(value)
		if err != nil || cfg.WatermarkDelay != want {
			t.Fatalf("delay %q: %+v %v", value, cfg, err)
		}
	}
	for _, value := range []string{"0", "-1m", "invalid"} {
		t.Setenv("CLOUD_WINDOW_SIZE", value)
		if _, err := loadCloudWorkerConfig(); err == nil {
			t.Fatalf("bad window %q accepted", value)
		}
	}
	t.Setenv("CLOUD_WINDOW_SIZE", "30m")
	if cfg, err := loadCloudWorkerConfig(); err != nil || cfg.WindowSize != 30*time.Minute {
		t.Fatalf("custom window: %+v %v", cfg, err)
	}
}

func TestSourceEOSBalancerUsesActualPartition(t *testing.T) {
	for partition := 0; partition < model.SourcePartitionCount; partition++ {
		m := sourceEOS(partition)
		if got := (sourcePartitionBalancer{}).Balance(m, 5, 4, 3, 2, 1, 0); got != partition {
			t.Fatalf("EOS routed to %d, want %d", got, partition)
		}
	}
}
