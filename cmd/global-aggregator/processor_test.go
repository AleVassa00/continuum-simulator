package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/globalaggregator"
	"continuum/internal/model"
	"errors"
	"github.com/segmentio/kafka-go"
	"testing"
	"time"
)

func TestGlobalAcceptsOnlyPartitionContracts(t *testing.T) {
	a, _ := globalaggregator.New(func(context.Context, model.GlobalAggregate) error { return nil })
	p := &GlobalMessageProcessor{aggregator: a}
	progress := model.PartitionProgress{SourcePartition: 0, CompleteThrough: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	payload, err := avrocodec.EncodePartitionProgress(progress)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []kafka.Message{
		{Key: []byte("edge-0"), Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypeEndOfReplay)}}},
		{Key: []byte("1"), Value: payload, Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypePartitionProgress)}}},
		{Key: []byte("6"), Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypePartitionEndOfReplay)}}},
	} {
		committed := false
		_, err := processAndCommitMessage(context.Background(), msg, p, func(kafka.Message) error { committed = true; return nil })
		if err == nil || committed {
			t.Fatal("invalid protocol committed")
		}
	}
}
func TestGlobalDoesNotReportCompletionBeforeCommit(t *testing.T) {
	a, _ := globalaggregator.New(func(context.Context, model.GlobalAggregate) error { return nil })
	p := &GlobalMessageProcessor{aggregator: a}
	for i := 0; i < 5; i++ {
		a.EndPartition(context.Background(), i)
	}
	msg := kafka.Message{Key: []byte("5"), Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypePartitionEndOfReplay)}}}
	complete, err := processAndCommitMessage(context.Background(), msg, p, func(kafka.Message) error { return errors.New("commit failed") })
	if complete || err == nil {
		t.Fatal("reported success despite commit failure")
	}
}
func TestGlobalConfigWithoutEdgesOrTimeSettings(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "unused:9092")
	t.Setenv("SOURCE_PARTITION_COUNT", "6")
	t.Setenv("EXPECTED_EDGE_IDS", "")
	t.Setenv("GLOBAL_SINK_TYPE", "log")
	cfg, err := loadGlobalAggregatorConfig()
	if err != nil || cfg.InputTopic != "cloud-partition-aggregates" {
		t.Fatalf("%+v %v", cfg, err)
	}
	t.Setenv("SOURCE_PARTITION_COUNT", "7")
	if _, err := loadGlobalAggregatorConfig(); err == nil {
		t.Fatal("count changed")
	}
}
