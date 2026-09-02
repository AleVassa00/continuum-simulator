package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"continuum/internal/globalaggregator"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

func TestGlobalProcessorRejectsKafkaKeyMismatch(t *testing.T) {
	processor := newTestGlobalProcessor(t, []string{"edge-0"}, discardGlobalSink)

	for name, message := range map[string]kafka.Message{
		"data": globalKafkaMessage(
			t,
			"edge-9",
			model.RecordTypeCloudEdgeAggregate,
			globalTestCloudAggregate("edge-0", commandTestTime(10, 0)),
		),
		"eos": globalKafkaMessage(
			t,
			"edge-9",
			model.RecordTypeEndOfReplay,
			globalTestEOS("edge-0"),
		),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := processor.Process(
				context.Background(),
				message,
			); err == nil || !strings.Contains(err.Error(), "non coerente") {
				t.Fatalf("key mismatch accettato: %v", err)
			}
		})
	}
}

func TestSinkFailurePreventsKafkaCommit(t *testing.T) {
	want := errors.New("sink down")
	processor := newTestGlobalProcessor(
		t,
		[]string{"edge-0"},
		func(context.Context, model.GlobalAggregate) error { return want },
	)
	commits := 0
	completed, err := processAndCommitMessage(
		context.Background(),
		globalKafkaMessage(
			t,
			"edge-0",
			model.RecordTypeCloudEdgeAggregate,
			globalTestCloudAggregate("edge-0", commandTestTime(10, 0)),
		),
		processor,
		func(kafka.Message) error {
			commits++
			return nil
		},
	)
	if !errors.Is(err, want) || completed {
		t.Fatalf("risultato inatteso: completed=%t err=%v", completed, err)
	}
	if commits != 0 {
		t.Fatalf("input committato %d volte dopo sink failure", commits)
	}
}

func TestLastEOSOrdersFlushSinkCommitAndCompletion(t *testing.T) {
	steps := make([]string, 0)
	processor := newTestGlobalProcessor(
		t,
		[]string{"edge-0", "edge-1"},
		func(_ context.Context, aggregate model.GlobalAggregate) error {
			steps = append(steps, "sink")
			if aggregate.ExpectedEdges != 2 || aggregate.ContributingEdges != 1 {
				t.Fatalf("counts globali inattesi: %#v", aggregate)
			}
			return nil
		},
	)

	if _, err := processor.Process(
		context.Background(),
		globalKafkaMessage(
			t,
			"edge-0",
			model.RecordTypeCloudEdgeAggregate,
			globalTestCloudAggregate("edge-0", commandTestTime(10, 0)),
		),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.Process(
		context.Background(),
		globalKafkaMessage(
			t,
			"edge-0",
			model.RecordTypeEndOfReplay,
			globalTestEOS("edge-0"),
		),
	); err != nil {
		t.Fatal(err)
	}

	completed, err := processAndCommitMessage(
		context.Background(),
		globalKafkaMessage(
			t,
			"edge-1",
			model.RecordTypeEndOfReplay,
			globalTestEOS("edge-1"),
		),
		processor,
		func(kafka.Message) error {
			steps = append(steps, "commit")
			return nil
		},
	)
	if err != nil || !completed {
		t.Fatalf("ultimo EOS: completed=%t err=%v", completed, err)
	}
	if completed {
		steps = append(steps, "completion")
	}
	want := "sink,commit,completion"
	if strings.Join(steps, ",") != want {
		t.Fatalf("ordine=%q, atteso %q", strings.Join(steps, ","), want)
	}
}

func TestLastEOSCommitFailurePreventsCompletion(t *testing.T) {
	processor := newTestGlobalProcessor(t, []string{"edge-0"}, discardGlobalSink)
	want := errors.New("commit down")
	completed, err := processAndCommitMessage(
		context.Background(),
		globalKafkaMessage(
			t,
			"edge-0",
			model.RecordTypeEndOfReplay,
			globalTestEOS("edge-0"),
		),
		processor,
		func(kafka.Message) error { return want },
	)
	if completed || !errors.Is(err, want) {
		t.Fatalf("completion prima del commit: completed=%t err=%v", completed, err)
	}
}

func TestGlobalProcessorRejectsUnknownRecordType(t *testing.T) {
	processor := newTestGlobalProcessor(t, []string{"edge-0"}, discardGlobalSink)
	message := kafka.Message{
		Headers: []kafka.Header{{
			Key:   model.RecordTypeHeader,
			Value: []byte("window_progress"),
		}},
	}
	if _, err := processor.Process(
		context.Background(),
		message,
	); err == nil || !strings.Contains(err.Error(), "sconosciuto") {
		t.Fatalf("record type sconosciuto accettato: %v", err)
	}
}

func TestJSONLogSinkUsesRecognizablePrefixAndValidJSON(t *testing.T) {
	var buffer bytes.Buffer
	sink := newJSONLogSink(&buffer)
	aggregate := model.GlobalAggregate{
		SchemaVersion:     model.GlobalAggregateSchemaVersion,
		AggregateID:       "global:window",
		WindowStart:       commandTestTime(10, 0),
		WindowEnd:         commandTestTime(10, 15),
		ExpectedEdges:     2,
		ContributingEdges: 1,
		Events:            1,
		Temperature:       commandTestMetric(),
		Humidity:          commandTestMetric(),
		Pressure:          commandTestMetric(),
		EmittedAt:         commandTestTime(11, 0),
	}
	if err := sink(context.Background(), aggregate); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(buffer.String())
	const prefix = "GLOBAL_AGGREGATE "
	if !strings.HasPrefix(line, prefix) {
		t.Fatalf("log senza prefisso: %q", line)
	}
	var decoded model.GlobalAggregate
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &decoded); err != nil {
		t.Fatalf("JSON log non valido: %v", err)
	}
	if decoded.AggregateID != aggregate.AggregateID {
		t.Fatalf("aggregate_id=%q", decoded.AggregateID)
	}
}

func TestLoadGlobalConfiguration(t *testing.T) {
	env := map[string]string{
		"GLOBAL_WINDOW_SIZE": "30m",
		"EXPECTED_EDGE_IDS":  "edge-0, edge-2,edge-7",
	}
	getenv := func(name string) string { return env[name] }
	window, err := loadGlobalWindowSize(getenv)
	if err != nil || window != 30*time.Minute {
		t.Fatalf("window=%s err=%v", window, err)
	}
	edges, err := loadExpectedEdgeIDs(getenv)
	if err != nil || strings.Join(edges, ",") != "edge-0,edge-2,edge-7" {
		t.Fatalf("edges=%v err=%v", edges, err)
	}
	if value := envOrDefault(getenv, "KAFKA_INPUT_TOPIC", "cloud-edge-aggregates"); value != "cloud-edge-aggregates" {
		t.Fatalf("topic default=%q", value)
	}
}

func TestLoadGlobalConfigurationRejectsInvalidValues(t *testing.T) {
	if _, err := loadExpectedEdgeIDs(func(string) string { return " " }); err == nil {
		t.Fatal("EXPECTED_EDGE_IDS vuota accettata")
	}
	if _, err := loadGlobalWindowSize(func(string) string { return "0s" }); err == nil {
		t.Fatal("GLOBAL_WINDOW_SIZE zero accettata")
	}
}

func newTestGlobalProcessor(
	t *testing.T,
	expectedEdgeIDs []string,
	sink globalaggregator.GlobalAggregateSink,
) *GlobalMessageProcessor {
	t.Helper()
	aggregator, err := globalaggregator.New(
		expectedEdgeIDs,
		15*time.Minute,
		sink,
	)
	if err != nil {
		t.Fatal(err)
	}
	return &GlobalMessageProcessor{aggregator: aggregator}
}

func discardGlobalSink(context.Context, model.GlobalAggregate) error {
	return nil
}

func globalKafkaMessage(
	t *testing.T,
	key string,
	recordType string,
	value interface{},
) kafka.Message {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Headers: []kafka.Header{{
			Key:   model.RecordTypeHeader,
			Value: []byte(recordType),
		}},
	}
}

func globalTestCloudAggregate(
	edgeID string,
	start time.Time,
) model.CloudEdgeAggregate {
	end := start.Add(15 * time.Minute)
	metric := commandTestMetric()
	return model.CloudEdgeAggregate{
		SchemaVersion: model.CloudEdgeAggregateSchemaVersion,
		AggregateID: fmt.Sprintf(
			"cloud:%s:%s:%s",
			edgeID,
			start.Format(time.RFC3339),
			end.Format(time.RFC3339),
		),
		EdgeID:          edgeID,
		WindowStart:     start,
		WindowEnd:       end,
		InputAggregates: 3,
		Events:          1,
		Temperature:     metric,
		Humidity:        metric,
		Pressure:        metric,
		EmittedAt:       commandTestTime(10, 16),
	}
}

func commandTestMetric() model.MetricAggregate {
	average := 10.0
	minimum := 10.0
	maximum := 10.0
	return model.MetricAggregate{
		Valid:   1,
		Sum:     10,
		Average: &average,
		Min:     &minimum,
		Max:     &maximum,
	}
}

func globalTestEOS(edgeID string) model.EndOfReplay {
	return model.EndOfReplay{
		EdgeID:        edgeID,
		LastEventTime: commandTestTime(10, 14),
		EmittedAt:     commandTestTime(10, 17),
	}
}

func commandTestTime(hour int, minute int) time.Time {
	return time.Date(2025, time.January, 1, hour, minute, 0, 0, time.UTC)
}
