package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"continuum/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

func TestEdgeIngressQueueKeepsAcceptedDataBeforeReservedEndOfReplay(t *testing.T) {
	queue, err := newEdgeIngressQueue(3)
	if err != nil {
		t.Fatal(err)
	}

	for _, payload := range []string{"A", "B", "C"} {
		if result := queue.TryEnqueueTelemetry([]byte(payload)); result != TelemetryEnqueued {
			t.Fatalf("enqueue %s=%d", payload, result)
		}
	}
	if result := queue.TryEnqueueTelemetry([]byte("D")); result != TelemetryDroppedQueueFull {
		t.Fatalf("queue piena result=%d", result)
	}
	if !queue.RegisterEndOfReplay([]byte("EOS")) {
		t.Fatal("EOS perso con data queue piena")
	}
	if queue.RegisterEndOfReplay([]byte("EOS-duplicate")) {
		t.Fatal("EOS duplicato registrato")
	}
	if result := queue.TryEnqueueTelemetry([]byte("post")); result != TelemetryDroppedAfterEOS {
		t.Fatalf("telemetry post-EOS result=%d", result)
	}

	queue.Close()
	var order []string
	for {
		record, ok := queue.Next()
		if !ok {
			break
		}
		order = append(order, string(record.Payload))
	}
	if got := strings.Join(order, ","); got != "A,B,C,EOS" {
		t.Fatalf("ordine ingress=%s", got)
	}
}

func TestEdgeMQTTCallbackOnlyEnqueuesAndCountsDrops(t *testing.T) {
	queue, err := newEdgeIngressQueue(1)
	if err != nil {
		t.Fatal(err)
	}
	stats := &EdgeStats{}
	handler := makeEdgeMessageHandler("edge-3", queue, stats)

	handler(nil, testMQTTMessage{
		topic:   telemetrySubscriptionTopic,
		payload: []byte("telemetry-a"),
	})
	handler(nil, testMQTTMessage{
		topic:   telemetrySubscriptionTopic,
		payload: []byte("telemetry-b"),
	})
	handler(nil, testMQTTMessage{
		topic:   replayEndTopic("edge-3"),
		payload: []byte("eos"),
	})
	handler(nil, testMQTTMessage{
		topic:   telemetrySubscriptionTopic,
		payload: []byte("telemetry-post"),
	})

	snapshot := stats.Snapshot()
	if snapshot.TelemetryReceived != 3 ||
		snapshot.IngressAccepted != 1 ||
		snapshot.IngressQueueDropped != 1 ||
		snapshot.PostEOSDropped != 1 ||
		snapshot.Processed != 0 ||
		snapshot.AggregatesEmitted != 0 {
		t.Fatalf("callback stats=%#v", snapshot)
	}

	first, ok := queue.Next()
	if !ok || first.Kind != EdgeIngressTelemetry || string(first.Payload) != "telemetry-a" {
		t.Fatalf("primo record=%#v ok=%t", first, ok)
	}
	second, ok := queue.Next()
	if !ok || second.Kind != EdgeIngressEndOfReplay || string(second.Payload) != "eos" {
		t.Fatalf("secondo record=%#v ok=%t", second, ok)
	}
}

func TestEdgeProcessorProcessesAcceptedTelemetryBeforeEndOfReplay(t *testing.T) {
	queue, err := newEdgeIngressQueue(3)
	if err != nil {
		t.Fatal(err)
	}
	stats := &EdgeStats{}
	messages := make([]kafka.Message, 0, 2)
	aggregator := &WindowAggregator{
		edgeID:     "edge-0",
		windowSize: 5 * time.Minute,
		kafkaTopic: "edge-aggregates",
		stats:      stats,
		publishMessage: func(_ context.Context, message kafka.Message) error {
			messages = append(messages, message)
			return nil
		},
	}
	processor := &EdgeProcessor{
		edgeID:     "edge-0",
		ingress:    queue,
		aggregator: aggregator,
		stats:      stats,
		now:        func() time.Time { return edgeTestTime(12, 0) },
	}

	for index, minute := range []int{1, 2, 3} {
		if result := queue.TryEnqueueTelemetry(sensorEventPayload(
			t,
			eventID(index),
			edgeTestTime(10, minute),
		)); result != TelemetryEnqueued {
			t.Fatalf("enqueue %d result=%d", index, result)
		}
	}
	if !queue.RegisterEndOfReplay(endOfReplayPayload(t, "edge-0")) {
		t.Fatal("EOS non registrato")
	}
	queue.Close()

	if err := processor.Run(); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 ||
		recordType(t, messages[0]) != model.RecordTypeEdgeAggregate ||
		recordType(t, messages[1]) != model.RecordTypeEndOfReplay {
		t.Fatalf("messaggi Kafka inattesi: %#v", messages)
	}
	aggregate := decodeEdgeAggregate(t, messages[0])
	if aggregate.Events != 3 {
		t.Fatalf("eventi aggregate=%d, attesi 3", aggregate.Events)
	}
	snapshot := stats.Snapshot()
	if snapshot.Processed != 3 ||
		snapshot.AggregatesEmitted != 1 ||
		snapshot.EndOfReplayProcessed != 1 {
		t.Fatalf("processor stats=%#v", snapshot)
	}
}

func TestEdgeProcessorDropsClosedWindowAndPostEOSWithoutReopening(t *testing.T) {
	queue, err := newEdgeIngressQueue(1)
	if err != nil {
		t.Fatal(err)
	}
	stats := &EdgeStats{}
	aggregator, _ := newTestEdgeAggregator()
	aggregator.stats = stats
	processor := &EdgeProcessor{
		edgeID:     "edge-0",
		ingress:    queue,
		aggregator: aggregator,
		stats:      stats,
		now:        func() time.Time { return edgeTestTime(12, 0) },
	}

	if err := processor.Process(EdgeIngressRecord{
		Kind:    EdgeIngressTelemetry,
		Payload: sensorEventPayload(t, "current", edgeTestTime(10, 6)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(EdgeIngressRecord{
		Kind:    EdgeIngressTelemetry,
		Payload: sensorEventPayload(t, "old", edgeTestTime(10, 1)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(EdgeIngressRecord{
		Kind:    EdgeIngressEndOfReplay,
		Payload: endOfReplayPayload(t, "edge-0"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(EdgeIngressRecord{
		Kind:    EdgeIngressTelemetry,
		Payload: sensorEventPayload(t, "post", edgeTestTime(10, 7)),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot := stats.Snapshot()
	if snapshot.Processed != 1 ||
		snapshot.OutOfOrderDropped != 1 ||
		snapshot.PostEOSDropped != 1 ||
		snapshot.EndOfReplayProcessed != 1 ||
		aggregator.current != nil || !aggregator.ended {
		t.Fatalf("stats=%#v current=%#v ended=%t", snapshot, aggregator.current, aggregator.ended)
	}
}

func TestEdgeProcessorCountsInvalidTelemetry(t *testing.T) {
	queue, _ := newEdgeIngressQueue(1)
	stats := &EdgeStats{}
	aggregator, _ := newTestEdgeAggregator()
	processor := &EdgeProcessor{
		edgeID:     "edge-0",
		ingress:    queue,
		aggregator: aggregator,
		stats:      stats,
		now:        time.Now,
	}

	if err := processor.Process(EdgeIngressRecord{
		Kind:    EdgeIngressTelemetry,
		Payload: []byte("not-json"),
	}); err != nil {
		t.Fatal(err)
	}
	if snapshot := stats.Snapshot(); snapshot.InvalidTelemetry != 1 || snapshot.Processed != 0 {
		t.Fatalf("stats=%#v", snapshot)
	}
}

func TestLoadEdgeIngressQueueCapacity(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
		ok    bool
	}{
		{"", defaultEdgeIngressQueueCapacity, true},
		{"25", 25, true},
		{"0", 0, false},
		{"invalid", 0, false},
	} {
		got, err := loadEdgeIngressQueueCapacity(func(string) string { return test.value })
		if test.ok && (err != nil || got != test.want) {
			t.Fatalf("value=%q got=%d err=%v", test.value, got, err)
		}
		if !test.ok && err == nil {
			t.Fatalf("value=%q accettato", test.value)
		}
	}
}

type testMQTTMessage struct {
	topic   string
	payload []byte
}

func (message testMQTTMessage) Duplicate() bool   { return false }
func (message testMQTTMessage) Qos() byte         { return 0 }
func (message testMQTTMessage) Retained() bool    { return false }
func (message testMQTTMessage) Topic() string     { return message.topic }
func (message testMQTTMessage) MessageID() uint16 { return 0 }
func (message testMQTTMessage) Payload() []byte   { return message.payload }
func (message testMQTTMessage) Ack()              {}

var _ mqtt.Message = testMQTTMessage{}

func sensorEventPayload(
	t *testing.T,
	eventID string,
	observedAt time.Time,
) []byte {
	t.Helper()
	payload, err := json.Marshal(model.SensorEvent{
		SchemaVersion: 1,
		EventID:       eventID,
		SensorID:      "sensor-1",
		SensorType:    "BME280",
		LocationID:    "location-1",
		Sequence:      1,
		ObservedAt:    observedAt,
		EmittedAt:     edgeTestTime(11, 0),
		Measurements: map[string]string{
			"temperature": "20",
			"humidity":    "50",
			"pressure":    "100000",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func endOfReplayPayload(t *testing.T, edgeID string) []byte {
	t.Helper()
	payload, err := json.Marshal(validEndOfReplay(edgeID))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
