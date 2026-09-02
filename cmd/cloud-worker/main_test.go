package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

func TestCloudMessageProcessorDispatchesEdgeAggregate(t *testing.T) {
	published := make([]kafka.Message, 0)
	processor := newTestCloudProcessor(t, func(
		_ context.Context,
		message kafka.Message,
	) error {
		published = append(published, message)
		return nil
	})

	start := cloudTestTime(10, 0)
	if err := processor.Process(edgeAggregateKafkaMessage(
		t,
		cloudTestEdgeAggregate("edge-1", start),
	)); err != nil {
		t.Fatalf("primo EdgeAggregate rifiutato: %v", err)
	}

	if len(published) != 0 {
		t.Fatalf("pubblicati %d messaggi prima del cambio finestra", len(published))
	}

	if err := processor.Process(edgeAggregateKafkaMessage(
		t,
		cloudTestEdgeAggregate("edge-1", start.Add(15*time.Minute)),
	)); err != nil {
		t.Fatalf("secondo EdgeAggregate rifiutato: %v", err)
	}

	if len(published) != 1 {
		t.Fatalf("messaggi pubblicati=%d, atteso 1", len(published))
	}

	if got := cloudRecordType(t, published[0]); got != model.RecordTypeCloudEdgeAggregate {
		t.Fatalf("record_type=%q, atteso %q", got, model.RecordTypeCloudEdgeAggregate)
	}

	if string(published[0].Key) != "edge-1" {
		t.Fatalf("key Kafka=%q, attesa edge-1", published[0].Key)
	}
}

func TestCloudKafkaWriterUsesSingleSynchronousAttempt(t *testing.T) {
	writer := newKafkaWriter("kafka:29092", "cloud-edge-aggregates")

	if writer.MaxAttempts != 1 ||
		writer.RequiredAcks != kafka.RequireAll ||
		writer.Async {
		t.Fatalf(
			"writer Kafka Cloud inatteso: attempts=%d acks=%d async=%t",
			writer.MaxAttempts,
			writer.RequiredAcks,
			writer.Async,
		)
	}
}

func TestCloudMessageProcessorRejectsMissingAndUnknownRecordType(t *testing.T) {
	processor := newTestCloudProcessor(t, func(
		context.Context,
		kafka.Message,
	) error {
		return nil
	})

	for name, message := range map[string]kafka.Message{
		"missing": {},
		"unknown": {
			Headers: []kafka.Header{{
				Key:   model.RecordTypeHeader,
				Value: []byte("unknown"),
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := processor.Process(message); err == nil {
				t.Fatal("messaggio senza record_type supportato accettato")
			}
		})
	}
}

func TestCloudEndOfReplayFlushesOnlyItsEdgeBeforeControlAndCommit(t *testing.T) {
	steps := make([]string, 0)
	published := make([]kafka.Message, 0)
	processor := newTestCloudProcessor(t, func(
		_ context.Context,
		message kafka.Message,
	) error {
		recordType := cloudRecordType(t, message)
		steps = append(steps, "publish:"+recordType)
		published = append(published, message)
		return nil
	})

	start := cloudTestTime(10, 0)
	for _, edgeID := range []string{"edge-1", "edge-2"} {
		if err := processor.Process(edgeAggregateKafkaMessage(
			t,
			cloudTestEdgeAggregate(edgeID, start),
		)); err != nil {
			t.Fatalf("preparazione stato %s fallita: %v", edgeID, err)
		}
	}

	input := cloudTestEndOfReplay("edge-1")
	message := endOfReplayKafkaMessage(t, input, "edge-1")
	err := processAndCommitMessage(
		message,
		processor,
		func(kafka.Message) error {
			steps = append(steps, "commit")
			return nil
		},
	)
	if err != nil {
		t.Fatalf("EndOfReplay rifiutato: %v", err)
	}

	wantSteps := []string{
		"publish:" + model.RecordTypeCloudEdgeAggregate,
		"publish:" + model.RecordTypeEndOfReplay,
		"commit",
	}
	if strings.Join(steps, ",") != strings.Join(wantSteps, ",") {
		t.Fatalf("ordine operazioni=%v, atteso %v", steps, wantSteps)
	}

	if len(published) != 2 {
		t.Fatalf("messaggi pubblicati=%d, attesi 2", len(published))
	}
	for index, output := range published {
		if string(output.Key) != "edge-1" {
			t.Fatalf("output %d key=%q, attesa edge-1", index, output.Key)
		}
	}

	forwarded := decodeCloudEndOfReplay(t, published[1])
	if !forwarded.LastObservedAt.Equal(input.LastObservedAt) {
		t.Fatalf(
			"LastObservedAt=%s, atteso %s",
			forwarded.LastObservedAt,
			input.LastObservedAt,
		)
	}
	if !forwarded.EmittedAt.Equal(cloudTestTime(12, 0)) {
		t.Fatalf("EmittedAt=%s, atteso clock Cloud", forwarded.EmittedAt)
	}

	if _, found := processor.aggregator.FlushEdge("edge-1"); found {
		t.Fatal("lo stato edge-1 non e stato rimosso")
	}
	if output, found := processor.aggregator.FlushEdge("edge-2"); !found || output.EdgeID != "edge-2" {
		t.Fatalf("lo stato edge-2 e stato modificato: output=%#v found=%t", output, found)
	}
}

func TestCloudEndOfReplayWithoutStateIsStillPublished(t *testing.T) {
	published := make([]kafka.Message, 0)
	processor := newTestCloudProcessor(t, func(
		_ context.Context,
		message kafka.Message,
	) error {
		published = append(published, message)
		return nil
	})

	if err := processor.Process(endOfReplayKafkaMessage(
		t,
		cloudTestEndOfReplay("edge-7"),
		"edge-7",
	)); err != nil {
		t.Fatalf("EndOfReplay senza stato rifiutato: %v", err)
	}

	if len(published) != 1 ||
		cloudRecordType(t, published[0]) != model.RecordTypeEndOfReplay {
		t.Fatalf("output inatteso: %#v", published)
	}
}

func TestCloudEndOfReplayRejectsKafkaKeyMismatch(t *testing.T) {
	processor := newTestCloudProcessor(t, func(
		context.Context,
		kafka.Message,
	) error {
		return nil
	})

	err := processor.Process(endOfReplayKafkaMessage(
		t,
		cloudTestEndOfReplay("edge-1"),
		"edge-2",
	))
	if err == nil || !strings.Contains(err.Error(), "non coerente") {
		t.Fatalf("mismatch key/edge non rilevato: %v", err)
	}
}

func TestCloudEndOfReplayDoesNotPublishControlWhenFinalAggregateFails(t *testing.T) {
	publishedTypes := make([]string, 0)
	processor := newTestCloudProcessor(t, func(
		_ context.Context,
		message kafka.Message,
	) error {
		publishedTypes = append(publishedTypes, cloudRecordType(t, message))
		return errors.New("Kafka indisponibile")
	})

	if err := processor.Process(edgeAggregateKafkaMessage(
		t,
		cloudTestEdgeAggregate("edge-4", cloudTestTime(10, 0)),
	)); err != nil {
		t.Fatalf("preparazione stato fallita: %v", err)
	}

	err := processor.Process(endOfReplayKafkaMessage(
		t,
		cloudTestEndOfReplay("edge-4"),
		"edge-4",
	))
	if err == nil {
		t.Fatal("errore pubblicazione aggregate finale ignorato")
	}
	if len(publishedTypes) != 1 ||
		publishedTypes[0] != model.RecordTypeCloudEdgeAggregate {
		t.Fatalf("pubblicazioni inattese: %v", publishedTypes)
	}
}

func TestCloudEndOfReplayPublishFailurePreventsInputCommit(t *testing.T) {
	publishCount := 0
	commitCount := 0
	processor := newTestCloudProcessor(t, func(
		_ context.Context,
		_ kafka.Message,
	) error {
		publishCount++
		if publishCount == 2 {
			return errors.New("EOS Kafka fallito")
		}
		return nil
	})

	if err := processor.Process(edgeAggregateKafkaMessage(
		t,
		cloudTestEdgeAggregate("edge-5", cloudTestTime(10, 0)),
	)); err != nil {
		t.Fatalf("preparazione stato fallita: %v", err)
	}

	err := processAndCommitMessage(
		endOfReplayKafkaMessage(
			t,
			cloudTestEndOfReplay("edge-5"),
			"edge-5",
		),
		processor,
		func(kafka.Message) error {
			commitCount++
			return nil
		},
	)
	if err == nil {
		t.Fatal("errore pubblicazione EndOfReplay ignorato")
	}
	if publishCount != 2 {
		t.Fatalf("publish count=%d, atteso 2", publishCount)
	}
	if commitCount != 0 {
		t.Fatalf("input EndOfReplay committato %d volte", commitCount)
	}
}

func TestCloudEndOfReplayDuplicateIsNotRepublished(t *testing.T) {
	publishCount := 0
	processor := newTestCloudProcessor(t, func(
		context.Context,
		kafka.Message,
	) error {
		publishCount++
		return nil
	})
	message := endOfReplayKafkaMessage(
		t,
		cloudTestEndOfReplay("edge-6"),
		"edge-6",
	)

	for attempt := 0; attempt < 2; attempt++ {
		if err := processor.Process(message); err != nil {
			t.Fatalf("tentativo %d fallito: %v", attempt+1, err)
		}
	}

	if publishCount != 1 {
		t.Fatalf("EndOfReplay pubblicato %d volte, atteso 1", publishCount)
	}
}

func TestCloudRejectsAggregateAfterEndOfReplayForSameEdge(t *testing.T) {
	processor := newTestCloudProcessor(t, func(
		context.Context,
		kafka.Message,
	) error {
		return nil
	})

	if err := processor.Process(endOfReplayKafkaMessage(
		t,
		cloudTestEndOfReplay("edge-1"),
		"edge-1",
	)); err != nil {
		t.Fatal(err)
	}

	err := processor.Process(edgeAggregateKafkaMessage(
		t,
		cloudTestEdgeAggregate("edge-1", cloudTestTime(10, 0)),
	))
	if err == nil || !strings.Contains(err.Error(), "dopo EndOfReplay") {
		t.Fatalf("aggregate post-EOS non rifiutato: %v", err)
	}
}

func TestCloudOtherEdgeContinuesAfterOneEdgeEnds(t *testing.T) {
	processor := newTestCloudProcessor(t, func(
		context.Context,
		kafka.Message,
	) error {
		return nil
	})

	if err := processor.Process(endOfReplayKafkaMessage(
		t,
		cloudTestEndOfReplay("edge-1"),
		"edge-1",
	)); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(edgeAggregateKafkaMessage(
		t,
		cloudTestEdgeAggregate("edge-2", cloudTestTime(10, 0)),
	)); err != nil {
		t.Fatalf("aggregate di edge-2 rifiutato dopo EOS edge-1: %v", err)
	}

	output, found := processor.aggregator.FlushEdge("edge-2")
	if !found || output.EdgeID != "edge-2" {
		t.Fatalf("stato edge-2 inatteso: output=%#v found=%t", output, found)
	}
}

func newTestCloudProcessor(
	t *testing.T,
	publish KafkaMessagePublisher,
) *CloudMessageProcessor {
	t.Helper()

	aggregator, err := cloudworker.NewWindowAggregator(15 * time.Minute)
	if err != nil {
		t.Fatalf("NewWindowAggregator() fallita: %v", err)
	}

	return &CloudMessageProcessor{
		aggregator:     aggregator,
		outputTopic:    "cloud-edge-aggregates",
		workerID:       "worker-test",
		publishMessage: publish,
		now: func() time.Time {
			return cloudTestTime(12, 0)
		},
		endedEdges: make(map[string]bool),
	}
}

func cloudTestEdgeAggregate(
	edgeID string,
	start time.Time,
) model.EdgeAggregate {
	value := 20.0
	metric := model.MetricAggregate{
		Valid:   1,
		Sum:     value,
		Average: &value,
		Min:     &value,
		Max:     &value,
	}

	return model.EdgeAggregate{
		SchemaVersion: model.EdgeAggregateSchemaVersion,
		AggregateID: edgeID + ":" +
			start.Format(time.RFC3339),
		EdgeID:      edgeID,
		WindowStart: start,
		WindowEnd:   start.Add(5 * time.Minute),
		Events:      1,
		Temperature: metric,
		Humidity:    metric,
		Pressure:    metric,
		EmittedAt:   cloudTestTime(11, 0),
	}
}

func cloudTestEndOfReplay(edgeID string) model.EndOfReplay {
	return model.EndOfReplay{
		EdgeID:         edgeID,
		LastObservedAt: cloudTestTime(10, 14),
		EmittedAt:      cloudTestTime(10, 15),
	}
}

func edgeAggregateKafkaMessage(
	t *testing.T,
	aggregate model.EdgeAggregate,
) kafka.Message {
	t.Helper()
	return cloudKafkaMessage(
		t,
		aggregate.EdgeID,
		model.RecordTypeEdgeAggregate,
		aggregate,
	)
}

func endOfReplayKafkaMessage(
	t *testing.T,
	record model.EndOfReplay,
	key string,
) kafka.Message {
	t.Helper()
	return cloudKafkaMessage(
		t,
		key,
		model.RecordTypeEndOfReplay,
		record,
	)
}

func cloudKafkaMessage(
	t *testing.T,
	key string,
	recordType string,
	payload any,
) kafka.Message {
	t.Helper()

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() fallita: %v", err)
	}

	return kafka.Message{
		Key:   []byte(key),
		Value: encoded,
		Headers: []kafka.Header{{
			Key:   model.RecordTypeHeader,
			Value: []byte(recordType),
		}},
	}
}

func cloudRecordType(
	t *testing.T,
	message kafka.Message,
) string {
	t.Helper()

	recordType, err := kafkaRecordType(message.Headers)
	if err != nil {
		t.Fatalf("record_type non valido: %v", err)
	}

	return recordType
}

func decodeCloudEndOfReplay(
	t *testing.T,
	message kafka.Message,
) model.EndOfReplay {
	t.Helper()

	record, err := decodeEndOfReplay(message.Value)
	if err != nil {
		t.Fatalf("decodeEndOfReplay() fallita: %v", err)
	}

	return record
}

func cloudTestTime(hour int, minute int) time.Time {
	return time.Date(
		2026,
		time.August,
		28,
		hour,
		minute,
		0,
		0,
		time.UTC,
	)
}
