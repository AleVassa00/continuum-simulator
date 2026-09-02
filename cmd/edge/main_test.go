package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"continuum/internal/model"
	"continuum/internal/mqtttopic"

	"github.com/segmentio/kafka-go"
)

func TestBuildMetricAggregateIncludesComposableSum(t *testing.T) {
	state := MetricState{
		Valid:   2,
		Invalid: 1,
		Sum:     7,
		Min:     2,
		Max:     5,
	}

	aggregate := buildMetricAggregate(state)

	if aggregate.Valid != 2 ||
		aggregate.Invalid != 1 ||
		aggregate.Sum != 7 {
		t.Fatalf("conteggi o somma inattesi: %#v", aggregate)
	}

	if aggregate.Average == nil ||
		*aggregate.Average != 3.5 {
		t.Fatalf("average inattesa: %v", aggregate.Average)
	}

	if aggregate.Min == nil || *aggregate.Min != 2 {
		t.Fatalf("min inatteso: %v", aggregate.Min)
	}

	if aggregate.Max == nil || *aggregate.Max != 5 {
		t.Fatalf("max inatteso: %v", aggregate.Max)
	}
}

func TestBuildMetricAggregateWithoutValidValues(t *testing.T) {
	aggregate := buildMetricAggregate(
		MetricState{
			Invalid: 3,
		},
	)

	if aggregate.Valid != 0 ||
		aggregate.Invalid != 3 ||
		aggregate.Sum != 0 ||
		aggregate.Average != nil ||
		aggregate.Min != nil ||
		aggregate.Max != nil {
		t.Fatalf("aggregato senza valori validi inatteso: %#v", aggregate)
	}
}

func TestBuildEdgeAggregateUsesCurrentSchema(t *testing.T) {
	start := time.Date(
		2026,
		time.January,
		1,
		10,
		0,
		0,
		0,
		time.UTC,
	)

	aggregate := buildEdgeAggregate(
		"edge-0",
		&WindowState{
			Start:  start,
			End:    start.Add(5 * time.Minute),
			Events: 1,
			Temperature: MetricState{
				Valid: 1,
				Sum:   20,
				Min:   20,
				Max:   20,
			},
			Humidity: MetricState{
				Valid: 1,
				Sum:   50,
				Min:   50,
				Max:   50,
			},
			Pressure: MetricState{
				Valid: 1,
				Sum:   100000,
				Min:   100000,
				Max:   100000,
			},
		},
	)

	if aggregate.SchemaVersion != model.EdgeAggregateSchemaVersion {
		t.Fatalf(
			"schema_version=%d, attesa %d",
			aggregate.SchemaVersion,
			model.EdgeAggregateSchemaVersion,
		)
	}
}

func TestEdgeKafkaWriterUsesSingleSynchronousAttempt(t *testing.T) {
	writer := newKafkaWriter("kafka:29092", "edge-aggregates")

	if writer.MaxAttempts != 1 ||
		writer.RequiredAcks != kafka.RequireAll ||
		writer.Async {
		t.Fatalf(
			"writer Kafka Edge inatteso: attempts=%d acks=%d async=%t",
			writer.MaxAttempts,
			writer.RequiredAcks,
			writer.Async,
		)
	}
}

func TestTelemetrySubscriptionUsesSensorScopedTopic(t *testing.T) {
	const expected = "sensors/+/telemetry"

	if mqtttopic.TelemetrySubscription != expected {
		t.Fatalf(
			"topic sottoscrizione=%q, atteso %q",
			mqtttopic.TelemetrySubscription,
			expected,
		)
	}
}

func TestEdgeSubscriptionsIncludeTelemetryAndOwnReplayEnd(t *testing.T) {
	topics := edgeSubscriptionTopics("edge-3")

	if len(topics) != 2 ||
		topics[mqtttopic.TelemetrySubscription] != 0 ||
		topics[mqtttopic.ReplayEnd("edge-3")] != 1 {
		t.Fatalf("subscription inattese: %#v", topics)
	}

	if _, found := topics[mqtttopic.ReplayEnd("edge-4")]; found {
		t.Fatal("Edge sottoscritto al control topic di un altro sito")
	}
}

func TestWindowAggregatorKeepsEventsInSameFiveMinuteWindow(t *testing.T) {
	aggregator, messages := newTestEdgeAggregator()

	for index, minute := range []int{1, 3} {
		err := aggregator.Add(
			eventID(index),
			edgeTestTime(10, minute),
			validMeasurement(20, 50, 100000),
		)
		if err != nil {
			t.Fatalf("Add() ha restituito un errore: %v", err)
		}
	}

	if len(*messages) != 0 {
		t.Fatalf("aggregati emessi=%d, attesi 0", len(*messages))
	}

	if aggregator.current == nil ||
		!aggregator.current.Start.Equal(edgeTestTime(10, 0)) ||
		!aggregator.current.End.Equal(edgeTestTime(10, 5)) ||
		aggregator.current.Events != 2 {
		t.Fatalf("stato della finestra inatteso: %#v", aggregator.current)
	}
}

func TestWindowAggregatorEmitsPreviousWindowOnTransition(t *testing.T) {
	aggregator, messages := newTestEdgeAggregator()

	for index, minute := range []int{1, 3, 6} {
		err := aggregator.Add(
			eventID(index),
			edgeTestTime(10, minute),
			validMeasurement(20, 50, 100000),
		)
		if err != nil {
			t.Fatalf("Add() ha restituito un errore: %v", err)
		}
	}

	if len(*messages) != 1 {
		t.Fatalf("aggregati emessi=%d, atteso 1", len(*messages))
	}

	emitted := decodeEdgeAggregate(t, (*messages)[0])
	if !emitted.WindowStart.Equal(edgeTestTime(10, 0)) ||
		!emitted.WindowEnd.Equal(edgeTestTime(10, 5)) ||
		emitted.Events != 2 {
		t.Fatalf("finestra emessa inattesa: %#v", emitted)
	}

	if string((*messages)[0].Key) != "edge-0" {
		t.Fatalf("chiave Kafka=%q, attesa edge-0", (*messages)[0].Key)
	}
	if recordType(t, (*messages)[0]) != model.RecordTypeEdgeAggregate {
		t.Fatalf("record_type aggregate inatteso: %#v", (*messages)[0].Headers)
	}

	if aggregator.current == nil ||
		!aggregator.current.Start.Equal(edgeTestTime(10, 5)) ||
		!aggregator.current.End.Equal(edgeTestTime(10, 10)) ||
		aggregator.current.Events != 1 {
		t.Fatalf("nuova finestra inattesa: %#v", aggregator.current)
	}
}

func TestWindowAggregatorRejectsEventFromPreviousWindow(t *testing.T) {
	aggregator, _ := newTestEdgeAggregator()
	measurement := validMeasurement(20, 50, 100000)

	if err := aggregator.Add(
		"event-current",
		edgeTestTime(10, 6),
		measurement,
	); err != nil {
		t.Fatalf("primo Add() ha restituito un errore: %v", err)
	}

	err := aggregator.Add(
		"event-old",
		edgeTestTime(10, 1),
		measurement,
	)
	if !errors.Is(err, errEdgeWindowClosed) {
		t.Fatal("evento appartenente a una finestra precedente accettato")
	}

	if aggregator.current.Events != 1 {
		t.Fatalf("evento fuori ordine ha modificato lo stato: %#v", aggregator.current)
	}
}

func TestWindowAggregatorRejectsEventsAfterEndOfReplay(t *testing.T) {
	aggregator, _ := newTestEdgeAggregator()
	if err := aggregator.EndReplay(
		validEndOfReplay("edge-0"),
		edgeTestTime(12, 0),
	); err != nil {
		t.Fatal(err)
	}

	err := aggregator.Add(
		"event-after-eos",
		edgeTestTime(12, 1),
		validMeasurement(20, 50, 100000),
	)
	if !errors.Is(err, errEdgeReplayEnded) {
		t.Fatalf("evento post-EOS non rifiutato: %v", err)
	}
	if aggregator.current != nil {
		t.Fatalf("finestra riaperta dopo EOS: %#v", aggregator.current)
	}
}

func TestWindowAggregatorFlushIsIdempotent(t *testing.T) {
	aggregator, messages := newTestEdgeAggregator()

	if err := aggregator.Add(
		"event-1",
		edgeTestTime(10, 1),
		validMeasurement(20, 50, 100000),
	); err != nil {
		t.Fatalf("Add() ha restituito un errore: %v", err)
	}

	if err := aggregator.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := aggregator.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(*messages) != 1 {
		t.Fatalf("aggregati emessi da due Flush=%d, atteso 1", len(*messages))
	}

	if aggregator.current != nil {
		t.Fatalf("finestra ancora attiva dopo Flush: %#v", aggregator.current)
	}
}

func TestWindowAggregatorTracksValidAndInvalidMetrics(t *testing.T) {
	aggregator, messages := newTestEdgeAggregator()
	measurements := []EdgeMeasurement{
		validMeasurement(20, 50, 100000),
		{
			Temperature: MetricValue{Valid: false},
			Humidity:    MetricValue{Value: 60, Valid: true},
			Pressure:    MetricValue{Valid: false},
		},
		{
			Temperature: MetricValue{Value: 30, Valid: true},
			Humidity:    MetricValue{Valid: false},
			Pressure:    MetricValue{Value: 90000, Valid: true},
		},
	}

	for index, measurement := range measurements {
		if err := aggregator.Add(
			eventID(index),
			edgeTestTime(10, index+1),
			measurement,
		); err != nil {
			t.Fatalf("Add() ha restituito un errore: %v", err)
		}
	}

	if err := aggregator.Flush(); err != nil {
		t.Fatal(err)
	}
	emitted := decodeEdgeAggregate(t, (*messages)[0])

	assertEdgeMetric(t, emitted.Temperature, 2, 1, 50, 25, 20, 30)
	assertEdgeMetric(t, emitted.Humidity, 2, 1, 110, 55, 50, 60)
	assertEdgeMetric(t, emitted.Pressure, 2, 1, 190000, 95000, 90000, 100000)
}

func TestWindowAggregatorFlushPropagatesPublishFailure(t *testing.T) {
	aggregator := &WindowAggregator{
		edgeID:     "edge-0",
		windowSize: 5 * time.Minute,
		kafkaTopic: "edge-aggregates",
		publishMessage: func(context.Context, kafka.Message) error {
			return errors.New("Kafka non disponibile")
		},
	}

	if err := aggregator.Add(
		"event-1",
		edgeTestTime(10, 1),
		validMeasurement(20, 50, 100000),
	); err != nil {
		t.Fatal(err)
	}

	err := aggregator.Flush()
	if err == nil || !strings.Contains(err.Error(), "Kafka non disponibile") {
		t.Fatalf("errore inatteso: %v", err)
	}

	if aggregator.current == nil {
		t.Fatal("stato finale perso dopo flush fallito")
	}
}

func TestEndOfReplayFlushesFinalWindowBeforeKafkaControl(t *testing.T) {
	aggregator, messages := newTestEdgeAggregator()
	lastEventTime := edgeTestTime(10, 3)
	if err := aggregator.Add(
		"event-1",
		lastEventTime,
		validMeasurement(20, 50, 100000),
	); err != nil {
		t.Fatal(err)
	}

	record := validEndOfReplay("edge-0")
	record.LastEventTime = lastEventTime
	edgeEmittedAt := edgeTestTime(12, 5)
	if err := aggregator.EndReplay(record, edgeEmittedAt); err != nil {
		t.Fatal(err)
	}

	if len(*messages) != 2 {
		t.Fatalf("messaggi Kafka=%d, attesi aggregate+control", len(*messages))
	}

	if recordType(t, (*messages)[0]) != model.RecordTypeEdgeAggregate ||
		recordType(t, (*messages)[1]) != model.RecordTypeEndOfReplay {
		t.Fatalf(
			"ordine record_type inatteso: %s, %s",
			recordType(t, (*messages)[0]),
			recordType(t, (*messages)[1]),
		)
	}

	for index, message := range *messages {
		if string(message.Key) != "edge-0" {
			t.Fatalf("messaggio %d key=%q", index, message.Key)
		}
	}

	finalAggregate := decodeEdgeAggregate(t, (*messages)[0])
	if finalAggregate.Events != 1 ||
		!finalAggregate.WindowStart.Equal(edgeTestTime(10, 0)) {
		t.Fatalf("aggregate finale inatteso: %#v", finalAggregate)
	}

	var forwarded model.EndOfReplay
	if err := json.Unmarshal((*messages)[1].Value, &forwarded); err != nil {
		t.Fatal(err)
	}
	if !forwarded.LastEventTime.Equal(lastEventTime) ||
		!forwarded.EmittedAt.Equal(edgeEmittedAt) {
		t.Fatalf("EndOfReplay inoltrato inatteso: %#v", forwarded)
	}

	if aggregator.current != nil || !aggregator.ended {
		t.Fatalf("stato finale Edge inatteso: current=%#v ended=%t", aggregator.current, aggregator.ended)
	}
}

func TestEndOfReplayDoesNotPropagateWhenFinalFlushFails(t *testing.T) {
	publishedTypes := make([]string, 0, 2)
	aggregator := &WindowAggregator{
		edgeID:     "edge-0",
		windowSize: 5 * time.Minute,
		kafkaTopic: "edge-aggregates",
		publishMessage: func(_ context.Context, message kafka.Message) error {
			publishedTypes = append(publishedTypes, recordType(t, message))
			return errors.New("aggregate finale fallito")
		},
	}

	if err := aggregator.Add(
		"event-1",
		edgeTestTime(10, 1),
		validMeasurement(20, 50, 100000),
	); err != nil {
		t.Fatal(err)
	}

	err := aggregator.EndReplay(
		validEndOfReplay("edge-0"),
		edgeTestTime(12, 0),
	)
	if err == nil || !strings.Contains(err.Error(), "aggregate finale fallito") {
		t.Fatalf("errore inatteso: %v", err)
	}

	if len(publishedTypes) != 1 ||
		publishedTypes[0] != model.RecordTypeEdgeAggregate ||
		aggregator.ended {
		t.Fatalf(
			"publish=%v ended=%t",
			publishedTypes,
			aggregator.ended,
		)
	}
}

func TestDuplicateEndOfReplayIsIgnored(t *testing.T) {
	aggregator, messages := newTestEdgeAggregator()
	if err := aggregator.Add(
		"event-1",
		edgeTestTime(10, 1),
		validMeasurement(20, 50, 100000),
	); err != nil {
		t.Fatal(err)
	}
	record := validEndOfReplay("edge-0")

	if err := aggregator.EndReplay(record, edgeTestTime(12, 0)); err != nil {
		t.Fatal(err)
	}
	if err := aggregator.EndReplay(record, edgeTestTime(12, 1)); err != nil {
		t.Fatal(err)
	}

	if len(*messages) != 2 {
		t.Fatalf("EOS duplicato ha prodotto %d messaggi", len(*messages))
	}
}

func TestRetrySubscriptionSucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	var waits []time.Duration

	attempts, err := retrySubscription(
		testRetryPolicy(),
		func() bool { return true },
		func(timeout time.Duration) error {
			if timeout != time.Second {
				t.Fatalf("timeout=%s, atteso 1s", timeout)
			}

			calls++
			if calls < 3 {
				return errors.New("errore temporaneo")
			}

			return nil
		},
		func(duration time.Duration) {
			waits = append(waits, duration)
		},
	)
	if err != nil {
		t.Fatalf("retrySubscription() ha restituito un errore: %v", err)
	}

	if attempts != 3 || calls != 3 {
		t.Fatalf("attempts=%d calls=%d, attesi 3 e 3", attempts, calls)
	}

	if len(waits) != 2 ||
		waits[0] != 10*time.Millisecond ||
		waits[1] != 20*time.Millisecond {
		t.Fatalf("backoff inattesi: %v", waits)
	}
}

func TestRetrySubscriptionStopsAfterConfiguredAttempts(t *testing.T) {
	calls := 0
	waits := 0

	attempts, err := retrySubscription(
		testRetryPolicy(),
		func() bool { return true },
		func(time.Duration) error {
			calls++
			return errors.New("errore temporaneo")
		},
		func(time.Duration) { waits++ },
	)
	if err == nil {
		t.Fatal("esaurimento retry non segnalato")
	}

	if attempts != 3 || calls != 3 || waits != 2 {
		t.Fatalf(
			"attempts=%d calls=%d waits=%d, attesi 3, 3, 2",
			attempts,
			calls,
			waits,
		)
	}
}

func TestRetrySubscriptionStopsWhenConnectionGenerationIsInvalidated(t *testing.T) {
	coordinator := &SubscriptionCoordinator{}
	generation := coordinator.Begin()
	calls := 0

	attempts, err := retrySubscription(
		testRetryPolicy(),
		func() bool { return coordinator.IsCurrent(generation) },
		func(time.Duration) error {
			calls++
			coordinator.Invalidate()
			return errors.New("connessione persa")
		},
		func(time.Duration) {
			t.Fatal("backoff eseguito dopo invalidazione")
		},
	)
	if !errors.Is(err, errSubscriptionInactive) {
		t.Fatalf("errore=%v, atteso errSubscriptionInactive", err)
	}

	if attempts != 1 || calls != 1 {
		t.Fatalf("attempts=%d calls=%d, attesi 1 e 1", attempts, calls)
	}
}

func TestWaitForShutdownReturnsProcessorCompletion(t *testing.T) {
	tests := []struct {
		name         string
		processorErr error
	}{
		{name: "success"},
		{name: "error", processorErr: errors.New("processor fallito")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			processorDone := make(chan error, 1)
			processorDone <- test.processorErr

			processorErr, processorFinished := waitForShutdown(ctx, processorDone)
			if !processorFinished || !errors.Is(processorErr, test.processorErr) {
				t.Fatalf(
					"processorErr=%v processorFinished=%t",
					processorErr,
					processorFinished,
				)
			}

			select {
			case <-processorDone:
				t.Fatal("risultato del processor non consumato una sola volta")
			default:
			}
		})
	}
}

func TestWaitForShutdownReturnsWhileProcessorIsStillRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	processorDone := make(chan error, 1)
	cancel()

	processorErr, processorFinished := waitForShutdown(ctx, processorDone)
	if processorErr != nil || processorFinished {
		t.Fatalf(
			"processorErr=%v processorFinished=%t",
			processorErr,
			processorFinished,
		)
	}

	want := errors.New("processor terminato durante cleanup")
	processorDone <- want
	if got := <-processorDone; !errors.Is(got, want) {
		t.Fatalf("risultato processor inatteso: %v", got)
	}
}

func TestReadinessStateTransitions(t *testing.T) {
	state := &ReadinessState{}

	if state.IsReady() {
		t.Fatal("lo stato iniziale deve essere not ready")
	}

	state.MarkReady()
	if !state.IsReady() {
		t.Fatal("la subscription completata deve rendere ready l'Edge")
	}

	state.MarkNotReady()
	if state.IsReady() {
		t.Fatal("la perdita di connessione deve rendere not ready l'Edge")
	}
}

func TestReadinessEndpoint(t *testing.T) {
	state := &ReadinessState{}

	assertReadinessStatus(
		t,
		state,
		http.StatusServiceUnavailable,
	)

	state.MarkReady()

	assertReadinessStatus(
		t,
		state,
		http.StatusOK,
	)
}

func assertReadinessStatus(
	t *testing.T,
	state *ReadinessState,
	expected int,
) {
	t.Helper()

	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)
	response := httptest.NewRecorder()

	state.ServeHTTP(
		response,
		request,
	)

	if response.Code != expected {
		t.Fatalf(
			"GET /readyz status=%d, atteso %d",
			response.Code,
			expected,
		)
	}
}

func newTestEdgeAggregator() (*WindowAggregator, *[]kafka.Message) {
	messages := make([]kafka.Message, 0)

	return &WindowAggregator{
		edgeID:     "edge-0",
		windowSize: 5 * time.Minute,
		kafkaTopic: "edge-aggregates",
		publishMessage: func(
			_ context.Context,
			message kafka.Message,
		) error {
			messages = append(messages, message)
			return nil
		},
	}, &messages
}

func decodeEdgeAggregate(
	t *testing.T,
	message kafka.Message,
) model.EdgeAggregate {
	t.Helper()

	var aggregate model.EdgeAggregate
	if err := json.Unmarshal(message.Value, &aggregate); err != nil {
		t.Fatalf("payload Kafka non valido: %v", err)
	}

	return aggregate
}

func recordType(
	t *testing.T,
	message kafka.Message,
) string {
	t.Helper()

	for _, header := range message.Headers {
		if header.Key == model.RecordTypeHeader {
			return string(header.Value)
		}
	}

	t.Fatalf("header %q mancante", model.RecordTypeHeader)
	return ""
}

func validEndOfReplay(
	edgeID string,
) model.EndOfReplay {
	return model.EndOfReplay{
		EdgeID:        edgeID,
		LastEventTime: edgeTestTime(10, 3),
		EmittedAt:     edgeTestTime(11, 0),
	}
}

func validMeasurement(
	temperature float64,
	humidity float64,
	pressure float64,
) EdgeMeasurement {
	return EdgeMeasurement{
		Temperature: MetricValue{Value: temperature, Valid: true},
		Humidity:    MetricValue{Value: humidity, Valid: true},
		Pressure:    MetricValue{Value: pressure, Valid: true},
	}
}

func assertEdgeMetric(
	t *testing.T,
	actual model.MetricAggregate,
	valid uint64,
	invalid uint64,
	sum float64,
	average float64,
	minimum float64,
	maximum float64,
) {
	t.Helper()

	if actual.Valid != valid || actual.Invalid != invalid {
		t.Fatalf("conteggi metrici inattesi: %#v", actual)
	}

	if math.Abs(actual.Sum-sum) > 1e-9 ||
		actual.Average == nil || math.Abs(*actual.Average-average) > 1e-9 ||
		actual.Min == nil || math.Abs(*actual.Min-minimum) > 1e-9 ||
		actual.Max == nil || math.Abs(*actual.Max-maximum) > 1e-9 {
		t.Fatalf("statistiche metriche inattese: %#v", actual)
	}
}

func edgeTestTime(
	hour int,
	minute int,
) time.Time {
	return time.Date(
		2026,
		time.January,
		1,
		hour,
		minute,
		0,
		0,
		time.UTC,
	)
}

func eventID(index int) string {
	return "event-" + string(rune('a'+index))
}

func testRetryPolicy() SubscriptionRetryPolicy {
	return SubscriptionRetryPolicy{
		Attempts: 3,
		Timeout:  time.Second,
		Backoff:  10 * time.Millisecond,
	}
}
