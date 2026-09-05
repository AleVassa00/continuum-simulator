package main

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"continuum/internal/model"
)

func TestAggregatorReturnsWindowsWithoutOutput(t *testing.T) {
	aggregator := &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute}
	start := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	measurement := EdgeMeasurement{Temperature: MetricValue{Value: 20, Valid: true}}
	if aggregate := aggregator.Flush(); aggregate != nil {
		t.Fatalf("flush vuoto: %#v", aggregate)
	}
	for _, offset := range []time.Duration{time.Minute, 3 * time.Minute} {
		if aggregate, err := aggregator.Add("same-window", start.Add(offset), measurement); err != nil || aggregate != nil {
			t.Fatalf("stessa finestra: aggregate=%#v err=%v", aggregate, err)
		}
	}
	aggregate, err := aggregator.Add("next-window", start.Add(5*time.Minute), measurement)
	if err != nil || aggregate == nil {
		t.Fatalf("transizione: aggregate=%#v err=%v", aggregate, err)
	}
	if aggregate.Events != 2 || !aggregate.WindowStart.Equal(start) ||
		!aggregate.WindowEnd.Equal(start.Add(5*time.Minute)) ||
		aggregate.Temperature.Valid != 2 || aggregate.Temperature.Sum != 40 ||
		aggregate.Humidity.Invalid != 2 || aggregate.Humidity.Average != nil {
		t.Fatalf("finestra precedente: %#v", aggregate)
	}
	if aggregate, err := aggregator.Add("closed-window", start, measurement); aggregate != nil || !errors.Is(err, errEdgeWindowClosed) {
		t.Fatalf("finestra chiusa: aggregate=%#v err=%v", aggregate, err)
	}
	final, err := aggregator.EndReplay()
	if err != nil || final == nil || final.Events != 1 || !final.WindowStart.Equal(start.Add(5*time.Minute)) {
		t.Fatalf("finestra finale: aggregate=%#v err=%v", final, err)
	}
	if aggregator.current != nil || !aggregator.ended || aggregator.Flush() != nil {
		t.Fatal("stato finale o flush ripetuto inatteso")
	}
	if aggregate, err := aggregator.EndReplay(); aggregate != nil || !errors.Is(err, errEdgeReplayEnded) {
		t.Fatalf("EOS duplicato: aggregate=%#v err=%v", aggregate, err)
	}
	if aggregate, err := aggregator.Add("post-eos", start.Add(10*time.Minute), measurement); aggregate != nil || !errors.Is(err, errEdgeReplayEnded) {
		t.Fatalf("post-EOS: aggregate=%#v err=%v", aggregate, err)
	}
}

func TestProcessorDrainsIngressAndClosesOutput(t *testing.T) {
	for _, eos := range []bool{false, true} {
		name := "shutdown"
		if eos {
			name = "eos"
		}
		t.Run(name, func(t *testing.T) {
			ingress := newEdgeIngress(3)
			output := make(chan EdgeOutputRecord, 3)
			stats := &EdgeStats{}
			processor := &EdgeProcessor{
				edgeID: "edge-0", ingress: ingress, stats: stats, output: output,
				aggregator: &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute},
			}
			for _, minute := range []int{1, 3, 6} {
				if got := ingress.TryEnqueueTelemetry(processorTestPayload(t, minute)); got != TelemetryEnqueued {
					t.Fatalf("enqueue=%v", got)
				}
			}
			if eos {
				ingress.RegisterEndOfReplay() // Lo slot N+1 deve accettare EOS anche a queue piena.
				ingress.RegisterEndOfReplay()
				if got := ingress.TryEnqueueTelemetry(nil); got != TelemetryDroppedAfterEOS {
					t.Fatalf("post-EOS enqueue=%v", got)
				}
			}
			ingress.Close()
			if err := processor.Run(); err != nil {
				t.Fatal(err)
			}
			for index, events := range []uint64{2, 1} {
				record := <-output
				if record.Kind != EdgeOutputAggregate || record.Aggregate.Events != events {
					t.Fatalf("record %d: %#v", index, record)
				}
				if eos && index == 1 {
					control := <-output
					if control.Kind != EdgeOutputEndOfReplay ||
						!control.EndOfReplay.LastEventTime.Equal(processor.lastEventTime) ||
						control.EndOfReplay.EmittedAt.Before(record.Aggregate.EmittedAt) {
						t.Fatalf("EOS dopo aggregate: %#v", control)
					}
				}
			}
			if record, ok := <-output; ok {
				t.Fatalf("output non chiuso o record duplicato: %#v", record)
			}
			if snapshot := stats.Snapshot(); snapshot.Processed != 3 || snapshot.AggregatesEmitted != 0 || snapshot.EndOfReplayProcessed != 0 {
				t.Fatalf("statistiche processor: %#v", snapshot)
			}
			if processor.aggregator.current != nil || processor.aggregator.ended != eos || ingress.telemetryQueued != 0 {
				t.Fatal("pipeline non drenata correttamente")
			}
		})
	}
}

func TestProcessorCreatesEOSAfterFinalAggregateSend(t *testing.T) {
	ingress := newEdgeIngress(1)
	ingress.TryEnqueueTelemetry(processorTestPayload(t, 1))
	ingress.RegisterEndOfReplay()
	ingress.Close()
	output := make(chan EdgeOutputRecord)
	stopped := make(chan struct{})
	defer close(stopped)
	processor := &EdgeProcessor{
		edgeID: "edge-0", ingress: ingress, output: output, egressStopped: stopped, stats: &EdgeStats{},
		aggregator: &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute},
	}
	done := make(chan error, 1)
	go func() { done <- processor.Run() }()
	// Senza ricevitore il send dell'aggregate blocca: EOS deve essere creato dopo.
	time.Sleep(20 * time.Millisecond)
	beforeReceive := time.Now().UTC()
	select {
	case record := <-output:
		if record.Kind != EdgeOutputAggregate {
			t.Fatalf("primo record: %#v", record)
		}
	case <-time.After(time.Second):
		t.Fatal("aggregate finale non ricevuto")
	}
	select {
	case record := <-output:
		if record.Kind != EdgeOutputEndOfReplay || record.EndOfReplay.EmittedAt.Before(beforeReceive) {
			t.Fatalf("EOS creato prima del send finale: %#v", record)
		}
	case <-time.After(time.Second):
		t.Fatal("EOS non ricevuto")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestProcessorDuplicateEOSAndDropAccounting(t *testing.T) {
	output := make(chan EdgeOutputRecord, 3)
	processor := &EdgeProcessor{
		edgeID: "edge-0", output: output, stats: &EdgeStats{},
		aggregator: &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute},
	}
	for _, record := range []EdgeIngressRecord{
		{Kind: EdgeIngressTelemetry, Payload: []byte("not-json")},
		{Kind: EdgeIngressTelemetry, Payload: []byte(`{}`)},
		{Kind: EdgeIngressTelemetry, Payload: processorTestPayload(t, 6)},
		{Kind: EdgeIngressTelemetry, Payload: processorTestPayload(t, 1)},
		{Kind: EdgeIngressEndOfReplay},
		{Kind: EdgeIngressEndOfReplay},
		{Kind: EdgeIngressTelemetry, Payload: processorTestPayload(t, 7)},
	} {
		if err := processor.Process(record); err != nil {
			t.Fatal(err)
		}
	}
	if len(output) != 2 {
		t.Fatalf("aggregate + EOS attesi, ricevuti %d record", len(output))
	}
	snapshot := processor.stats.Snapshot()
	if snapshot.Processed != 1 || snapshot.InvalidTelemetry != 2 || snapshot.OutOfOrderDropped != 1 ||
		snapshot.PostEOSDropped != 1 || snapshot.AggregatesEmitted != 0 || snapshot.EndOfReplayProcessed != 0 {
		t.Fatalf("statistiche: %#v", snapshot)
	}
}

func TestProcessorEmptyReplay(t *testing.T) {
	ingress := newEdgeIngress(1)
	ingress.RegisterEndOfReplay()
	ingress.Close()
	output := make(chan EdgeOutputRecord, 1)
	processor := &EdgeProcessor{
		edgeID: "edge-0", ingress: ingress, output: output, stats: &EdgeStats{},
		aggregator: &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute},
	}
	started := time.Now().UTC()
	if err := processor.Run(); err != nil {
		t.Fatal(err)
	}
	record := <-output
	if record.Kind != EdgeOutputEndOfReplay || !record.EndOfReplay.LastEventTime.Equal(record.EndOfReplay.EmittedAt) ||
		record.EndOfReplay.EmittedAt.Before(started) || record.EndOfReplay.EmittedAt.After(time.Now().UTC()) {
		t.Fatalf("EOS senza telemetry: %#v", record)
	}
	if _, ok := <-output; ok {
		t.Fatal("output non chiuso")
	}
}

func TestProcessorStopsWhenEgressHasStopped(t *testing.T) {
	for _, stage := range []string{"transition", "eos", "shutdown"} {
		t.Run(stage, func(t *testing.T) {
			ingress := newEdgeIngress(2)
			ingress.TryEnqueueTelemetry(processorTestPayload(t, 1))
			if stage == "transition" {
				ingress.TryEnqueueTelemetry(processorTestPayload(t, 6))
			} else if stage == "eos" {
				ingress.RegisterEndOfReplay()
			}
			ingress.Close()
			output := make(chan EdgeOutputRecord)
			stopped := make(chan struct{})
			processor := &EdgeProcessor{
				edgeID: "edge-0", ingress: ingress, output: output, egressStopped: stopped, stats: &EdgeStats{},
				aggregator: &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute},
			}
			done := make(chan error, 1)
			go func() { done <- processor.Run() }()
			close(stopped)
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "Kafka egress terminato") {
					t.Fatalf("errore: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("processor bloccato dopo arresto egress")
			}
			if record, ok := <-output; ok {
				t.Fatalf("record dopo arresto egress: %#v", record)
			}
			if processor.stats.Snapshot().Processed != 1 {
				t.Fatal("evento con emissione fallita contato come processato")
			}
		})
	}
}

func TestRunEdgeCleansUpStartupFailures(t *testing.T) {
	t.Setenv("EDGE_ID", "edge-0")
	t.Setenv("MQTT_BROKER", "invalid://127.0.0.1:1883")
	t.Setenv("KAFKA_BROKER", "127.0.0.1:9092")
	t.Setenv("KAFKA_TOPIC", "edge-aggregates")
	t.Setenv("WINDOW_SIZE", "5m")
	t.Setenv("EDGE_INGRESS_QUEUE_CAPACITY", "1")

	listener, err := net.Listen("tcp", readinessAddress)
	if err != nil {
		t.Skipf("porta readiness occupata: %v", err)
	}
	defer listener.Close()
	if err := runEdge(); err == nil || !strings.Contains(err.Error(), "avvio readiness server") {
		t.Fatalf("errore readiness atteso: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	// Il protocollo invalido fa fallire Connect senza richiedere un broker esterno.
	// runEdge deve attendere la pipeline e liberare readiness anche su questo percorso.
	if err := runEdge(); err == nil || !strings.Contains(err.Error(), "connessione MQTT fallita") {
		t.Fatalf("errore connessione MQTT atteso: %v", err)
	}
	reopened, err := net.Listen("tcp", readinessAddress)
	if err != nil {
		t.Fatalf("readiness non chiuso dopo errore MQTT: %v", err)
	}
	reopened.Close()
}

func processorTestPayload(t *testing.T, minute int) []byte {
	t.Helper()
	payload, err := json.Marshal(model.SensorEvent{
		EventID: "event", SensorID: "sensor-1",
		EventTime:    time.Date(2026, 1, 1, 10, minute, 0, 0, time.UTC),
		Measurements: map[string]model.NullableFloat64{"temperature": {Value: 20, Valid: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
