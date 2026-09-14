package main

import (
	"encoding/json"
	"testing"
	"time"

	"continuum/internal/model"
)

func TestEdgeEmbedsProgressAndEmitsTerminalMarkerInOrder(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ingress := newEdgeIngress(2)
	for i, eventTime := range []time.Time{start, start.Add(11 * time.Minute)} {
		payload, err := json.Marshal(model.SensorEvent{EventID: string(rune('a' + i)), SensorID: "sensor-0", EventTime: eventTime})
		if err != nil {
			t.Fatal(err)
		}
		if result := ingress.TryEnqueueTelemetry(payload); result != TelemetryEnqueued {
			t.Fatalf("enqueue result=%d", result)
		}
	}
	ingress.RegisterEndOfReplay()

	output := make(chan EdgeOutputRecord, 4)
	stats := &EdgeStats{}
	if err := runEdgeLoop(ingress, &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute}, output, make(chan struct{}), stats); err != nil {
		t.Fatal(err)
	}

	var records []EdgeOutputRecord
	for record := range output {
		records = append(records, record)
	}
	wantKinds := []EdgeOutputKind{EdgeOutputAggregate, EdgeOutputAggregate, EdgeOutputEndOfInput}
	if len(records) != len(wantKinds) {
		t.Fatalf("output records=%d want=%d", len(records), len(wantKinds))
	}
	for i, want := range wantKinds {
		if records[i].Kind != want {
			t.Fatalf("output[%d].kind=%d want=%d", i, records[i].Kind, want)
		}
	}
	if !records[0].Aggregate.CompleteThrough.Equal(start.Add(10 * time.Minute)) {
		t.Fatalf("complete_through=%s", records[0].Aggregate.CompleteThrough)
	}
	if stats.endOfReplayProcessed.Load() != 1 {
		t.Fatal("Simulator EOS not processed")
	}
}
