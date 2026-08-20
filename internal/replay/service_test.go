package replay

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"continuum/internal/domain"
)

type sliceReader struct {
	records []Record
	index   int
}

func (r *sliceReader) Next(context.Context) (Record, error) {
	if r.index == len(r.records) {
		return Record{}, io.EOF
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

type staticRoutes map[string]Route

type Route struct {
	edgeID      string
	macroareaID string
}

func (r staticRoutes) Resolve(sensorID string) (string, string, bool) {
	route, found := r[sensorID]
	return route.edgeID, route.macroareaID, found
}

type noopPacer struct{}

func (noopPacer) Wait(context.Context, time.Time) error { return nil }

type collectingPublisher struct {
	events []domain.SensorEvent
}

func (p *collectingPublisher) PublishSensorEvent(_ context.Context, event domain.SensorEvent) error {
	p.events = append(p.events, event)
	return nil
}

func TestServiceRoutesAndPublishesRecordsInOrder(t *testing.T) {
	eventTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	reader := &sliceReader{records: []Record{{
		SensorID: "42", LocationID: "7", EventTime: eventTime,
		Measurements: map[string]string{"temperature_c": "abc"},
	}}}
	publisher := &collectingPublisher{}
	service, err := NewService(
		reader,
		staticRoutes{"42": {edgeID: "edge-m0-0", macroareaID: "0"}},
		noopPacer{},
		publisher,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published events = %d", len(publisher.events))
	}
	event := publisher.events[0]
	if event.Sequence != 1 || event.EdgeID != "edge-m0-0" || event.MacroareaID != "0" {
		t.Fatalf("unexpected event: %+v", event)
	}
	if event.Measurements["temperature_c"] != "abc" {
		t.Fatalf("raw measurement was changed: %+v", event.Measurements)
	}
	if event.EventID != eventID("42", eventTime, 1) {
		t.Fatalf("event ID = %q", event.EventID)
	}
}

func TestServiceRejectsSensorWithoutRoute(t *testing.T) {
	reader := &sliceReader{records: []Record{{SensorID: "missing", EventTime: time.Now()}}}
	service, err := NewService(reader, staticRoutes{}, noopPacer{}, &collectingPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	err = service.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not present in the topology") {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestServiceUsesSequencePerSensor(t *testing.T) {
	eventTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	reader := &sliceReader{records: []Record{
		{SensorID: "A", EventTime: eventTime},
		{SensorID: "B", EventTime: eventTime},
		{SensorID: "A", EventTime: eventTime},
	}}
	publisher := &collectingPublisher{}
	service, err := NewService(
		reader,
		staticRoutes{
			"A": {edgeID: "edge-0", macroareaID: "0"},
			"B": {edgeID: "edge-0", macroareaID: "0"},
		},
		noopPacer{},
		publisher,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 1, 2}
	for index, sequence := range want {
		if publisher.events[index].Sequence != sequence {
			t.Fatalf("event %d sequence = %d, want %d", index, publisher.events[index].Sequence, sequence)
		}
	}
}
