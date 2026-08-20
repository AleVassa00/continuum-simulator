package mqtttransport

import (
	"bytes"
	"testing"
	"time"

	"continuum/internal/domain"
)

func TestSensorEventPayloadExcludesRoutingMetadata(t *testing.T) {
	now := time.Date(2026, 8, 20, 15, 0, 0, 0, time.UTC)
	event := domain.SensorEvent{
		SchemaVersion: domain.SchemaVersion,
		EventID:       "event-1", SensorID: "87575", LocationID: "78536",
		EdgeID: "edge-m0-0", MacroareaID: "0", Sequence: 1,
		EventTime: now.Add(-time.Hour), EmittedAt: now,
		Measurements: map[string]string{"temperature_c": "5.33"},
	}
	payload, err := encodeSensorEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("edge-m0-0")) || bytes.Contains(payload, []byte("macroarea")) {
		t.Fatalf("payload leaks routing metadata: %s", payload)
	}
	decoded, err := decodeSensorEvent(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SensorID != event.SensorID || decoded.Measurements["temperature_c"] != "5.33" {
		t.Fatalf("decoded event = %+v", decoded)
	}
}
