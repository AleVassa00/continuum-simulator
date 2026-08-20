package edge

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"continuum/internal/domain"
	"continuum/internal/topology"
)

func TestIngestServiceAcceptsSensorAssignedToEdge(t *testing.T) {
	topologyPath := filepath.Join(t.TempDir(), "topology.csv")
	if err := os.WriteFile(
		topologyPath,
		[]byte("sensor_id,macroarea_id,edge_id,lat,lon\n42,0,edge-0,41.9,12.5\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	index, err := topology.Load(topologyPath)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewIngestService("edge-0", "0", index, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.SensorEvent{
		SchemaVersion: domain.SchemaVersion,
		EventID:       "event-1", SensorID: "42",
		EventTime: time.Now().UTC(), EmittedAt: time.Now().UTC(),
		Measurements: map[string]string{"temperature": "20.0"},
	}
	if err := service.HandleSensorEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if service.AcceptedCount() != 1 {
		t.Fatalf("accepted = %d", service.AcceptedCount())
	}
}
