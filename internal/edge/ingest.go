package edge

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"continuum/internal/domain"
	"continuum/internal/topology"
)

type IngestService struct {
	edgeID      string
	macroareaID string
	topology    *topology.Index
	accepted    atomic.Uint64
	onAccepted  func(domain.SensorEvent, uint64)
}

func NewIngestService(
	edgeID string,
	macroareaID string,
	index *topology.Index,
	onAccepted func(domain.SensorEvent, uint64),
) (*IngestService, error) {
	if strings.TrimSpace(edgeID) == "" || strings.TrimSpace(macroareaID) == "" {
		return nil, fmt.Errorf("edge and macroarea IDs are required")
	}
	if index == nil {
		return nil, fmt.Errorf("topology index is required")
	}
	return &IngestService{
		edgeID: edgeID, macroareaID: macroareaID,
		topology: index, onAccepted: onAccepted,
	}, nil
}

func (s *IngestService) HandleSensorEvent(_ context.Context, event domain.SensorEvent) error {
	if event.SchemaVersion != domain.SchemaVersion {
		return fmt.Errorf("unsupported sensor event schema version %d", event.SchemaVersion)
	}
	if strings.TrimSpace(event.EventID) == "" || strings.TrimSpace(event.SensorID) == "" {
		return fmt.Errorf("event and sensor IDs are required")
	}
	if event.EventTime.IsZero() || event.EmittedAt.IsZero() {
		return fmt.Errorf("observed_at and emitted_at are required")
	}
	assignment, found := s.topology.Assignment(event.SensorID)
	if !found {
		return fmt.Errorf("sensor %q is not present in the topology", event.SensorID)
	}
	if assignment.EdgeID != s.edgeID {
		return fmt.Errorf(
			"sensor %q belongs to edge %q, not %q",
			event.SensorID,
			assignment.EdgeID,
			s.edgeID,
		)
	}
	if len(event.Measurements) == 0 {
		return fmt.Errorf("sensor event %q contains no measurements", event.EventID)
	}

	event.EdgeID = s.edgeID
	event.MacroareaID = s.macroareaID
	count := s.accepted.Add(1)
	if s.onAccepted != nil {
		s.onAccepted(event, count)
	}
	return nil
}

func (s *IngestService) AcceptedCount() uint64 {
	return s.accepted.Load()
}
