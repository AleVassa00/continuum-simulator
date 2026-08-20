package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"continuum/internal/domain"
	"continuum/internal/ports"
)

// Record is one raw, unrouted row read from a replay trace.
type Record struct {
	SensorID     string
	LocationID   string
	EventTime    time.Time
	Measurements map[string]string
}

type TraceReader interface {
	Next(context.Context) (Record, error)
}

type RouteResolver interface {
	Resolve(sensorID string) (edgeID, macroareaID string, found bool)
}

type Pacer interface {
	Wait(context.Context, time.Time) error
}

type Service struct {
	reader    TraceReader
	routes    RouteResolver
	pacer     Pacer
	publisher ports.SensorEventPublisher
}

func NewService(reader TraceReader, routes RouteResolver, pacer Pacer, publisher ports.SensorEventPublisher) (*Service, error) {
	if reader == nil || routes == nil || pacer == nil || publisher == nil {
		return nil, fmt.Errorf("replay service dependencies must not be nil")
	}
	return &Service{reader: reader, routes: routes, pacer: pacer, publisher: publisher}, nil
}

// Run replays the trace once. EOF is a successful, deterministic completion.
func (s *Service) Run(ctx context.Context) error {
	return s.RunN(ctx, 0)
}

// RunN replays at most maxEvents records. A value of zero means the whole trace.
func (s *Service) RunN(ctx context.Context, maxEvents uint64) error {
	sequences := make(map[string]uint64)
	var published uint64
	for {
		if maxEvents > 0 && published >= maxEvents {
			return nil
		}
		record, err := s.reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read replay record: %w", err)
		}

		edgeID, macroareaID, found := s.routes.Resolve(record.SensorID)
		if !found {
			return fmt.Errorf("sensor %q is not present in the topology", record.SensorID)
		}
		sequences[record.SensorID]++
		sequence := sequences[record.SensorID]
		if err := s.pacer.Wait(ctx, record.EventTime); err != nil {
			return fmt.Errorf("pace replay: %w", err)
		}

		event := domain.SensorEvent{
			SchemaVersion: domain.SchemaVersion,
			EventID:       eventID(record.SensorID, record.EventTime, sequence),
			SensorID:      record.SensorID,
			LocationID:    record.LocationID,
			EdgeID:        edgeID,
			MacroareaID:   macroareaID,
			Sequence:      sequence,
			EventTime:     record.EventTime,
			EmittedAt:     time.Now().UTC(),
			Measurements:  record.Measurements,
		}
		if err := s.publisher.PublishSensorEvent(ctx, event); err != nil {
			return fmt.Errorf("publish sensor event %q: %w", event.EventID, err)
		}
		published++
	}
}

func eventID(sensorID string, eventTime time.Time, sequence uint64) string {
	return fmt.Sprintf("v%d:%s:%d:%d", domain.SchemaVersion, sensorID, eventTime.UnixNano(), sequence)
}
