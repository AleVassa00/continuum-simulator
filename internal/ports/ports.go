package ports

import (
	"context"

	"continuum/internal/domain"
)

// These interfaces are the stable boundaries of the application core.
// MQTT, Kafka and PostgreSQL adapters implement transport and storage details.
type SensorEventPublisher interface {
	PublishSensorEvent(context.Context, domain.SensorEvent) error
}

type SensorEventHandler interface {
	HandleSensorEvent(context.Context, domain.SensorEvent) error
}

type EdgeWindowPublisher interface {
	PublishEdgeWindow(context.Context, domain.EdgeWindow) error
}

type EdgeWindowHandler interface {
	HandleEdgeWindow(context.Context, domain.EdgeWindow) error
}

type MacroareaWindowStore interface {
	UpsertMacroareaWindow(context.Context, domain.MacroareaWindow) error
}
