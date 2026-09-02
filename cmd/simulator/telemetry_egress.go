package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"continuum/internal/model"
	"continuum/internal/mqtttopic"
)

type TelemetryPublisher func(topic string, event model.SensorEvent) error

type TelemetryQueue interface {
	TryEnqueue(SensorMeasurement, uint64) bool
	CloseAndWait() TelemetryEgressStats
}

type TelemetryEgressFactory func(
	capacity int,
	publish TelemetryPublisher,
	now func() time.Time,
) (TelemetryQueue, error)

type queuedTelemetry struct {
	measurement SensorMeasurement
	sequence    uint64
}

type TelemetryEgressStats struct {
	QueueCapacity     int
	CurrentQueueDepth int
	PublishAttempts   uint64
	PublishErrors     uint64
}

// TelemetryEgress ha un solo producer. CloseAndWait deve essere chiamata
// soltanto dopo l'ultima TryEnqueue; dopo la chiusura non sono ammesse enqueue.
type TelemetryEgress struct {
	queue chan queuedTelemetry

	publish   TelemetryPublisher
	now       func() time.Time
	done      chan struct{}
	closeOnce sync.Once

	publishAttempts atomic.Uint64
	publishErrors   atomic.Uint64
}

func newTelemetryEgress(capacity int, publish TelemetryPublisher, now func() time.Time) (*TelemetryEgress, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("TELEMETRY_QUEUE_CAPACITY deve essere maggiore di zero")
	}
	if publish == nil {
		return nil, fmt.Errorf("publisher MQTT telemetry non configurato")
	}
	if now == nil {
		return nil, fmt.Errorf("clock telemetry sender non configurato")
	}

	egress := &TelemetryEgress{
		queue:   make(chan queuedTelemetry, capacity),
		publish: publish,
		now:     now,
		done:    make(chan struct{}),
	}

	go egress.run()

	return egress, nil
}

func (
	egress *TelemetryEgress,
) TryEnqueue(
	measurement SensorMeasurement,
	sequence uint64,
) bool {
	telemetry := queuedTelemetry{
		measurement: measurement,
		sequence:    sequence,
	}

	select {
	case egress.queue <- telemetry:
		return true
	default:
		return false
	}
}

func (
	egress *TelemetryEgress,
) CloseAndWait() TelemetryEgressStats {
	egress.closeOnce.Do(func() {
		close(egress.queue)
	})
	<-egress.done

	return egress.Stats()
}

func (egress *TelemetryEgress) Stats() TelemetryEgressStats {
	return TelemetryEgressStats{
		QueueCapacity:     cap(egress.queue),
		CurrentQueueDepth: len(egress.queue),
		PublishAttempts:   egress.publishAttempts.Load(),
		PublishErrors:     egress.publishErrors.Load(),
	}
}

func (egress *TelemetryEgress) run() {
	defer close(egress.done)

	for telemetry := range egress.queue {
		egress.publishAttempts.Add(1)

		event := buildSensorEvent(telemetry.measurement, telemetry.sequence, egress.now().UTC())
		if err := egress.publish(mqtttopic.Telemetry(telemetry.measurement.SensorID), event); err != nil {
			egress.publishErrors.Add(1)
		}
	}
}
