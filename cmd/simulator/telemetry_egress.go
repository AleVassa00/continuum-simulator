package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"continuum/internal/model"
)

type TelemetryPublisher func(
	topic string,
	event model.SensorEvent,
) error

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
	QueueCapacity         int
	CurrentQueueDepth     int
	MaxQueueDepthObserved int
	PublishAttempts       uint64
	PublishErrors         uint64
}

type TelemetryEgress struct {
	mu sync.Mutex

	queue    chan queuedTelemetry
	size     int
	maxDepth int
	closed   bool

	publish   TelemetryPublisher
	now       func() time.Time
	done      chan struct{}
	closeOnce sync.Once

	publishAttempts atomic.Uint64
	publishErrors   atomic.Uint64
}

func newTelemetryEgress(
	capacity int,
	publish TelemetryPublisher,
	now func() time.Time,
) (*TelemetryEgress, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf(
			"TELEMETRY_QUEUE_CAPACITY deve essere maggiore di zero",
		)
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
	egress.mu.Lock()
	defer egress.mu.Unlock()

	if egress.closed || egress.size == cap(egress.queue) {
		return false
	}

	telemetry := queuedTelemetry{
		measurement: measurement,
		sequence:    sequence,
	}

	select {
	case egress.queue <- telemetry:
		egress.size++
		egress.maxDepth = max(egress.maxDepth, egress.size)
		return true
	default:
		return false
	}
}

func (
	egress *TelemetryEgress,
) CloseAndWait() TelemetryEgressStats {
	egress.closeOnce.Do(func() {
		egress.mu.Lock()
		egress.closed = true
		close(egress.queue)
		egress.mu.Unlock()
	})
	<-egress.done

	return egress.Stats()
}

func (egress *TelemetryEgress) Stats() TelemetryEgressStats {
	egress.mu.Lock()
	defer egress.mu.Unlock()

	return TelemetryEgressStats{
		QueueCapacity:         cap(egress.queue),
		CurrentQueueDepth:     egress.size,
		MaxQueueDepthObserved: egress.maxDepth,
		PublishAttempts:       egress.publishAttempts.Load(),
		PublishErrors:         egress.publishErrors.Load(),
	}
}

func (
	egress *TelemetryEgress,
) run() {
	defer close(egress.done)

	for telemetry := range egress.queue {
		egress.mu.Lock()
		egress.size--
		egress.mu.Unlock()

		egress.publishAttempts.Add(1)

		event := buildSensorEvent(
			telemetry.measurement,
			telemetry.sequence,
			egress.now().UTC(),
		)
		if err := egress.publish(
			telemetryTopic(telemetry.measurement.SensorID),
			event,
		); err != nil {
			egress.publishErrors.Add(1)
		}
	}
}
