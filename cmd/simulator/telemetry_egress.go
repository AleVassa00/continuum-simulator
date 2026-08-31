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

	queue    []queuedTelemetry
	head     int
	size     int
	maxDepth int
	closed   bool
	wake     chan struct{}

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
		queue:   make([]queuedTelemetry, capacity),
		wake:    make(chan struct{}, 1),
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

	if egress.closed || egress.size == len(egress.queue) {
		return false
	}

	tail := (egress.head + egress.size) % len(egress.queue)
	egress.queue[tail] = queuedTelemetry{
		measurement: measurement,
		sequence:    sequence,
	}
	egress.size++
	egress.maxDepth = max(egress.maxDepth, egress.size)
	egress.signal()

	return true
}

func (
	egress *TelemetryEgress,
) CloseAndWait() TelemetryEgressStats {
	egress.closeOnce.Do(func() {
		egress.mu.Lock()
		egress.closed = true
		egress.signal()
		egress.mu.Unlock()
	})
	<-egress.done

	return egress.Stats()
}

func (egress *TelemetryEgress) Stats() TelemetryEgressStats {
	egress.mu.Lock()
	defer egress.mu.Unlock()

	return TelemetryEgressStats{
		QueueCapacity:         len(egress.queue),
		CurrentQueueDepth:     egress.size,
		MaxQueueDepthObserved: egress.maxDepth,
		PublishAttempts:       egress.publishAttempts.Load(),
		PublishErrors:         egress.publishErrors.Load(),
	}
}

func (egress *TelemetryEgress) next() (queuedTelemetry, bool) {
	for {
		egress.mu.Lock()
		if egress.size > 0 {
			telemetry := egress.queue[egress.head]
			egress.queue[egress.head] = queuedTelemetry{}
			egress.head = (egress.head + 1) % len(egress.queue)
			egress.size--
			if egress.size == 0 {
				egress.head = 0
			}
			egress.mu.Unlock()
			return telemetry, true
		}
		if egress.closed {
			egress.mu.Unlock()
			return queuedTelemetry{}, false
		}
		egress.mu.Unlock()

		<-egress.wake
	}
}

func (egress *TelemetryEgress) signal() {
	select {
	case egress.wake <- struct{}{}:
	default:
	}
}

func (
	egress *TelemetryEgress,
) run() {
	defer close(egress.done)

	for {
		telemetry, ok := egress.next()
		if !ok {
			return
		}

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
