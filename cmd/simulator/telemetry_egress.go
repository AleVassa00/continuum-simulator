package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"continuum/internal/model"
	"continuum/internal/mqtttopic"
)

// struct che gestisce la queue
type TelemetryEgress struct {
	queue chan model.SensorEvent

	publish   TelemetryPublisher
	done      chan struct{}
	closeOnce sync.Once

	publishAttempts atomic.Uint64
	publishErrors   atomic.Uint64
}

type TelemetryEgressStats struct {
	QueueCapacity     int
	CurrentQueueDepth int
	PublishAttempts   uint64
	PublishErrors     uint64
}

type TelemetryPublisher func(topic string, event model.SensorEvent) error

// costruisce un gestore della coda e lancia la goroutine che si occupa di pubblicare su MQTT
func newTelemetryEgress(capacity int, publish TelemetryPublisher) (*TelemetryEgress, error) {

	if capacity <= 0 {
		return nil, fmt.Errorf("TELEMETRY_QUEUE_CAPACITY deve essere maggiore di zero")
	}
	if publish == nil {
		return nil, fmt.Errorf("publisher MQTT telemetry non configurato")
	}
	egress := &TelemetryEgress{
		queue:   make(chan model.SensorEvent, capacity),
		publish: publish,
		done:    make(chan struct{}),
	}

	go egress.run()

	return egress, nil
}

// Se c'è spazio nella coda, inserisce il SensorEvent senza bloccare il replay.
func (egress *TelemetryEgress) TryEnqueue(event model.SensorEvent) bool {
	select {
	case egress.queue <- event:
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

// metodo di una TelemetryEgress che si preoccupa di consumare gli elementi nella coda e pu
func (egress *TelemetryEgress) run() {
	defer close(egress.done)

	for event := range egress.queue {
		egress.publishAttempts.Add(1)

		event.EmittedAt = time.Now().UTC()
		if err := egress.publish(mqtttopic.Telemetry(event.SensorID), event); err != nil {
			egress.publishErrors.Add(1)
		}
	}
}
