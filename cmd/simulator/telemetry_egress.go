package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"continuum/internal/model"
	"continuum/internal/mqtttopic"
)

// TelemetryEgress disaccoppia il pacing del replay dalla pubblicazione MQTT tramite una coda locale consumata da una goroutine dedicata
type TelemetryEgress struct {
	queue chan model.SensorEvent

	publish   TelemetryPublisher
	done      chan struct{}
	closeOnce sync.Once

	publishAttempts atomic.Uint64
	publishErrors   atomic.Uint64
}

type TelemetryEgressStats struct {
	CurrentQueueDepth int
	PublishAttempts   uint64
	PublishErrors     uint64
}

type TelemetryPublisher func(topic string, event model.SensorEvent) error

// newTelemetryEgress crea la coda locale della telemetry e avvia la goroutine responsabile delle publish MQTT
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

// TryEnqueue prova a inserire un evento nella coda senza bloccare il replay. Restituisce false se la coda è piena
func (egress *TelemetryEgress) TryEnqueue(event model.SensorEvent) bool {
	select {
	case egress.queue <- event:
		return true
	default:
		return false
	}

} // CloseAndWait chiude la coda e attende che tutti gli eventi già accettati siano stati processati dalla goroutine di pubblicazione
func (egress *TelemetryEgress) CloseAndWait() TelemetryEgressStats {
	egress.closeOnce.Do(
		func() {
			close(egress.queue)
		})
	<-egress.done

	return egress.Stats()
}

// Stats restituisce una fotografia delle statistiche correnti della telemetry egress
func (egress *TelemetryEgress) Stats() TelemetryEgressStats {
	return TelemetryEgressStats{
		CurrentQueueDepth: len(egress.queue),
		PublishAttempts:   egress.publishAttempts.Load(),
		PublishErrors:     egress.publishErrors.Load(),
	}
}

// run consuma gli eventi dalla coda e ne esegue la pubblicazione MQTT
func (egress *TelemetryEgress) run() {
	defer close(egress.done)

	for event := range egress.queue {
		egress.publishAttempts.Add(1)

		// EmittedAt rappresenta l'istante reale in cui l'evento viene consegnato al publisher MQTT, distinto dall'EventTime del dataset
		event.EmittedAt = time.Now().UTC()
		if err := egress.publish(mqtttopic.Telemetry(event.SensorID), event); err != nil {
			egress.publishErrors.Add(1)
		}
	}
}
