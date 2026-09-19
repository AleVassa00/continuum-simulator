package main

import (
	"fmt"
	"sync"
	"time"

	"continuum/internal/model"
	"continuum/internal/mqtttopic"
)

type ReplayEgressKind byte

const (
	ReplayEgressTelemetry ReplayEgressKind = iota
	ReplayEgressEndOfReplay
)

type ReplayEgressRecord struct {
	Kind      ReplayEgressKind
	Telemetry model.SensorEvent
}

type TelemetryPublisher func(topic string, event model.SensorEvent) error

// Telemetria ed EOS condividono una coda ordinata.
type ReplayEgress struct {
	queue chan ReplayEgressRecord

	siteID             string
	publishTelemetry   TelemetryPublisher
	publishEndOfReplay EndOfReplayPublisher

	done      chan struct{}
	closeOnce sync.Once

	publishAttempts    uint64
	publishErrors      uint64
	eosSuccesses       int
	eosFailures        int
	telemetryDrainedAt time.Time
	eosErr             error
}

type ReplayEgressStats struct {
	PublishAttempts    uint64
	PublishErrors      uint64
	EOSSuccesses       int
	EOSFailures        int
	TelemetryDrainedAt time.Time
}

func newReplayEgress(siteID string, capacity int, publishTelemetry TelemetryPublisher, publishEndOfReplay EndOfReplayPublisher) *ReplayEgress {

	egress := &ReplayEgress{
		queue:              make(chan ReplayEgressRecord, capacity),
		siteID:             siteID,
		publishTelemetry:   publishTelemetry,
		publishEndOfReplay: publishEndOfReplay,
		done:               make(chan struct{}),
	}

	go egress.run()

	return egress
}

func (egress *ReplayEgress) TryEnqueueTelemetry(event model.SensorEvent) bool {
	select {
	case egress.queue <- ReplayEgressRecord{
		Kind:      ReplayEgressTelemetry,
		Telemetry: event,
	}:
		return true
	default:
		return false
	}
}

func (egress *ReplayEgress) EnqueueEndOfReplay() {
	egress.queue <- ReplayEgressRecord{
		Kind: ReplayEgressEndOfReplay,
	}
}

func (egress *ReplayEgress) CloseAndWait() (ReplayEgressStats, error) {
	egress.closeOnce.Do(func() {
		close(egress.queue)
	})
	<-egress.done

	return ReplayEgressStats{
		PublishAttempts:    egress.publishAttempts,
		PublishErrors:      egress.publishErrors,
		EOSSuccesses:       egress.eosSuccesses,
		EOSFailures:        egress.eosFailures,
		TelemetryDrainedAt: egress.telemetryDrainedAt,
	}, egress.eosErr
}

func (egress *ReplayEgress) run() {
	defer close(egress.done)

	for record := range egress.queue {
		switch record.Kind {
		case ReplayEgressTelemetry:
			egress.publishAttempts++

			event := record.Telemetry
			// EventTime resta quello del dataset.
			event.EmittedAt = time.Now().UTC()
			if err := egress.publishTelemetry(mqtttopic.Telemetry(event.SensorID), event); err != nil {
				egress.publishErrors++
			}

		case ReplayEgressEndOfReplay:
			// L'EOS viene pubblicato dopo il drain della telemetria.
			if egress.telemetryDrainedAt.IsZero() {
				egress.telemetryDrainedAt = time.Now()
			}

			endTopic := mqtttopic.ReplayEnd(egress.siteID)
			if err := egress.publishEndOfReplay(endTopic); err != nil {
				egress.eosFailures++
				egress.eosErr = fmt.Errorf("pubblicazione EndOfReplay MQTT edge=%s fallita: %w", egress.siteID, err)
			} else {
				egress.eosSuccesses++
			}
		}
	}

	// Gestisce anche la chiusura anticipata senza EOS.
	if egress.telemetryDrainedAt.IsZero() {
		egress.telemetryDrainedAt = time.Now()
	}
}
