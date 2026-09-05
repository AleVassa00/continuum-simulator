package main

import (
	"fmt"
	"sync"
	"time"

	"continuum/internal/model"
	"continuum/internal/mqtttopic"
)

// ReplayEgressKind discrimina la tipologia di record instradato attraverso l'egress del replay.
type ReplayEgressKind byte

const (
	ReplayEgressTelemetry ReplayEgressKind = iota
	ReplayEgressEndOfReplay
)

// ReplayEgressRecord rappresenta un elemento tipizzato dell'unico stream logico del replay
type ReplayEgressRecord struct {
	Kind      ReplayEgressKind
	Telemetry model.SensorEvent
}

type TelemetryPublisher func(topic string, event model.SensorEvent) error

// ReplayEgress disaccoppia la generazione/pacing del replay dalla trasmissione MQTT
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

// ReplayEgressStats aggrega le metriche operative raccolte durante il ciclo di vita della egress.
type ReplayEgressStats struct {
	CurrentQueueDepth  int
	PublishAttempts    uint64
	PublishErrors      uint64
	EOSSuccesses       int
	EOSFailures        int
	TelemetryDrainedAt time.Time
}

// newReplayEgress alloca la coda locale con la capacità configurata e avvia la goroutine di consumo.
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

// TryEnqueueTelemetry offre un evento di telemetria alla coda in modalità non bloccante
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

// EnqueueEndOfReplay accoda il marker di fine replay nello stesso canale della telemetria
func (egress *ReplayEgress) EnqueueEndOfReplay() {
	egress.queue <- ReplayEgressRecord{
		Kind: ReplayEgressEndOfReplay,
	}
}

// CloseAndWait chiude la coda e attende la terminazione della goroutine consumatrice
func (egress *ReplayEgress) CloseAndWait() (ReplayEgressStats, error) {
	egress.closeOnce.Do(func() {
		close(egress.queue)
	})
	<-egress.done

	return ReplayEgressStats{
		CurrentQueueDepth:  len(egress.queue),
		PublishAttempts:    egress.publishAttempts,
		PublishErrors:      egress.publishErrors,
		EOSSuccesses:       egress.eosSuccesses,
		EOSFailures:        egress.eosFailures,
		TelemetryDrainedAt: egress.telemetryDrainedAt,
	}, egress.eosErr
}

// run consuma sequenzialmente i record dalla coda ed esegue le relative pubblicazioni MQTT
func (egress *ReplayEgress) run() {
	defer close(egress.done)

	for record := range egress.queue {
		switch record.Kind {
		case ReplayEgressTelemetry:
			egress.publishAttempts++

			event := record.Telemetry
			// EmittedAt riflette il momento esatto di consegna al client MQTT, distinto dall'EventTime originale
			event.EmittedAt = time.Now().UTC()
			if err := egress.publishTelemetry(mqtttopic.Telemetry(event.SensorID), event); err != nil {
				egress.publishErrors++
			}

		case ReplayEgressEndOfReplay:
			// Tutte le telemetrie precedenti sono state elaborate e invocate
			// Fissiamo TelemetryDrainedAt prima della publish bloccante con PUBACK dell'EOS, preservando il significato di CompletedAt e DrainDuration
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

	// Se il replay non ha generato alcun EOS registriamo TelemetryDrainedAt al termine del drain delle sole telemetrie
	if egress.telemetryDrainedAt.IsZero() {
		egress.telemetryDrainedAt = time.Now()
	}
}
