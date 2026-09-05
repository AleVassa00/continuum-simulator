package main

import (
	"fmt"
	"time"
)

type ReplayStats struct {
	OfferedEvents           int // Numero totale di eventi offerti dal replay alla telemetry egress
	TelemetryEnqueued       int // Numero di eventi accettati nella coda locale di telemetria
	TelemetryLocallyDropped int // Numero di eventi scartati localmente perché non accettati dalla coda

	QueueCapacity int // Capacità massima configurata della telemetry queue

	MQTTPublishAttempts uint64 // Numero totale di tentativi di publish MQTT QoS0 effettuati dalla egress
	MQTTPublishErrors   uint64 // Numero di errori rilevati durante i tentativi di publish MQTT QoS0

	SchedulingLagTotal time.Duration // Somma dei ritardi tra scheduled time e momento effettivo di offerta degli eventi
	SchedulingLagMax   time.Duration // Massimo scheduling lag osservato durante il replay

	FirstOfferedAt time.Time // Istante reale in cui è stato offerto il primo evento.
	LastOfferedAt  time.Time // Istante reale in cui è stato offerto l'ultimo evento.
	CompletedAt    time.Time // Istante reale in cui il replay ha completato anche il drain della egress.

	ReachedEOF    bool      // True se il replay ha raggiunto realmente la fine del file CSV.
	LastEventTime time.Time // Event time dell'ultimo evento offerto dal replay.

	EOSSuccesses int // Numero di EndOfReplay pubblicati con PUBACK ricevuto correttamente.
	EOSFailures  int // Numero di fallimenti durante publish o attesa del PUBACK dell'EndOfReplay.
}

// AverageSchedulingLag restituisce il ritardo medio osservato tra le deadline pianificate e l'effettiva offerta degli eventi
func (stats ReplayStats) AverageSchedulingLag() time.Duration {
	if stats.OfferedEvents == 0 {
		return 0
	}
	return stats.SchedulingLagTotal / time.Duration(stats.OfferedEvents)
}

// OfferDuration restituisce l'intervallo reale compreso tra la prima e l'ultima offerta del replay
func (stats ReplayStats) OfferDuration() time.Duration {
	if stats.OfferedEvents <= 1 || stats.FirstOfferedAt.IsZero() || stats.LastOfferedAt.IsZero() {
		return 0
	}

	duration := stats.LastOfferedAt.Sub(stats.FirstOfferedAt)
	if duration <= 0 {
		return 0
	}

	return duration
}

// DrainDuration misura il tempo necessario a completare la telemetry egress dopo l'offerta dell'ultimo evento
func (stats ReplayStats) DrainDuration() time.Duration {
	if stats.LastOfferedAt.IsZero() || stats.CompletedAt.IsZero() {
		return 0
	}

	duration := stats.CompletedAt.Sub(stats.LastOfferedAt)
	if duration <= 0 {
		return 0
	}

	return duration
}

// Throughput calcola il rate medio degli eventi offerti durante il replay
func (stats ReplayStats) Throughput() float64 {
	duration := stats.OfferDuration()
	if stats.OfferedEvents <= 1 || duration <= 0 {
		return 0
	}

	// Prima e ultima offerta delimitano OfferedEvents-1 intervalli
	return float64(stats.OfferedEvents-1) / duration.Seconds()
}

// RecordOffer registra una nuova offerta e aggiorna le metriche temporali e di scheduling del replay
func (stats *ReplayStats) RecordOffer(offeredAt time.Time, schedulingLag time.Duration) {
	if schedulingLag < 0 {
		schedulingLag = 0
	}

	if stats.OfferedEvents == 0 {
		stats.FirstOfferedAt = offeredAt
	}

	stats.OfferedEvents++
	stats.LastOfferedAt = offeredAt
	stats.SchedulingLagTotal += schedulingLag
	stats.SchedulingLagMax = max(stats.SchedulingLagMax, schedulingLag)
}

// recordReplayEgressStats trasferisce le statistiche finali della replay egress nelle statistiche complessive del replay
func recordReplayEgressStats(stats *ReplayStats, egressStats ReplayEgressStats) {
	stats.MQTTPublishAttempts = egressStats.PublishAttempts
	stats.MQTTPublishErrors = egressStats.PublishErrors
	stats.EOSSuccesses = egressStats.EOSSuccesses
	stats.EOSFailures = egressStats.EOSFailures
}

// printReplaySummary stampa le principali metriche raccolte durante l'esecuzione del replay
func printReplaySummary(siteID string, stats ReplayStats, replayErr error) {
	status := "completato"
	if replayErr != nil {
		status = "fallito"
	}

	fmt.Printf("\nReplay %s %s\n", siteID, status)
	fmt.Printf("Eventi offered/generated: %d\n", stats.OfferedEvents)
	fmt.Printf("Telemetry accettata in coda: %d\n", stats.TelemetryEnqueued)
	fmt.Printf("Telemetry scartata localmente: %d\n", stats.TelemetryLocallyDropped)
	fmt.Printf("Telemetry queue capacity: %d\n", stats.QueueCapacity)
	fmt.Printf("Tentativi publish MQTT QoS0: %d\n", stats.MQTTPublishAttempts)
	fmt.Printf("Errori publish MQTT QoS0: %d\n", stats.MQTTPublishErrors)
	fmt.Printf("Scheduling lag medio: %s\n", stats.AverageSchedulingLag())
	fmt.Printf("Scheduling lag massimo: %s\n", stats.SchedulingLagMax)
	fmt.Printf("Durata workload offerto: %s\n", stats.OfferDuration())
	fmt.Printf("Durata completamento dopo ultima offerta: %s\n", stats.DrainDuration())
	fmt.Printf("Throughput workload offerto: %.2f eventi/s\n", stats.Throughput())
	fmt.Printf("EOS successi: %d\n", stats.EOSSuccesses)
	fmt.Printf("EOS fallimenti: %d\n", stats.EOSFailures)
}
