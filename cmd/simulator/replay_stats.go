package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type ReplayStats struct {
	OfferedEvents           int
	TelemetryEnqueued       int
	TelemetryLocallyDropped int

	QueueCapacity int

	MQTTPublishAttempts uint64
	MQTTPublishErrors   uint64

	SchedulingLagTotal time.Duration
	SchedulingLagMax   time.Duration

	FirstOfferedAt time.Time
	LastOfferedAt  time.Time
	CompletedAt    time.Time

	EOSSuccesses int
	EOSFailures  int
}

func (stats ReplayStats) AverageSchedulingLag() time.Duration {
	if stats.OfferedEvents == 0 {
		return 0
	}
	return stats.SchedulingLagTotal / time.Duration(stats.OfferedEvents)
}

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

func (stats ReplayStats) Throughput() float64 {
	duration := stats.OfferDuration()
	if stats.OfferedEvents <= 1 || duration <= 0 {
		return 0
	}

	// N eventi delimitano N-1 intervalli.
	return float64(stats.OfferedEvents-1) / duration.Seconds()
}

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

func recordReplayEgressStats(stats *ReplayStats, egressStats ReplayEgressStats) {
	stats.MQTTPublishAttempts = egressStats.PublishAttempts
	stats.MQTTPublishErrors = egressStats.PublishErrors
	stats.EOSSuccesses = egressStats.EOSSuccesses
	stats.EOSFailures = egressStats.EOSFailures
}

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

type simulatorStatsJSON struct {
	EdgeID                  string  `json:"edge_id"`
	Status                  string  `json:"status"`
	OfferedEvents           int     `json:"offered_events"`
	TelemetryEnqueued       int     `json:"telemetry_enqueued"`
	TelemetryLocallyDropped int     `json:"telemetry_locally_dropped"`
	QueueCapacity           int     `json:"queue_capacity"`
	MQTTPublishAttempts     uint64  `json:"mqtt_publish_attempts"`
	MQTTPublishErrors       uint64  `json:"mqtt_publish_errors"`
	AvgSchedulingLagMs      float64 `json:"avg_scheduling_lag_ms"`
	MaxSchedulingLagMs      float64 `json:"max_scheduling_lag_ms"`
	OfferDurationS          float64 `json:"offer_duration_s"`
	DrainDurationS          float64 `json:"drain_duration_s"`
	ThroughputEPS           float64 `json:"throughput_eps"`
	EOSSuccesses            int     `json:"eos_successes"`
	EOSFailures             int     `json:"eos_failures"`
}

func printSimulatorStatsJSON(siteID string, stats ReplayStats, replayErr error) {
	status := "completato"
	if replayErr != nil {
		status = "fallito"
	}
	payload := simulatorStatsJSON{
		EdgeID:                  siteID,
		Status:                  status,
		OfferedEvents:           stats.OfferedEvents,
		TelemetryEnqueued:       stats.TelemetryEnqueued,
		TelemetryLocallyDropped: stats.TelemetryLocallyDropped,
		QueueCapacity:           stats.QueueCapacity,
		MQTTPublishAttempts:     stats.MQTTPublishAttempts,
		MQTTPublishErrors:       stats.MQTTPublishErrors,
		AvgSchedulingLagMs:      durationMs(stats.AverageSchedulingLag()),
		MaxSchedulingLagMs:      durationMs(stats.SchedulingLagMax),
		OfferDurationS:          stats.OfferDuration().Seconds(),
		DrainDurationS:          stats.DrainDuration().Seconds(),
		ThroughputEPS:           stats.Throughput(),
		EOSSuccesses:            stats.EOSSuccesses,
		EOSFailures:             stats.EOSFailures,
	}
	fmt.Print("SIMULATOR_STATS ")
	json.NewEncoder(os.Stdout).Encode(payload)
}

func durationMs(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}
