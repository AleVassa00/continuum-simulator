package main

import (
	"fmt"
	"sync/atomic"
)

type EdgeStats struct {
	telemetryReceived    atomic.Uint64
	ingressAccepted      atomic.Uint64
	ingressQueueDropped  atomic.Uint64
	invalidTelemetry     atomic.Uint64
	outOfOrderDropped    atomic.Uint64
	postEOSDropped       atomic.Uint64
	processed            atomic.Uint64
	aggregatesEmitted    atomic.Uint64
	endOfReplayProcessed atomic.Uint64
}

type EdgeStatsSnapshot struct {
	IngressQueueCapacity         int
	MaxIngressQueueDepthObserved int
	TelemetryReceived            uint64
	IngressAccepted              uint64
	IngressQueueDropped          uint64
	InvalidTelemetry             uint64
	OutOfOrderDropped            uint64
	PostEOSDropped               uint64
	Processed                    uint64
	AggregatesEmitted            uint64
	EndOfReplayProcessed         uint64
}

func (
	stats *EdgeStats,
) Snapshot() EdgeStatsSnapshot {
	return EdgeStatsSnapshot{
		TelemetryReceived:    stats.telemetryReceived.Load(),
		IngressAccepted:      stats.ingressAccepted.Load(),
		IngressQueueDropped:  stats.ingressQueueDropped.Load(),
		InvalidTelemetry:     stats.invalidTelemetry.Load(),
		OutOfOrderDropped:    stats.outOfOrderDropped.Load(),
		PostEOSDropped:       stats.postEOSDropped.Load(),
		Processed:            stats.processed.Load(),
		AggregatesEmitted:    stats.aggregatesEmitted.Load(),
		EndOfReplayProcessed: stats.endOfReplayProcessed.Load(),
	}
}

func (
	stats *EdgeStats,
) SnapshotWithQueue(ingress *EdgeIngress) EdgeStatsSnapshot {
	snapshot := stats.Snapshot()
	queueStats := ingress.Stats()
	snapshot.IngressQueueCapacity = queueStats.Capacity
	snapshot.MaxIngressQueueDepthObserved = queueStats.MaxDepthObserved

	return snapshot
}

func (stats EdgeStatsSnapshot) MaxIngressQueueUtilization() float64 {
	if stats.IngressQueueCapacity <= 0 {
		return 0
	}

	return float64(stats.MaxIngressQueueDepthObserved) /
		float64(stats.IngressQueueCapacity) * 100
}

func printEdgeSummary(
	edgeID string,
	stats EdgeStatsSnapshot,
) {
	fmt.Printf("\nEdge %s summary\n", edgeID)
	fmt.Printf("MQTT telemetry ricevuta: %d\n", stats.TelemetryReceived)
	fmt.Printf("Ingress queue capacity: %d\n", stats.IngressQueueCapacity)
	fmt.Printf("Max ingress queue depth: %d\n", stats.MaxIngressQueueDepthObserved)
	fmt.Printf("Max ingress queue utilization: %.1f%%\n", stats.MaxIngressQueueUtilization())
	fmt.Printf("Ingress accettata: %d\n", stats.IngressAccepted)
	fmt.Printf("Ingress queue drop: %d\n", stats.IngressQueueDropped)
	fmt.Printf("Telemetry invalida scartata: %d\n", stats.InvalidTelemetry)
	fmt.Printf("Finestre chiuse/out-of-order scartati: %d\n", stats.OutOfOrderDropped)
	fmt.Printf("Telemetry post-EOS scartata: %d\n", stats.PostEOSDropped)
	fmt.Printf("Telemetry processata: %d\n", stats.Processed)
	fmt.Printf("Aggregate Kafka emessi: %d\n", stats.AggregatesEmitted)
	fmt.Printf("EndOfReplay processati: %d\n", stats.EndOfReplayProcessed)
}
