package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"continuum/internal/model"
)

type EdgeIngressKind byte

const (
	EdgeIngressTelemetry EdgeIngressKind = iota
	EdgeIngressEndOfReplay
)

type EdgeIngressRecord struct {
	Kind    EdgeIngressKind
	Payload []byte
}

type TelemetryEnqueueResult byte

const (
	TelemetryEnqueued TelemetryEnqueueResult = iota
	TelemetryDroppedQueueFull
	TelemetryDroppedAfterEOS
	TelemetryDroppedQueueClosed
)

type EdgeIngress struct {
	mu sync.Mutex

	records chan EdgeIngressRecord

	telemetryCapacity int
	telemetryQueued   int
	maxDepth          int

	eosRegistered bool
	closed        bool
}

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

type EdgeIngressQueueStats struct {
	Capacity         int
	MaxDepthObserved int
}

type EdgeProcessor struct {
	edgeID        string
	ingress       *EdgeIngress
	aggregator    *WindowAggregator
	stats         *EdgeStats
	lastEventTime time.Time
}

func newEdgeIngress(capacity int) *EdgeIngress {
	return &EdgeIngress{
		records:           make(chan EdgeIngressRecord, capacity+1),
		telemetryCapacity: capacity,
	}
}

func (ingress *EdgeIngress) TryEnqueueTelemetry(payload []byte) TelemetryEnqueueResult {
	ingress.mu.Lock()
	defer ingress.mu.Unlock()

	if ingress.closed {
		return TelemetryDroppedQueueClosed
	}
	if ingress.eosRegistered {
		return TelemetryDroppedAfterEOS
	}
	if ingress.telemetryQueued >= ingress.telemetryCapacity {
		return TelemetryDroppedQueueFull
	}

	ingress.records <- EdgeIngressRecord{
		Kind:    EdgeIngressTelemetry,
		Payload: append([]byte(nil), payload...),
	}
	ingress.telemetryQueued++
	ingress.maxDepth = max(ingress.maxDepth, ingress.telemetryQueued)

	return TelemetryEnqueued
}

func (
ingress *EdgeIngress,
) RegisterEndOfReplay() {
	ingress.mu.Lock()
	defer ingress.mu.Unlock()

	if ingress.closed || ingress.eosRegistered {
		return
	}

	ingress.records <- EdgeIngressRecord{
		Kind: EdgeIngressEndOfReplay,
	}
	ingress.eosRegistered = true
}

func (
ingress *EdgeIngress,
) Next() (EdgeIngressRecord, bool) {
	record, ok := <-ingress.records
	if !ok {
		return EdgeIngressRecord{}, false
	}

	if record.Kind == EdgeIngressTelemetry {
		ingress.mu.Lock()
		ingress.telemetryQueued--
		ingress.mu.Unlock()
	}

	return record, true
}

func (
ingress *EdgeIngress,
) Close() {
	ingress.mu.Lock()
	defer ingress.mu.Unlock()

	if ingress.closed {
		return
	}

	ingress.closed = true
	close(ingress.records)
}

func (ingress *EdgeIngress) Stats() EdgeIngressQueueStats {
	ingress.mu.Lock()
	defer ingress.mu.Unlock()

	return EdgeIngressQueueStats{
		Capacity:         ingress.telemetryCapacity,
		MaxDepthObserved: ingress.maxDepth,
	}
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

func (
processor *EdgeProcessor,
) Run() error {
	for {
		record, ok := processor.ingress.Next()
		if !ok {
			return nil
		}

		if err := processor.Process(record); err != nil {
			return err
		}
	}
}

func (
processor *EdgeProcessor,
) Process(record EdgeIngressRecord) error {
	switch record.Kind {
	case EdgeIngressTelemetry:
		return processor.processTelemetry(record.Payload)
	case EdgeIngressEndOfReplay:
		emittedAt := time.Now().UTC()
		lastEventTime := processor.lastEventTime
		if lastEventTime.IsZero() {
			lastEventTime = emittedAt
		}

		if err := processor.aggregator.EndReplay(
			model.EndOfReplay{
				EdgeID:        processor.edgeID,
				LastEventTime: lastEventTime,
				EmittedAt:     emittedAt,
			},
		); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("tipo Edge ingress sconosciuto: %d", record.Kind)
	}
}

func (
processor *EdgeProcessor,
) processTelemetry(payload []byte) error {
	var event model.SensorEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		processor.stats.invalidTelemetry.Add(1)
		return nil
	}
	if err := validateSensorEvent(event); err != nil {
		processor.stats.invalidTelemetry.Add(1)
		return nil
	}

	err := processor.aggregator.Add(
		event.EventID,
		event.EventTime,
		parseMeasurements(event),
	)
	if errors.Is(err, errEdgeWindowClosed) {
		processor.stats.outOfOrderDropped.Add(1)
		return nil
	}
	if errors.Is(err, errEdgeReplayEnded) {
		processor.stats.postEOSDropped.Add(1)
		return nil
	}
	if err != nil {
		return err
	}

	processor.lastEventTime = event.EventTime
	processor.stats.processed.Add(1)
	return nil
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
