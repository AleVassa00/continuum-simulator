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

type EdgeIngressQueue struct {
	mu   sync.Mutex
	cond *sync.Cond

	data     []EdgeIngressRecord
	head     int
	size     int
	maxDepth int

	terminal      *EdgeIngressRecord
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
	CurrentIngressQueueDepth     int
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
	CurrentDepth     int
	MaxDepthObserved int
}

type EdgeProcessor struct {
	edgeID     string
	ingress    *EdgeIngressQueue
	aggregator *WindowAggregator
	stats      *EdgeStats
	now        func() time.Time
}

func newEdgeIngressQueue(capacity int) (*EdgeIngressQueue, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf(
			"EDGE_INGRESS_QUEUE_CAPACITY deve essere maggiore di zero",
		)
	}

	queue := &EdgeIngressQueue{
		data: make([]EdgeIngressRecord, capacity),
	}
	queue.cond = sync.NewCond(&queue.mu)

	return queue, nil
}

func (
	queue *EdgeIngressQueue,
) TryEnqueueTelemetry(payload []byte) TelemetryEnqueueResult {
	queue.mu.Lock()
	defer queue.mu.Unlock()

	if queue.closed {
		return TelemetryDroppedQueueClosed
	}
	if queue.eosRegistered {
		return TelemetryDroppedAfterEOS
	}
	if queue.size == len(queue.data) {
		return TelemetryDroppedQueueFull
	}

	tail := (queue.head + queue.size) % len(queue.data)
	queue.data[tail] = EdgeIngressRecord{
		Kind:    EdgeIngressTelemetry,
		Payload: append([]byte(nil), payload...),
	}
	queue.size++
	queue.maxDepth = max(queue.maxDepth, queue.size)
	queue.cond.Signal()

	return TelemetryEnqueued
}

func (queue *EdgeIngressQueue) Stats() EdgeIngressQueueStats {
	queue.mu.Lock()
	defer queue.mu.Unlock()

	return EdgeIngressQueueStats{
		Capacity:         len(queue.data),
		CurrentDepth:     queue.size,
		MaxDepthObserved: queue.maxDepth,
	}
}

func (
	queue *EdgeIngressQueue,
) RegisterEndOfReplay(payload []byte) bool {
	queue.mu.Lock()
	defer queue.mu.Unlock()

	if queue.closed || queue.eosRegistered {
		return false
	}

	record := EdgeIngressRecord{
		Kind:    EdgeIngressEndOfReplay,
		Payload: append([]byte(nil), payload...),
	}
	queue.terminal = &record
	queue.eosRegistered = true
	queue.cond.Signal()

	return true
}

func (
	queue *EdgeIngressQueue,
) Next() (EdgeIngressRecord, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()

	for queue.size == 0 &&
		queue.terminal == nil &&
		!queue.closed {
		queue.cond.Wait()
	}

	if queue.size > 0 {
		record := queue.data[queue.head]
		queue.data[queue.head] = EdgeIngressRecord{}
		queue.head = (queue.head + 1) % len(queue.data)
		queue.size--
		if queue.size == 0 {
			queue.head = 0
		}
		return record, true
	}
	if queue.terminal != nil {
		record := *queue.terminal
		queue.terminal = nil
		return record, true
	}

	return EdgeIngressRecord{}, false
}

func (
	queue *EdgeIngressQueue,
) Close() {
	queue.mu.Lock()
	queue.closed = true
	queue.cond.Broadcast()
	queue.mu.Unlock()
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
) SnapshotWithQueue(queue *EdgeIngressQueue) EdgeStatsSnapshot {
	snapshot := stats.Snapshot()
	queueStats := queue.Stats()
	snapshot.IngressQueueCapacity = queueStats.Capacity
	snapshot.CurrentIngressQueueDepth = queueStats.CurrentDepth
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
		if err := handleEndOfReplayPayload(
			processor.edgeID,
			record.Payload,
			processor.aggregator,
			processor.now().UTC(),
		); err != nil {
			return err
		}
		processor.stats.endOfReplayProcessed.Add(1)
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
		event.ObservedAt,
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

	processor.stats.processed.Add(1)
	return nil
}
