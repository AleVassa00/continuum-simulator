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
	mu sync.Mutex

	data []EdgeIngressRecord
	head int
	size int

	terminal      *EdgeIngressRecord
	eosRegistered bool
	closed        bool
	wake          chan struct{}
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
	TelemetryReceived    uint64
	IngressAccepted      uint64
	IngressQueueDropped  uint64
	InvalidTelemetry     uint64
	OutOfOrderDropped    uint64
	PostEOSDropped       uint64
	Processed            uint64
	AggregatesEmitted    uint64
	EndOfReplayProcessed uint64
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

	return &EdgeIngressQueue{
		data: make([]EdgeIngressRecord, capacity),
		wake: make(chan struct{}, 1),
	}, nil
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
	queue.signal()

	return TelemetryEnqueued
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
	queue.signal()

	return true
}

func (
	queue *EdgeIngressQueue,
) Next() (EdgeIngressRecord, bool) {
	for {
		queue.mu.Lock()
		if queue.size > 0 {
			record := queue.data[queue.head]
			queue.data[queue.head] = EdgeIngressRecord{}
			queue.head = (queue.head + 1) % len(queue.data)
			queue.size--
			if queue.size == 0 {
				queue.head = 0
			}
			queue.mu.Unlock()
			return record, true
		}
		if queue.terminal != nil {
			record := *queue.terminal
			queue.terminal = nil
			queue.mu.Unlock()
			return record, true
		}
		if queue.closed {
			queue.mu.Unlock()
			return EdgeIngressRecord{}, false
		}
		queue.mu.Unlock()

		<-queue.wake
	}
}

func (
	queue *EdgeIngressQueue,
) Close() {
	queue.mu.Lock()
	queue.closed = true
	queue.signal()
	queue.mu.Unlock()
}

func (
	queue *EdgeIngressQueue,
) signal() {
	select {
	case queue.wake <- struct{}{}:
	default:
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
