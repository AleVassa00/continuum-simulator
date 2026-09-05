package main

import "sync"

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

type EdgeIngressQueueStats struct {
	Capacity         int
	MaxDepthObserved int
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
