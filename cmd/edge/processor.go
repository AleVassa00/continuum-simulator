package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"continuum/internal/model"
)

type EdgeProcessor struct {
	edgeID        string
	ingress       *EdgeIngress
	aggregator    *WindowAggregator
	output        chan<- EdgeOutputRecord
	egressStopped <-chan struct{}
	stats         *EdgeStats
	lastEventTime time.Time
}

func (
	processor *EdgeProcessor,
) Run() error {
	defer close(processor.output)

	for {
		record, ok := processor.ingress.Next()
		if !ok {
			if aggregate := processor.aggregator.Flush(); aggregate != nil {
				return processor.emit(EdgeOutputRecord{
					Kind:      EdgeOutputAggregate,
					Aggregate: *aggregate,
				})
			}
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
		finalAggregate, err := processor.aggregator.EndReplay()
		if errors.Is(err, errEdgeReplayEnded) {
			fmt.Printf("%s: EndOfReplay duplicato ignorato\n", processor.edgeID)
			return nil
		}
		if err != nil {
			return err
		}
		if finalAggregate != nil {
			if err := processor.emit(EdgeOutputRecord{
				Kind:      EdgeOutputAggregate,
				Aggregate: *finalAggregate,
			}); err != nil {
				return fmt.Errorf("flush finestra finale Edge %s fallito: %w", processor.edgeID, err)
			}
		}

		emittedAt := time.Now().UTC()
		lastEventTime := processor.lastEventTime
		if lastEventTime.IsZero() {
			lastEventTime = emittedAt
		}

		eos := model.EndOfReplay{
			EdgeID:        processor.edgeID,
			LastEventTime: lastEventTime,
			EmittedAt:     emittedAt,
		}
		if err := model.ValidateEndOfReplay(eos); err != nil {
			return err
		}
		return processor.emit(EdgeOutputRecord{
			Kind:        EdgeOutputEndOfReplay,
			EndOfReplay: eos,
		})
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

	aggregate, err := processor.aggregator.Add(
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

	if aggregate != nil {
		if err := processor.emit(EdgeOutputRecord{
			Kind:      EdgeOutputAggregate,
			Aggregate: *aggregate,
		}); err != nil {
			return err
		}
	}

	processor.lastEventTime = event.EventTime
	processor.stats.processed.Add(1)
	return nil
}

func (processor *EdgeProcessor) emit(record EdgeOutputRecord) error {
	select {
	case processor.output <- record:
		return nil
	case <-processor.egressStopped:
		return fmt.Errorf("Kafka egress terminato")
	}
}

type EdgeOutputKind byte

const (
	EdgeOutputAggregate EdgeOutputKind = iota
	EdgeOutputEndOfReplay
)

type EdgeOutputRecord struct {
	Kind        EdgeOutputKind
	Aggregate   model.EdgeAggregate
	EndOfReplay model.EndOfReplay
}

func validateSensorEvent(event model.SensorEvent) error {
	if strings.TrimSpace(event.EventID) == "" {
		return fmt.Errorf("event_id mancante")
	}

	if strings.TrimSpace(event.SensorID) == "" {
		return fmt.Errorf("sensor_id mancante")
	}

	if event.EventTime.IsZero() {
		return fmt.Errorf("event_time mancante")
	}

	return nil
}

func parseMeasurements(event model.SensorEvent) EdgeMeasurement {
	return EdgeMeasurement{
		Temperature: parseMetric(event.Measurements, "temperature", -40, 85),
		Humidity:    parseMetric(event.Measurements, "humidity", 0, 100),
		Pressure:    parseMetric(event.Measurements, "pressure", 30000, 110000),
	}
}

func parseMetric(measurements map[string]model.NullableFloat64, name string, minValue float64, maxValue float64) MetricValue {
	measurement, found := measurements[name]

	if !found {
		return MetricValue{
			Valid: false,
		}
	}

	if !measurement.Valid {
		return MetricValue{
			Valid: false,
		}
	}

	value := measurement.Value

	if math.IsNaN(value) ||
		math.IsInf(value, 0) {
		return MetricValue{
			Valid: false,
		}
	}

	if value < minValue ||
		value > maxValue {
		return MetricValue{
			Valid: false,
		}
	}

	return MetricValue{
		Value: value,
		Valid: true,
	}
}
