package main

import (
	"continuum/internal/model"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

func runEdgeLoop(ingress *EdgeIngress, aggregator *WindowAggregator, output chan<- EdgeOutputRecord, egressStopped <-chan struct{}, stats *EdgeStats) error {
	defer close(output)

	edgeID := aggregator.edgeID

	for {
		record, ok := ingress.Next()

		if !ok {
			finalAggregate := aggregator.Flush()

			if finalAggregate != nil {
				if err := emitEdgeOutput(output, egressStopped, EdgeOutputRecord{Kind: EdgeOutputAggregate, Aggregate: *finalAggregate}); err != nil {
					return fmt.Errorf("flush finale Edge %s fallito: %w", edgeID, err)
				}
			}
			return nil
		}

		switch record.Kind {
		case EdgeIngressTelemetry:
			if err := processTelemetry(record.Payload, aggregator, output, egressStopped, stats); err != nil {
				return err
			}

		case EdgeIngressEndOfReplay:
			finalAggregate := aggregator.EndReplay()

			if finalAggregate != nil {
				if err := emitEdgeOutput(output, egressStopped, EdgeOutputRecord{Kind: EdgeOutputAggregate, Aggregate: *finalAggregate}); err != nil {
					return fmt.Errorf("flush finestra finale Edge %s fallito: %w",
						edgeID,
						err,
					)
				}
			}

			// Simulator EOS only: the Kafka stream contains data, never per-Edge EOS.
			// runEdge reports completion only after the Kafka egress has drained.
			stats.endOfReplayProcessed.Add(1)

			return nil

		default:
			return fmt.Errorf("tipo Edge ingress sconosciuto: %d", record.Kind)
		}
	}
}

func processTelemetry(payload []byte, aggregator *WindowAggregator, output chan<- EdgeOutputRecord, egressStopped <-chan struct{}, stats *EdgeStats) error {
	var event model.SensorEvent

	if err := json.Unmarshal(payload, &event); err != nil {
		stats.invalidTelemetry.Add(1)
		return nil
	}

	if err := validateSensorEvent(event); err != nil {
		stats.invalidTelemetry.Add(1)
		return nil
	}

	aggregate, err := aggregator.Add(event.EventID, event.EventTime, parseMeasurements(event))

	if errors.Is(err, errEdgeWindowClosed) {
		stats.outOfOrderDropped.Add(1)
		return nil
	}

	if err != nil {
		return err
	}

	if aggregate != nil {
		if err := emitEdgeOutput(output, egressStopped, EdgeOutputRecord{Kind: EdgeOutputAggregate, Aggregate: *aggregate}); err != nil {
			return err
		}
	}

	stats.processed.Add(1)

	return nil
}

func emitEdgeOutput(output chan<- EdgeOutputRecord, egressStopped <-chan struct{}, record EdgeOutputRecord) error {
	select {
	case output <- record:
		return nil

	case <-egressStopped:
		return fmt.Errorf("Kafka egress terminato")
	}
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

		Humidity: parseMetric(event.Measurements, "humidity", 0, 100),

		Pressure: parseMetric(event.Measurements, "pressure", 30000, 110000),
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

	if math.IsNaN(value) || math.IsInf(value, 0) {
		return MetricValue{
			Valid: false,
		}
	}

	if value < minValue || value > maxValue {
		return MetricValue{
			Valid: false,
		}
	}

	return MetricValue{
		Value: value,
		Valid: true,
	}
}
