// Package avrocodec serializza soltanto i payload degli aggregati Kafka.
// Ogni payload contiene un singolo record Avro binario con schema statico.
package avrocodec

import (
	_ "embed"
	"fmt"
	"time"

	"continuum/internal/model"
	"github.com/linkedin/goavro/v2"
)

//go:embed schemas/edge_aggregate.avsc
var edgeSchema string

//go:embed schemas/cloud_edge_aggregate.avsc
var cloudSchema string

var (
	edgeCodec  = mustCompileSchema(edgeSchema)
	cloudCodec = mustCompileSchema(cloudSchema)
)

func mustCompileSchema(schema string) *goavro.Codec {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		panic(fmt.Errorf("schema Avro incorporato non valido: %w", err))
	}
	return codec
}

func EncodeEdgeAggregate(aggregate model.EdgeAggregate) ([]byte, error) {
	return encodeRecord(edgeCodec, map[string]any{
		"aggregate_id": aggregate.AggregateID,
		"edge_id":      aggregate.EdgeID,
		"window_start": aggregate.WindowStart,
		"window_end":   aggregate.WindowEnd,
		"events":       aggregate.Events,
		"temperature":  aggregate.Temperature,
		"humidity":     aggregate.Humidity,
		"pressure":     aggregate.Pressure,
		"emitted_at":   aggregate.EmittedAt,
	})
}

func DecodeEdgeAggregate(payload []byte) (model.EdgeAggregate, error) {
	record, err := decodeRecord(edgeCodec, payload)
	if err != nil {
		return model.EdgeAggregate{}, err
	}
	return model.EdgeAggregate{
		AggregateID: record["aggregate_id"].(string),
		EdgeID:      record["edge_id"].(string),
		WindowStart: record["window_start"].(time.Time),
		WindowEnd:   record["window_end"].(time.Time),
		Events:      record["events"].(uint64),
		Temperature: record["temperature"].(model.MetricAggregate),
		Humidity:    record["humidity"].(model.MetricAggregate),
		Pressure:    record["pressure"].(model.MetricAggregate),
		EmittedAt:   record["emitted_at"].(time.Time),
	}, nil
}

func EncodeCloudEdgeAggregate(aggregate model.CloudEdgeAggregate) ([]byte, error) {
	return encodeRecord(cloudCodec, map[string]any{
		"aggregate_id":     aggregate.AggregateID,
		"edge_id":          aggregate.EdgeID,
		"window_start":     aggregate.WindowStart,
		"window_end":       aggregate.WindowEnd,
		"input_aggregates": aggregate.InputAggregates,
		"events":           aggregate.Events,
		"temperature":      aggregate.Temperature,
		"humidity":         aggregate.Humidity,
		"pressure":         aggregate.Pressure,
		"emitted_at":       aggregate.EmittedAt,
	})
}

func DecodeCloudEdgeAggregate(payload []byte) (model.CloudEdgeAggregate, error) {
	record, err := decodeRecord(cloudCodec, payload)
	if err != nil {
		return model.CloudEdgeAggregate{}, err
	}
	return model.CloudEdgeAggregate{
		AggregateID:     record["aggregate_id"].(string),
		EdgeID:          record["edge_id"].(string),
		WindowStart:     record["window_start"].(time.Time),
		WindowEnd:       record["window_end"].(time.Time),
		InputAggregates: record["input_aggregates"].(uint64),
		Events:          record["events"].(uint64),
		Temperature:     record["temperature"].(model.MetricAggregate),
		Humidity:        record["humidity"].(model.MetricAggregate),
		Pressure:        record["pressure"].(model.MetricAggregate),
		EmittedAt:       record["emitted_at"].(time.Time),
	}, nil
}
