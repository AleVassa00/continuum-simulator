package model

import "time"

const (
	EdgeAggregateSchemaVersion      = 2
	CloudEdgeAggregateSchemaVersion = 1
)

type MetricAggregate struct {
	Valid   uint64  `json:"valid"`
	Invalid uint64  `json:"invalid"`
	Sum     float64 `json:"sum"`

	Average *float64 `json:"average"`
	Min     *float64 `json:"min"`
	Max     *float64 `json:"max"`
}

type EdgeAggregate struct {
	SchemaVersion int    `json:"schema_version"`
	AggregateID   string `json:"aggregate_id"`

	EdgeID string `json:"edge_id"`

	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`

	Events          uint64 `json:"events"`
	DuplicateEvents uint64 `json:"duplicate_events"`

	Temperature MetricAggregate `json:"temperature"`
	Humidity    MetricAggregate `json:"humidity"`
	Pressure    MetricAggregate `json:"pressure"`

	EmittedAt time.Time `json:"emitted_at"`
}

type CloudEdgeAggregate struct {
	SchemaVersion int    `json:"schema_version"`
	AggregateID   string `json:"aggregate_id"`

	EdgeID string `json:"edge_id"`

	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`

	InputAggregates uint64 `json:"input_aggregates"`

	Events          uint64 `json:"events"`
	DuplicateEvents uint64 `json:"duplicate_events"`

	Temperature MetricAggregate `json:"temperature"`
	Humidity    MetricAggregate `json:"humidity"`
	Pressure    MetricAggregate `json:"pressure"`

	EmittedAt time.Time `json:"emitted_at"`
}
