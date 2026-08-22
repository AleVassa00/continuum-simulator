package model

import "time"

type MetricAggregate struct {
	Valid   uint64 `json:"valid"`
	Invalid uint64 `json:"invalid"`

	Average *float64 `json:"average"`
	Min     *float64 `json:"min"`
	Max     *float64 `json:"max"`
}

type EdgeAggregate struct {
	SchemaVersion int `json:"schema_version"`

	EdgeID string `json:"edge_id"`

	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`

	Events uint64 `json:"events"`

	Temperature MetricAggregate `json:"temperature"`
	Humidity    MetricAggregate `json:"humidity"`
	Pressure    MetricAggregate `json:"pressure"`

	EmittedAt time.Time `json:"emitted_at"`
}
