package model

import "time"

type MetricAggregate struct {
	Valid   uint64
	Invalid uint64
	Sum     float64

	Average *float64
	Min     *float64
	Max     *float64
}

type EdgeAggregate struct {
	AggregateID string

	EdgeID string

	WindowStart time.Time
	WindowEnd   time.Time

	Events uint64

	Temperature MetricAggregate
	Humidity    MetricAggregate
	Pressure    MetricAggregate

	EmittedAt time.Time
}

type CloudEdgeAggregate struct {
	AggregateID string

	EdgeID string

	WindowStart time.Time
	WindowEnd   time.Time

	InputAggregates uint64

	Events uint64

	Temperature MetricAggregate
	Humidity    MetricAggregate
	Pressure    MetricAggregate

	EmittedAt time.Time
}

type GlobalAggregate struct {
	AggregateID string

	WindowStart time.Time
	WindowEnd   time.Time

	ExpectedEdges     uint64
	ContributingEdges uint64

	Events uint64

	Temperature MetricAggregate
	Humidity    MetricAggregate
	Pressure    MetricAggregate

	EmittedAt time.Time
}
