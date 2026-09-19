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
	// Frontiera event-time certificata dall'Edge.
	CompleteThrough time.Time

	Events uint64

	Temperature MetricAggregate
	Humidity    MetricAggregate
	Pressure    MetricAggregate

	EmittedAt time.Time
}

type CloudPartitionAggregate struct {
	AggregateID string

	SourcePartition int

	WindowStart time.Time
	WindowEnd   time.Time
	// Frontiera event-time certificata dalla partizione.
	CompleteThrough time.Time

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

	ExpectedPartitions     uint64
	ContributingPartitions uint64

	Events uint64

	Temperature MetricAggregate
	Humidity    MetricAggregate
	Pressure    MetricAggregate

	EmittedAt time.Time
}
