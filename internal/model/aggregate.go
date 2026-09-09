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

type CloudPartitionAggregate struct {
	AggregateID string

	SourcePartition int

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

	ExpectedPartitions     uint64
	ContributingPartitions uint64

	Events uint64

	Temperature MetricAggregate
	Humidity    MetricAggregate
	Pressure    MetricAggregate

	EmittedAt time.Time
}

// PartitionProgress certifies that all nonempty partials ending at or before
// CompleteThrough have already been published on the same ordered stream.
// Missing partials in that range are therefore zero contributions.
type PartitionProgress struct {
	SourcePartition int
	CompleteThrough time.Time
}
