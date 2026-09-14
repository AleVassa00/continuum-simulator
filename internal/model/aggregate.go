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
	// CompleteThrough certifies that this Edge will not subsequently publish
	// an aggregate whose WindowEnd is at or before this event-time frontier.
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

// PartitionProgress certifies that all accepted nonempty partials ending at or
// before CompleteThrough have already been published on the same ordered output
// stream. The frontier is monotonic and may apply the configured maximum skew.
type PartitionProgress struct {
	SourcePartition int
	CompleteThrough time.Time
}
