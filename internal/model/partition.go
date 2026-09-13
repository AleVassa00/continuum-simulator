package model

import (
	"fmt"
	"strconv"
	"time"
)

// DefaultSourcePartitionCount is used when kafka.partitions is omitted.
// The configured count is fixed before a run and independent of Edge/Worker counts.
const DefaultSourcePartitionCount = 6

func ValidateSourcePartition(partition, count int) error {
	if count <= 0 || partition < 0 || partition >= count {
		return fmt.Errorf("source partition %d outside [0,%d)", partition, count)
	}
	return nil
}

func PartitionKey(partition int) string { return strconv.Itoa(partition) }

// Wire validation is structural; the owning aggregator validates the configured bound.
func ParsePartitionKey(key []byte) (int, error) {
	partition, err := strconv.Atoi(string(key))
	if err != nil {
		return 0, fmt.Errorf("invalid partition key %q", key)
	}
	if partition < 0 {
		return 0, fmt.Errorf("negative source partition %d", partition)
	}
	if PartitionKey(partition) != string(key) {
		return 0, fmt.Errorf("noncanonical partition key %q", key)
	}
	return partition, nil
}

// There are no other grouping dimensions in the current data model.
func PartitionAggregateID(partition int, start, end time.Time) string {
	return fmt.Sprintf("partition:%d:%s:%s", partition, start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
}

func ValidateCloudPartitionAggregate(a CloudPartitionAggregate) error {

	if a.SourcePartition < 0 {
		return fmt.Errorf("negative source partition %d", a.SourcePartition)
	}

	if a.WindowStart.IsZero() || !a.WindowEnd.After(a.WindowStart) || a.EmittedAt.IsZero() {
		return fmt.Errorf("invalid partition partial timestamps")
	}

	if a.AggregateID != PartitionAggregateID(a.SourcePartition, a.WindowStart, a.WindowEnd) {
		return fmt.Errorf("partial ID does not match source partition and window")
	}

	if a.InputAggregates == 0 || a.Events == 0 {
		return fmt.Errorf("empty partial: use partition progress instead")
	}

	for name, metric := range map[string]MetricAggregate{"temperature": a.Temperature, "humidity": a.Humidity, "pressure": a.Pressure} {
		if err := ValidateMetricAggregate(name, a.Events, metric); err != nil {
			return err
		}
	}
	return nil
}

func ValidatePartitionProgress(p PartitionProgress) error {
	if p.SourcePartition < 0 {
		return fmt.Errorf("negative source partition %d", p.SourcePartition)
	}
	if p.CompleteThrough.IsZero() {
		return fmt.Errorf("partition progress without complete_through")
	}
	return nil
}
