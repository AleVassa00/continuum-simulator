package model

import (
	"fmt"
	"strconv"
	"time"
)

// SourcePartitionCount is fixed for the entire experiment, regardless of workers.
const SourcePartitionCount = 6

func ValidateSourcePartition(partition int) error {
	if partition < 0 || partition >= SourcePartitionCount {
		return fmt.Errorf("source partition %d outside [0,%d)", partition, SourcePartitionCount)
	}
	return nil
}

func PartitionKey(partition int) string { return strconv.Itoa(partition) }

func ParsePartitionKey(key []byte) (int, error) {
	partition, err := strconv.Atoi(string(key))
	if err != nil {
		return 0, fmt.Errorf("invalid partition key %q", key)
	}
	if err := ValidateSourcePartition(partition); err != nil {
		return 0, err
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
	if err := ValidateSourcePartition(a.SourcePartition); err != nil {
		return err
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
	if err := ValidateSourcePartition(p.SourcePartition); err != nil {
		return err
	}
	if p.CompleteThrough.IsZero() {
		return fmt.Errorf("partition progress without complete_through")
	}
	return nil
}
