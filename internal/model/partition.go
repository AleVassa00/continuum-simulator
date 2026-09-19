package model

import (
	"fmt"
	"strconv"
	"time"
)

// Numero di partizioni usato in assenza di configurazione.
const DefaultSourcePartitionCount = 6

func ValidateSourcePartition(partition, count int) error {
	if count <= 0 || partition < 0 || partition >= count {
		return fmt.Errorf("source partition %d outside [0,%d)", partition, count)
	}
	return nil
}

func PartitionKey(partition int) string { return strconv.Itoa(partition) }

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

	if a.CompleteThrough.IsZero() || a.CompleteThrough.Before(a.WindowEnd) {
		return fmt.Errorf("complete_through %s precede la fine della finestra %s",
			a.CompleteThrough.Format(time.RFC3339),
			a.WindowEnd.Format(time.RFC3339),
		)
	}

	if a.AggregateID != PartitionAggregateID(a.SourcePartition, a.WindowStart, a.WindowEnd) {
		return fmt.Errorf("partial ID does not match source partition and window")
	}

	if a.InputAggregates == 0 || a.Events == 0 {
		return fmt.Errorf("empty partial")
	}

	for name, metric := range map[string]MetricAggregate{"temperature": a.Temperature, "humidity": a.Humidity, "pressure": a.Pressure} {
		if err := ValidateMetricAggregate(name, a.Events, metric); err != nil {
			return err
		}
	}
	return nil
}
