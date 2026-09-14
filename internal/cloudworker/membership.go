package cloudworker

import (
	"fmt"
	"sort"
	"strings"
)

// BuildMembership validates the static Edge-to-partition plan generated before
// the experiment and indexes it by Kafka source partition.
func BuildMembership(edgePartitions map[string]int, partitionCount int) (map[int][]string, error) {
	if partitionCount <= 0 {
		return nil, fmt.Errorf("source partition count must be positive")
	}
	membership := make(map[int][]string, partitionCount)
	for partition := 0; partition < partitionCount; partition++ {
		membership[partition] = nil
	}
	for id, partition := range edgePartitions {
		if strings.TrimSpace(id) == "" || strings.TrimSpace(id) != id {
			return nil, fmt.Errorf("invalid Edge membership entry %q", id)
		}
		if partition < 0 || partition >= partitionCount {
			return nil, fmt.Errorf("partition %d for Edge %q is outside [0,%d)", partition, id, partitionCount)
		}
		membership[partition] = append(membership[partition], id)
	}
	if len(edgePartitions) == 0 {
		return nil, fmt.Errorf("Cloud Edge membership is required")
	}
	for partition := range membership {
		sort.Strings(membership[partition])
	}
	return membership, nil
}
