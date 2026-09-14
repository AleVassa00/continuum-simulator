package cloudworker

import (
	"continuum/internal/kafkautil"
	"fmt"
	"sort"
	"strings"
)

// BuildMembership projects the immutable Edge topology through the exact same
// Kafka Hash balancer used by every Edge producer.
func BuildMembership(edgeIDs []string, partitionCount int) (map[int][]string, error) {
	if partitionCount <= 0 {
		return nil, fmt.Errorf("source partition count must be positive")
	}
	membership := make(map[int][]string, partitionCount)
	for partition := 0; partition < partitionCount; partition++ {
		membership[partition] = nil
	}
	seen := make(map[string]struct{}, len(edgeIDs))
	for _, raw := range edgeIDs {
		id := strings.TrimSpace(raw)
		if id == "" || id != raw {
			return nil, fmt.Errorf("invalid Edge membership entry %q", raw)
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("duplicate Edge membership entry %q", id)
		}
		seen[id] = struct{}{}
		partition := kafkautil.PartitionForEdge(id, partitionCount)
		membership[partition] = append(membership[partition], id)
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("Cloud Edge membership is required")
	}
	for partition := range membership {
		sort.Strings(membership[partition])
	}
	return membership, nil
}
