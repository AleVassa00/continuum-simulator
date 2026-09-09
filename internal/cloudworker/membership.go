package cloudworker

import (
	"fmt"
	"strings"

	"continuum/internal/model"
	"github.com/segmentio/kafka-go"
)

// PartitionForEdge uses exactly the Edge writer's balancer, including its signed
// hash conversion. Never replace it with a different FNV/modulo implementation.
func PartitionForEdge(edgeID string) int {
	return (&kafka.Hash{}).Balance(kafka.Message{Key: []byte(edgeID)}, 0, 1, 2, 3, 4, 5)
}

func BuildMembership(edgeIDs []string) (map[int][]string, error) {
	members := make(map[int][]string, model.SourcePartitionCount)
	for p := 0; p < model.SourcePartitionCount; p++ {
		members[p] = nil
	}
	seen := make(map[string]bool)
	for _, raw := range edgeIDs {
		id := strings.TrimSpace(raw)
		if id == "" || seen[id] {
			return nil, fmt.Errorf("empty or duplicate Edge membership entry %q", id)
		}
		seen[id] = true
		p := PartitionForEdge(id)
		members[p] = append(members[p], id)
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("Cloud Edge membership is required")
	}
	return members, nil
}
