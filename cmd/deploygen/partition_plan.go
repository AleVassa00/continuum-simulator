package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func loadEdgePartitionWeights(
	path string,
	edges []EdgeDeployment,
) (map[string]uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("apertura pesi Edge %q fallita: %w", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 2
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("lettura header pesi Edge fallita: %w", err)
	}
	expectedHeader := []string{"edge_id", "replay_event_count"}
	for index := range expectedHeader {
		if header[index] != expectedHeader[index] {
			return nil, fmt.Errorf(
				"header pesi Edge non valido: attese esattamente le colonne %s",
				strings.Join(expectedHeader, ","),
			)
		}
	}

	expectedEdges := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		expectedEdges[edge.EdgeID] = struct{}{}
	}

	weights := make(map[string]uint64, len(edges))
	for {
		row, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("lettura pesi Edge fallita: %w", readErr)
		}

		edgeID := strings.TrimSpace(row[0])
		if edgeID == "" || edgeID != row[0] {
			return nil, fmt.Errorf("edge_id non valido nei pesi: %q", row[0])
		}
		if _, known := expectedEdges[edgeID]; !known {
			return nil, fmt.Errorf("pesi presenti per Edge sconosciuto %q", edgeID)
		}
		if _, duplicate := weights[edgeID]; duplicate {
			return nil, fmt.Errorf("pesi duplicati per Edge %q", edgeID)
		}

		weight, parseErr := strconv.ParseUint(row[1], 10, 64)
		if parseErr != nil || weight == 0 {
			return nil, fmt.Errorf("replay_event_count non valido per Edge %q: %q", edgeID, row[1])
		}
		weights[edgeID] = weight
	}

	for _, edge := range edges {
		if _, found := weights[edge.EdgeID]; !found {
			return nil, fmt.Errorf("pesi mancanti per Edge %q", edge.EdgeID)
		}
	}
	return weights, nil
}

func assignEdgePartitions(
	edges []EdgeDeployment,
	weights map[string]uint64,
	partitionCount int,
) ([]EdgeDeployment, error) {
	if partitionCount <= 0 {
		return nil, fmt.Errorf("numero di partition Kafka non valido: %d", partitionCount)
	}

	assigned := append([]EdgeDeployment(nil), edges...)
	sort.Slice(assigned, func(i int, j int) bool {
		leftWeight := weights[assigned[i].EdgeID]
		rightWeight := weights[assigned[j].EdgeID]
		if leftWeight != rightWeight {
			return leftWeight > rightWeight
		}
		return assigned[i].EdgeNumber < assigned[j].EdgeNumber
	})

	partitionLoads := make([]uint64, partitionCount)
	for index := range assigned {
		weight, found := weights[assigned[index].EdgeID]
		if !found || weight == 0 {
			return nil, fmt.Errorf("peso assente o non valido per Edge %q", assigned[index].EdgeID)
		}

		partition := 0
		for candidate := 1; candidate < partitionCount; candidate++ {
			if partitionLoads[candidate] < partitionLoads[partition] {
				partition = candidate
			}
		}
		if partitionLoads[partition] > math.MaxUint64-weight {
			return nil, fmt.Errorf("overflow nel calcolo del carico della partition %d", partition)
		}
		partitionLoads[partition] += weight
		assigned[index].KafkaPartition = partition
		assigned[index].ReplayEventCount = weight
	}

	sort.Slice(assigned, func(i int, j int) bool {
		return assigned[i].EdgeNumber < assigned[j].EdgeNumber
	})
	return assigned, nil
}
