package topology

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

func Load(path string) (*Index, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf(
			"open topology %q: %w",
			path,
			err,
		)
	}

	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()

	if err != nil {
		return nil, fmt.Errorf(
			"read topology header: %w",
			err,
		)
	}

	columns := createColumnIndex(header)
	err = validateRequiredColumns(columns)
	if err != nil {
		return nil, err
	}

	result := &Index{
		assignmentsBySensor: make(map[string]Assignment),
		edges:               make(map[string]Edge),
		macroareas:          make(map[string]bool),
	}
	rowNumber := 2
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf(
				"read topology row %d: %w",
				rowNumber,
				err,
			)
		}
		assignment, err := parseAssignment(row, columns)
		if err != nil {
			return nil, fmt.Errorf(
				"topology row %d: %w",
				rowNumber,
				err,
			)
		}
		err = result.addAssignment(assignment)
		if err != nil {
			return nil, fmt.Errorf(
				"topology row %d: %w",
				rowNumber,
				err,
			)
		}
		rowNumber++
	}
	if len(result.assignmentsBySensor) == 0 {
		return nil, fmt.Errorf(
			"topology contains no sensor assignments",
		)
	}
	return result, nil
}

func createColumnIndex(header []string) map[string]int {

	columns := make(map[string]int)

	for index, name := range header {

		columnName := strings.TrimSpace(name)

		columns[columnName] = index
	}

	return columns
}

func validateRequiredColumns(columns map[string]int) error {

	requiredColumns := []string{
		"sensor_id",
		"macroarea_id",
		"edge_id",
		"lat",
		"lon",
	}

	for _, column := range requiredColumns {

		_, exists := columns[column]

		if !exists {
			return fmt.Errorf(
				"topology is missing required column %q",
				column,
			)
		}
	}

	return nil
}

func (i *Index) addAssignment(assignment Assignment) error {

	_, exists := i.assignmentsBySensor[assignment.SensorID]

	if exists {
		return fmt.Errorf(
			"duplicate sensor_id %q",
			assignment.SensorID,
		)
	}

	i.assignmentsBySensor[assignment.SensorID] = assignment

	edge := i.edges[assignment.EdgeID]

	if edge.ID != "" && edge.MacroareaID != assignment.MacroareaID {
		return fmt.Errorf(
			"edge %q belongs to multiple macroareas",
			assignment.EdgeID,
		)
	}

	edge.ID = assignment.EdgeID
	edge.MacroareaID = assignment.MacroareaID
	edge.SensorCount++

	i.edges[edge.ID] = edge

	i.macroareas[assignment.MacroareaID] = true

	return nil
}
