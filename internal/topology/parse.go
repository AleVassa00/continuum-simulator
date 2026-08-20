package topology

import (
	"fmt"
	"strconv"
	"strings"
)

func parseAssignment(
	row []string,
	columns map[string]int,
) (Assignment, error) {

	sensorID, err := readRequiredValue(
		row,
		columns,
		"sensor_id",
	)

	if err != nil {
		return Assignment{}, err
	}

	macroareaID, err := readRequiredValue(
		row,
		columns,
		"macroarea_id",
	)

	if err != nil {
		return Assignment{}, err
	}

	edgeID, err := readRequiredValue(
		row,
		columns,
		"edge_id",
	)

	if err != nil {
		return Assignment{}, err
	}

	latitudeText, err := readRequiredValue(
		row,
		columns,
		"lat",
	)

	if err != nil {
		return Assignment{}, err
	}

	longitudeText, err := readRequiredValue(
		row,
		columns,
		"lon",
	)

	if err != nil {
		return Assignment{}, err
	}

	latitude, err := parseLatitude(latitudeText)

	if err != nil {
		return Assignment{}, err
	}

	longitude, err := parseLongitude(longitudeText)

	if err != nil {
		return Assignment{}, err
	}

	assignment := Assignment{
		SensorID:    sensorID,
		MacroareaID: macroareaID,
		EdgeID:      edgeID,
		Latitude:    latitude,
		Longitude:   longitude,
	}

	return assignment, nil
}

func readRequiredValue(
	row []string,
	columns map[string]int,
	columnName string,
) (string, error) {

	columnIndex := columns[columnName]

	if columnIndex >= len(row) {
		return "", fmt.Errorf(
			"missing value for column %q",
			columnName,
		)
	}

	value := strings.TrimSpace(row[columnIndex])

	if value == "" {
		return "", fmt.Errorf(
			"empty value for column %q",
			columnName,
		)
	}

	return value, nil
}

func parseLatitude(value string) (float64, error) {

	latitude, err := strconv.ParseFloat(value, 64)

	if err != nil {
		return 0, fmt.Errorf(
			"invalid latitude %q",
			value,
		)
	}

	if latitude < -90 || latitude > 90 {
		return 0, fmt.Errorf(
			"invalid latitude %q",
			value,
		)
	}

	return latitude, nil
}

func parseLongitude(value string) (float64, error) {

	longitude, err := strconv.ParseFloat(value, 64)

	if err != nil {
		return 0, fmt.Errorf(
			"invalid longitude %q",
			value,
		)
	}

	if longitude < -180 || longitude > 180 {
		return 0, fmt.Errorf(
			"invalid longitude %q",
			value,
		)
	}

	return longitude, nil
}
