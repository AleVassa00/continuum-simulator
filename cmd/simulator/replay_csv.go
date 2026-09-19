package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"continuum/internal/model"
)

type SensorMeasurement struct {
	SensorID  string
	EventTime time.Time

	Pressure    string
	Temperature string
	Humidity    string
}

var replayCSVHeader = []string{"sensor_id", "timestamp", "pressure", "temperature", "humidity"}

func validateReplayCSVHeader(header []string) error {
	if len(header) != len(replayCSVHeader) {
		return fmt.Errorf("header replay non valido: attese esattamente %d colonne", len(replayCSVHeader))
	}
	for index, expected := range replayCSVHeader {
		if header[index] != expected {
			return fmt.Errorf("header replay non valido: colonna %d deve essere %q", index+1, expected)
		}
	}
	return nil
}

func parseMeasurement(row []string) (SensorMeasurement, error) {
	eventTime, err := parseEventTime(strings.TrimSpace(row[1]))
	if err != nil {
		return SensorMeasurement{}, err
	}

	measurement := SensorMeasurement{
		SensorID:    strings.TrimSpace(row[0]),
		EventTime:   eventTime,
		Pressure:    strings.TrimSpace(row[2]),
		Temperature: strings.TrimSpace(row[3]),
		Humidity:    strings.TrimSpace(row[4]),
	}

	return measurement, nil
}

func parseEventTime(value string) (time.Time, error) {
	eventTime, err := time.Parse(time.RFC3339, value)

	if err == nil {
		return eventTime, nil
	}

	eventTime, err = time.ParseInLocation("2006-01-02T15:04:05", value, time.UTC)

	if err != nil {
		return time.Time{},
			fmt.Errorf("event_time non valido %q: %w", value, err)
	}

	return eventTime, nil
}

func parseNullableMeasurement(value string) (model.NullableFloat64, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "null") {
		return model.NullableFloat64{}, nil
	}

	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return model.NullableFloat64{}, fmt.Errorf("misura %q non numerica: %w", value, err)
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return model.NullableFloat64{}, fmt.Errorf("misura %q non finita", value)
	}

	return model.NullableFloat64{
		Value: parsed,
		Valid: true,
	}, nil
}

func buildSensorEvent(measurement SensorMeasurement, sequence uint64) (model.SensorEvent, error) {
	pressure, err := parseNullableMeasurement(measurement.Pressure)
	if err != nil {
		return model.SensorEvent{}, fmt.Errorf("pressure non valida: %w", err)
	}

	temperature, err := parseNullableMeasurement(measurement.Temperature)
	if err != nil {
		return model.SensorEvent{}, fmt.Errorf("temperature non valida: %w", err)
	}

	humidity, err := parseNullableMeasurement(measurement.Humidity)
	if err != nil {
		return model.SensorEvent{}, fmt.Errorf("humidity non valida: %w", err)
	}

	return model.SensorEvent{
		EventID:  fmt.Sprintf("%s-%d", measurement.SensorID, sequence),
		SensorID: measurement.SensorID,
		Sequence: sequence,

		EventTime: measurement.EventTime,

		Measurements: map[string]model.NullableFloat64{
			"pressure":    pressure,
			"temperature": temperature,
			"humidity":    humidity,
		},
	}, nil
}

func openReplayFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("apertura REPLAY_FILE %q fallita: %w", path, err)
	}

	return file, nil
}
