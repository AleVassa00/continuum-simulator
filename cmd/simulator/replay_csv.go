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

/* Riga del dataset */
type SensorMeasurement struct {
	SensorID   string
	SensorType string
	LocationID string

	Latitude  float64
	Longitude float64
	EventTime time.Time

	Pressure    string
	Temperature string
	Humidity    string
}

// buildColumnIndex associa ciascun nome di colonna CSV al relativo indice
func buildColumnIndex(header []string) map[string]int {
	columns := make(map[string]int)

	for index, name := range header {
		name = strings.TrimSpace(name)
		columns[name] = index
	}
	return columns
}

// requiredColumn restituisce l'indice di una colonna richiesta oppure un errore se la colonna non è presente
func requiredColumn(columns map[string]int, name string) (int, error) {
	index, found := columns[name]

	if !found {
		return 0,
			fmt.Errorf("colonna %q non trovata nel CSV", name)
	}

	return index, nil
}

// parseMeasurement converte una riga CSV nella rappresentazione interna SensorMeasurement
func parseMeasurement(row []string, columns map[string]int) (SensorMeasurement, error) {
	sensorIDIndex, err := requiredColumn(columns, "sensor_id")
	if err != nil {
		return SensorMeasurement{}, err
	}

	sensorTypeIndex, err := requiredColumn(columns, "sensor_type")
	if err != nil {
		return SensorMeasurement{}, err
	}

	locationIndex, err := requiredColumn(columns, "location")
	if err != nil {
		return SensorMeasurement{}, err
	}

	latitudeIndex, err := requiredColumn(columns, "lat")
	if err != nil {
		return SensorMeasurement{}, err
	}

	longitudeIndex, err := requiredColumn(columns, "lon")
	if err != nil {
		return SensorMeasurement{}, err
	}

	eventTimeIndex, err := requiredColumn(columns, "timestamp")
	if err != nil {
		return SensorMeasurement{}, err
	}

	pressureIndex, err := requiredColumn(columns, "pressure")
	if err != nil {
		return SensorMeasurement{}, err
	}

	temperatureIndex, err := requiredColumn(columns, "temperature")
	if err != nil {
		return SensorMeasurement{}, err
	}

	humidityIndex, err := requiredColumn(columns, "humidity")
	if err != nil {
		return SensorMeasurement{}, err
	}

	latitude, err := strconv.ParseFloat(strings.TrimSpace(row[latitudeIndex]), 64)
	if err != nil {
		return SensorMeasurement{}, fmt.Errorf("latitudine non valida %q: %w",
			row[latitudeIndex],
			err)
	}

	longitude, err := strconv.ParseFloat(strings.TrimSpace(row[longitudeIndex]), 64)
	if err != nil {
		return SensorMeasurement{},
			fmt.Errorf("longitudine non valida %q: %w", row[longitudeIndex], err)
	}

	eventTime, err := parseEventTime(strings.TrimSpace(row[eventTimeIndex]))
	if err != nil {
		return SensorMeasurement{}, err
	}

	measurement := SensorMeasurement{
		SensorID:    strings.TrimSpace(row[sensorIDIndex]),
		SensorType:  strings.TrimSpace(row[sensorTypeIndex]),
		LocationID:  strings.TrimSpace(row[locationIndex]),
		Latitude:    latitude,
		Longitude:   longitude,
		EventTime:   eventTime,
		Pressure:    strings.TrimSpace(row[pressureIndex]),
		Temperature: strings.TrimSpace(row[temperatureIndex]),
		Humidity:    strings.TrimSpace(row[humidityIndex]),
	}

	return measurement, nil
}

// parseEventTime interpreta il timestamp del dataset nei formati supportati
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

// parseNullableMeasurement converte una misura testuale in un valore numerico nullable, preservando i valori mancanti
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

// buildSensorEvent converte una misura del dataset nel SensorEvent utilizzato dal dominio
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
		EventID:    fmt.Sprintf("%s-%d", measurement.SensorID, sequence),
		SensorID:   measurement.SensorID,
		SensorType: measurement.SensorType,
		LocationID: measurement.LocationID,
		Sequence:   sequence,

		EventTime: measurement.EventTime,

		Measurements: map[string]model.NullableFloat64{
			"pressure":    pressure,
			"temperature": temperature,
			"humidity":    humidity,
		},
	}, nil
}

// openReplayFile apre il file CSV utilizzato per il replay
func openReplayFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("apertura REPLAY_FILE %q fallita: %w", path, err)
	}

	return file, nil
}
