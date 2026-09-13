package main

import (
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDefinitiveReplayCSVContract(t *testing.T) {
	reader := csv.NewReader(strings.NewReader(
		"sensor_id;timestamp;pressure;temperature;humidity\n" +
			"31637;2025-01-01T01:11:24;102874.66;9.45;75.43\n",
	))
	reader.Comma = ';'
	header, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateReplayCSVHeader(header); err != nil {
		t.Fatal(err)
	}
	row, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	measurement, err := parseMeasurement(row)
	if err != nil {
		t.Fatal(err)
	}
	if measurement.SensorID != "31637" || !measurement.EventTime.Equal(time.Date(2025, 1, 1, 1, 11, 24, 0, time.UTC)) {
		t.Fatalf("measurement non valida: %+v", measurement)
	}
	event, err := buildSensorEvent(measurement, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"sensor_type", "location", "lat", "lon"} {
		if strings.Contains(string(payload), removed) {
			t.Fatalf("il messaggio MQTT contiene ancora il campo rimosso %q: %s", removed, payload)
		}
	}
}

func TestReplayCSVRejectsOldOrReorderedSchema(t *testing.T) {
	for _, header := range [][]string{
		{"sensor_id", "sensor_type", "location", "lat", "lon", "timestamp", "pressure", "temperature", "humidity"},
		{"timestamp", "sensor_id", "pressure", "temperature", "humidity"},
		{"sensor_id", "timestamp", "pressure", "temperature", "humidity", "extra"},
	} {
		if err := validateReplayCSVHeader(header); err == nil {
			t.Fatalf("header non definitivo accettato: %v", header)
		}
	}
}
