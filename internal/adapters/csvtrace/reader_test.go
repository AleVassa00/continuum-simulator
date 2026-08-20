package csvtrace

import (
	"context"
	"strings"
	"testing"

	"continuum/internal/config"
)

func TestReaderPreservesRawMeasurementValues(t *testing.T) {
	input := "sensor;location;timestamp;temperature;humidity;pressure\n" +
		"42;7;'2025-01-15 10:30:00';abc;;100500\n"
	reader, err := New(strings.NewReader(input), testDatasetConfig())
	if err != nil {
		t.Fatal(err)
	}

	record, err := reader.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if record.SensorID != "42" || record.LocationID != "7" {
		t.Fatalf("unexpected identity: %+v", record)
	}
	if got := record.Measurements["temperature_c"]; got != "abc" {
		t.Fatalf("temperature = %q", got)
	}
	if got := record.Measurements["humidity_pct"]; got != "" {
		t.Fatalf("humidity = %q", got)
	}
	if got := record.Measurements["pressure_hpa"]; got != "100500" {
		t.Fatalf("pressure = %q", got)
	}
}

func TestReaderRejectsMissingConfiguredColumn(t *testing.T) {
	input := "sensor;location;timestamp;temperature;humidity\n"
	_, err := New(strings.NewReader(input), testDatasetConfig())
	if err == nil || !strings.Contains(err.Error(), `missing configured column "pressure"`) {
		t.Fatalf("New() error = %v", err)
	}
}

func TestReaderReportsInvalidTimestampWithRowNumber(t *testing.T) {
	input := "sensor;location;timestamp;temperature;humidity;pressure\n" +
		"42;7;not-a-time;1;2;3\n"
	reader, err := New(strings.NewReader(input), testDatasetConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "replay row 2 has invalid timestamp") {
		t.Fatalf("Next() error = %v", err)
	}
}

func testDatasetConfig() config.DatasetConfig {
	return config.DatasetConfig{
		Delimiter: ";", TimestampLayout: "2006-01-02 15:04:05", TimestampTimezone: "UTC",
		Columns: config.ColumnConfig{
			SensorID: "sensor", LocationID: "location", Timestamp: "timestamp",
		},
		Measurements: []config.MeasurementConfig{
			{Name: "temperature_c", Column: "temperature"},
			{Name: "humidity_pct", Column: "humidity"},
			{Name: "pressure_hpa", Column: "pressure"},
		},
	}
}
