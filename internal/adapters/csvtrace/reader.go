package csvtrace

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"continuum/internal/config"
	"continuum/internal/replay"
)

type Reader struct {
	csv       *csv.Reader
	closer    io.Closer
	columns   map[string]int
	dataset   config.DatasetConfig
	location  *time.Location
	rowNumber int
}

func Open(path string, dataset config.DatasetConfig) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open replay trace %q: %w", path, err)
	}
	reader, err := New(file, dataset)
	if err != nil {
		file.Close()
		return nil, err
	}
	reader.closer = file
	return reader, nil
}

func New(source io.Reader, dataset config.DatasetConfig) (*Reader, error) {
	delimiter := []rune(dataset.Delimiter)
	if len(delimiter) != 1 {
		return nil, fmt.Errorf("replay delimiter must contain exactly one character")
	}
	location, err := time.LoadLocation(dataset.TimestampTimezone)
	if err != nil {
		return nil, fmt.Errorf("load timestamp timezone %q: %w", dataset.TimestampTimezone, err)
	}

	csvReader := csv.NewReader(source)
	csvReader.Comma = delimiter[0]
	csvReader.ReuseRecord = true
	header, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("read replay header: %w", err)
	}
	columns := make(map[string]int, len(header))
	for index, name := range header {
		name = strings.TrimSpace(name)
		if _, duplicate := columns[name]; duplicate {
			return nil, fmt.Errorf("replay header contains duplicate column %q", name)
		}
		columns[name] = index
	}

	required := []string{
		dataset.Columns.SensorID,
		dataset.Columns.LocationID,
		dataset.Columns.Timestamp,
	}
	for _, measurement := range dataset.Measurements {
		required = append(required, measurement.Column)
	}
	for _, name := range required {
		if _, found := columns[name]; !found {
			return nil, fmt.Errorf("replay is missing configured column %q", name)
		}
	}

	return &Reader{
		csv: csvReader, columns: columns, dataset: dataset,
		location: location, rowNumber: 1,
	}, nil
}

func (r *Reader) Next(ctx context.Context) (replay.Record, error) {
	if err := ctx.Err(); err != nil {
		return replay.Record{}, err
	}
	row, err := r.csv.Read()
	if err != nil {
		return replay.Record{}, err
	}
	r.rowNumber++

	value := func(column string) string {
		index := r.columns[column]
		if index >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[index])
	}
	sensorID := value(r.dataset.Columns.SensorID)
	if sensorID == "" {
		return replay.Record{}, fmt.Errorf("replay row %d has empty sensor_id", r.rowNumber)
	}
	timestampText := strings.Trim(value(r.dataset.Columns.Timestamp), "'")
	eventTime, err := time.ParseInLocation(r.dataset.TimestampLayout, timestampText, r.location)
	if err != nil {
		return replay.Record{}, fmt.Errorf("replay row %d has invalid timestamp %q: %w", r.rowNumber, timestampText, err)
	}

	measurements := make(map[string]string, len(r.dataset.Measurements))
	for _, measurement := range r.dataset.Measurements {
		measurements[measurement.Name] = value(measurement.Column)
	}
	return replay.Record{
		SensorID:     sensorID,
		LocationID:   value(r.dataset.Columns.LocationID),
		EventTime:    eventTime,
		Measurements: measurements,
	}, nil
}

func (r *Reader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}
