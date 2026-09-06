package avrocodec

import (
	"fmt"
	"math"
	"time"

	"continuum/internal/model"
	"github.com/linkedin/goavro/v2"
)

// Le mappe restano confinate al codec; il dominio usa sempre le struct model.
func encodeRecord(codec *goavro.Codec, record map[string]any) ([]byte, error) {
	for field, value := range record {
		var err error
		switch value := value.(type) {
		case uint64:
			record[field], err = encodeCounter(value)
		case time.Time:
			record[field], err = encodeTimestamp(value)
		case model.MetricAggregate:
			record[field], err = encodeMetric(value)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
	}
	return codec.BinaryFromNative(nil, record)
}

func decodeRecord(codec *goavro.Codec, payload []byte) (map[string]any, error) {
	native, remaining, err := codec.NativeFromBinary(payload)
	if err != nil {
		return nil, err
	}
	if len(remaining) != 0 {
		return nil, fmt.Errorf("payload Avro con %d byte dopo il record", len(remaining))
	}

	// Lo schema statico e il decoder garantiscono campi e tipi delle asserzioni.
	record := native.(map[string]any)
	for field, value := range record {
		switch field {
		case "events", "input_aggregates":
			record[field], err = decodeCounter(value.(int64))
		case "window_start", "window_end", "emitted_at":
			record[field] = time.Unix(0, value.(int64)).UTC()
		case "temperature", "humidity", "pressure":
			record[field], err = decodeMetric(value.(map[string]any))
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
	}
	return record, nil
}

func encodeCounter(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("contatore %d non rappresentabile come Avro long", value)
	}
	return int64(value), nil
}

func decodeCounter(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("contatore Avro negativo: %d", value)
	}
	return uint64(value), nil
}

func encodeTimestamp(value time.Time) (int64, error) {
	// goavro tratta timestamp-nanos come il long sottostante. Il mapping
	// esplicito conserva la precisione di time.Time e normalizza il decode in UTC.
	nanos := value.UnixNano()
	if !time.Unix(0, nanos).Equal(value) {
		return 0, fmt.Errorf("timestamp %s non rappresentabile come timestamp-nanos", value.Format(time.RFC3339Nano))
	}
	return nanos, nil
}

func encodeMetric(metric model.MetricAggregate) (map[string]any, error) {
	valid, err := encodeCounter(metric.Valid)
	if err != nil {
		return nil, fmt.Errorf("valid: %w", err)
	}
	invalid, err := encodeCounter(metric.Invalid)
	if err != nil {
		return nil, fmt.Errorf("invalid: %w", err)
	}
	return map[string]any{
		"valid":   valid,
		"invalid": invalid,
		"sum":     metric.Sum,
		"average": encodeNullableDouble(metric.Average),
		"min":     encodeNullableDouble(metric.Min),
		"max":     encodeNullableDouble(metric.Max),
	}, nil
}

func decodeMetric(record map[string]any) (model.MetricAggregate, error) {
	valid, err := decodeCounter(record["valid"].(int64))
	if err != nil {
		return model.MetricAggregate{}, fmt.Errorf("valid: %w", err)
	}
	invalid, err := decodeCounter(record["invalid"].(int64))
	if err != nil {
		return model.MetricAggregate{}, fmt.Errorf("invalid: %w", err)
	}
	return model.MetricAggregate{
		Valid:   valid,
		Invalid: invalid,
		Sum:     record["sum"].(float64),
		Average: decodeNullableDouble(record["average"]),
		Min:     decodeNullableDouble(record["min"]),
		Max:     decodeNullableDouble(record["max"]),
	}, nil
}

func encodeNullableDouble(value *float64) any {
	if value == nil {
		return nil
	}
	return goavro.Union("double", *value)
}

func decodeNullableDouble(value any) *float64 {
	if value == nil {
		return nil
	}
	number := value.(map[string]any)["double"].(float64)
	return &number
}
