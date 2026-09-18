package avrocodec

import (
	"continuum/internal/model"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestPartitionAvroRoundTripAndMalformedPayloads(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 123, time.UTC)
	end := start.Add(15 * time.Minute)
	a := model.CloudPartitionAggregate{SourcePartition: 5, WindowStart: start, WindowEnd: end, CompleteThrough: end, AggregateID: model.PartitionAggregateID(5, start, end), InputAggregates: 3, Events: 2, Temperature: model.MetricAggregate{Invalid: 2}, Humidity: model.MetricAggregate{Invalid: 2}, Pressure: model.MetricAggregate{Invalid: 2}, EmittedAt: end}
	payload, err := EncodeCloudPartitionAggregate(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCloudPartitionAggregate(payload)
	if err != nil || !reflect.DeepEqual(a, got) {
		t.Fatalf("roundtrip: %+v %v", got, err)
	}
	if err := model.ValidateCloudPartitionAggregate(got); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{payload[:len(payload)-1], append(append([]byte{}, payload...), 0), []byte("{}")} {
		if _, err := DecodeCloudPartitionAggregate(bad); err == nil {
			t.Fatal("malformed Avro accepted")
		}
	}
	a.Events = math.MaxUint64
	if _, err := EncodeCloudPartitionAggregate(a); err == nil {
		t.Fatal("overflow accepted")
	}
}
