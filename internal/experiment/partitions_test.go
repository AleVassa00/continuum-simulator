package experiment

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"continuum/internal/model"
)

func TestKafkaPartitionsConfig(t *testing.T) {
	absent, err := Decode(strings.NewReader(configFixture))
	if err != nil {
		t.Fatal(err)
	}
	if absent.Kafka.ResolvedPartitions() != model.DefaultSourcePartitionCount {
		t.Fatal("wrong default")
	}
	defaultHash, _ := Fingerprint(absent)
	for _, count := range []int{1, 3, 6, 8} {
		cfg, err := Decode(strings.NewReader(configFixture + fmt.Sprintf("kafka:\n  partitions: %d\n", count)))
		if err != nil {
			t.Fatal(err)
		}
		payload, err := MarshalResolved(cfg)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip, err := Decode(strings.NewReader(string(payload)))
		if err != nil || roundTrip.Kafka.ResolvedPartitions() != count {
			t.Fatalf("roundtrip: %s %v", payload, err)
		}
		if BuildEffective(cfg, time.Now()).Kafka.ResolvedPartitions() != count {
			t.Fatal("lost effective count")
		}
		hash, _ := Fingerprint(cfg)
		if (hash == defaultHash) != (count == model.DefaultSourcePartitionCount) {
			t.Fatal("fingerprint ignores partitions or default")
		}
	}
	for _, value := range []string{"0", "-1", "1.5", "invalid"} {
		if _, err := Decode(strings.NewReader(configFixture + "kafka:\n  partitions: " + value + "\n")); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}
