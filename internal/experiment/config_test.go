package experiment

import (
	"strings"
	"testing"
)

const configFixture = `experiment:
  name: test
workload:
  acceleration_factor: 1
  start_lead_time: 1m
simulator:
  telemetry_queue_capacity: 10
edge:
  window_size: 5m
  ingress_queue_capacity: 10
cloud:
  workers: 1
  window_size: 15m
`

func TestConsumerCommitBatchSizeConfig(t *testing.T) {
	base, err := Decode(strings.NewReader(configFixture))
	if err != nil || base.Cloud.ResolvedConsumerCommitBatchSize() != 1 {
		t.Fatalf("default: %+v %v", base, err)
	}
	for _, value := range []string{"0", "-1", "1.5", "bad"} {
		if _, err := Decode(strings.NewReader(configFixture + "  consumer_commit_batch_size: " + value + "\n")); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
	for _, value := range []string{"1", "32"} {
		cfg, err := Decode(strings.NewReader(configFixture + "  consumer_commit_batch_size: " + value + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		payload, err := MarshalResolved(cfg)
		if err != nil {
			t.Fatal(err)
		}
		again, err := Decode(strings.NewReader(string(payload)))
		if err != nil || again.Cloud.ResolvedConsumerCommitBatchSize() != cfg.Cloud.ResolvedConsumerCommitBatchSize() {
			t.Fatalf("roundtrip: %s %v", payload, err)
		}
		a, _ := Fingerprint(base)
		b, _ := Fingerprint(cfg)
		if (a == b) != (value == "1") {
			t.Fatal("batch size not reflected in fingerprint")
		}
	}
}
