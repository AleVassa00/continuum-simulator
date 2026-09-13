package experiment

import (
	"continuum/internal/cloudworker"
	"strings"
	"testing"
	"time"
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

func TestWatermarkDelayPresenceAndRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		want        time.Duration
		absent      bool
	}{
		{"default", "", cloudworker.DefaultWatermarkDelay, true},
		{"zero", "  watermark_delay: 0s\n", 0, false},
		{"configured", "  watermark_delay: 2m\n", 2 * time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Decode(strings.NewReader(configFixture + tc.field))
			if err != nil {
				t.Fatal(err)
			}
			if (cfg.Cloud.WatermarkDelay == nil) != tc.absent {
				t.Fatal("presence lost")
			}
			resolved := ResolveDefaults(cfg)
			if resolved.Cloud.WatermarkDelay == nil || resolved.Cloud.ResolvedWatermarkDelay().Duration() != tc.want {
				t.Fatal("incorrect resolved delay")
			}
			payload, err := MarshalResolved(cfg)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := Decode(strings.NewReader(string(payload)))
			if err != nil || decoded.Cloud.ResolvedWatermarkDelay().Duration() != tc.want {
				t.Fatalf("round trip: %s %v", payload, err)
			}
			if BuildEffective(cfg, time.Now()).Cloud.ResolvedWatermarkDelay().Duration() != tc.want {
				t.Fatal("effective config lost delay")
			}
		})
	}
}

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

func TestWatermarkDelayValidationAndFingerprint(t *testing.T) {
	for _, field := range []string{"  watermark_delay: -1s\n", "  watermark_delay: invalid\n"} {
		if _, err := Decode(strings.NewReader(configFixture + field)); err == nil {
			t.Fatal("invalid delay accepted")
		}
	}
	absent, _ := Decode(strings.NewReader(configFixture))
	explicit, _ := Decode(strings.NewReader(configFixture + "  watermark_delay: " + cloudworker.DefaultWatermarkDelay.String() + "\n"))
	zero, _ := Decode(strings.NewReader(configFixture + "  watermark_delay: 0s\n"))
	a, _ := Fingerprint(absent)
	b, _ := Fingerprint(explicit)
	c, _ := Fingerprint(zero)
	if a != b || a == c {
		t.Fatal("fingerprints do not reflect resolved watermark policy")
	}
}
