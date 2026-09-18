package experiment

import (
	"strings"
	"testing"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/globalaggregator"
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
	if err != nil || base.Cloud.ResolvedConsumerCommitBatchSize() != 1 || base.Cloud.ResolvedMaxEdgeWatermarkSkew() != cloudworker.DefaultMaxEdgeWatermarkSkew || base.Global.ResolvedMaxPartitionWatermarkSkew() != globalaggregator.DefaultMaxPartitionWatermarkSkew {
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

func TestGlobalWatermarkSkewConfig(t *testing.T) {
	configured := configFixture + "global:\n  max_partition_watermark_skew: 720h\n"
	cfg, err := Decode(strings.NewReader(configured))
	if err != nil || cfg.Global.ResolvedMaxPartitionWatermarkSkew() != 720*time.Hour {
		t.Fatalf("configured: %+v %v", cfg.Global, err)
	}
	for _, value := range []string{"0s", "-1s", "bad"} {
		if _, err := Decode(strings.NewReader(configFixture + "global:\n  max_partition_watermark_skew: " + value + "\n")); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestEdgeProducerBatchConfig(t *testing.T) {
	base, err := Decode(strings.NewReader(configFixture))
	if err != nil || base.Edge.ResolvedKafkaProducerBatchSize() != 1 || base.Edge.ResolvedKafkaProducerBatchMaxWait() != 100*time.Millisecond {
		t.Fatalf("default: %+v %v", base.Edge, err)
	}

	configured := strings.Replace(
		configFixture,
		"  ingress_queue_capacity: 10\n",
		"  ingress_queue_capacity: 10\n  kafka_producer_batch_size: 10\n  kafka_producer_batch_max_wait: 250ms\n",
		1,
	)
	cfg, err := Decode(strings.NewReader(configured))
	if err != nil || cfg.Edge.ResolvedKafkaProducerBatchSize() != 10 || cfg.Edge.ResolvedKafkaProducerBatchMaxWait() != 250*time.Millisecond {
		t.Fatalf("configured: %+v %v", cfg.Edge, err)
	}
}

func TestTargetedNetworkAndRealTimeWatermarkSkew(t *testing.T) {
	configured := strings.Replace(configFixture, "  acceleration_factor: 1\n", "  acceleration_factor: 20000\n", 1) + `global:
  max_partition_watermark_skew_real_time: 90ms
network:
  enabled: true
  edge_to_kafka:
    enabled: true
    edge_ids: [edge-8]
    delay: 50ms
  cloud_to_global:
    enabled: true
    source_partitions: [0]
    rate: 100kbit
`
	configured = strings.Replace(configured, "  workers: 1\n", "  workers: 1\n  max_edge_watermark_skew_real_time: 30ms\n", 1)
	cfg, err := Decode(strings.NewReader(configured))
	if err != nil {
		t.Fatal(err)
	}
	resolved := ResolveDefaults(cfg)
	if got := resolved.Cloud.ResolvedMaxEdgeWatermarkSkew(); got != 10*time.Minute {
		t.Fatalf("cloud event-time skew=%s, want 10m", got)
	}
	if got := resolved.Global.ResolvedMaxPartitionWatermarkSkew(); got != 30*time.Minute {
		t.Fatalf("global event-time skew=%s, want 30m", got)
	}
	if resolved.Network.EdgeToKafka.EdgeIDs[0] != "edge-8" || resolved.Network.CloudToGlobal.SourcePartitions[0] != 0 {
		t.Fatalf("target non preservati: %+v", resolved.Network)
	}
}
