package main

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"continuum/internal/experiment"
	"gopkg.in/yaml.v3"
)

func TestConfiguredPartitionsReachEveryDeployment(t *testing.T) {
	for _, count := range []int{1, 3, 8} {
		cfg, err := experiment.Load("../../experiments/baseline.yaml")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Kafka.Partitions = &count
		batchSize := experiment.CommitBatchSize(32)
		cfg.Cloud.ConsumerCommitBatchSize = &batchSize
		edges, err := assignEdgePartitions(
			[]EdgeDeployment{{EdgeID: "edge-0", SensorCount: 1, MQTTPort: 18830}},
			map[string]uint64{"edge-0": 1},
			count,
		)
		if err != nil {
			t.Fatal(err)
		}
		contents := []string{buildCompose(edges, experiment.BuildEffective(cfg, time.Now()))}
		for _, file := range buildDistributedComposes(edges, cfg) {
			contents = append(contents, file.Content)
		}
		consumers, initializers := 0, 0
		for _, content := range contents {
			var doc checkedCompose
			if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
				t.Fatal(err)
			}
			for name, service := range doc.Services {
				if name == "edge-0" {
					if service.Environment["KAFKA_PARTITION"] != "0" || service.Environment["SOURCE_PARTITION_COUNT"] != strconv.Itoa(count) {
						t.Fatal("static Edge partition assignment lost in deployment")
					}
					if service.Environment["KAFKA_PRODUCER_BATCH_SIZE"] != "1" || service.Environment["KAFKA_PRODUCER_BATCH_MAX_WAIT"] != "100ms" {
						t.Fatal("Edge producer batching defaults lost in deployment")
					}
				}
				if strings.HasPrefix(name, "cloud-worker-") && service.Environment["CLOUD_CONSUMER_COMMIT_BATCH_SIZE"] != "32" {
					t.Fatal("commit batch size lost in deployment")
				}
				if strings.HasPrefix(name, "cloud-worker-") && service.Environment["CLOUD_MAX_EDGE_WATERMARK_SKEW"] != "30m0s" {
					t.Fatal("maximum Edge watermark skew lost in deployment")
				}
				if name == "global-aggregator" || strings.HasPrefix(name, "cloud-worker-") {
					consumers++
					if service.Environment["SOURCE_PARTITION_COUNT"] != strconv.Itoa(count) {
						t.Fatalf("%s: wrong count", name)
					}
				}
				if name == "kafka-init" {
					initializers++
					command := strings.Join(service.Command, "\n")
					for _, expected := range []string{fmt.Sprintf("--partitions %d ", count), "--partitions 1 ", "set -euo pipefail", fmt.Sprintf("PartitionCount: %d([[:space:]]|$)", count)} {
						if !strings.Contains(command, expected) {
							t.Fatalf("missing bootstrap contract %q", expected)
						}
					}
					if strings.Contains(command, "--alter") {
						t.Fatal("runtime repartitioning introduced")
					}
				}
			}
		}
		if consumers != 2*(cfg.Cloud.Workers+1) || initializers != 2 {
			t.Fatal("missing deployment coverage")
		}
	}
}
