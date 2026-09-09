package main

import (
	"continuum/internal/cloudworker"
	"continuum/internal/experiment"
	"fmt"
	"gopkg.in/yaml.v3"
	"strings"
	"testing"
	"time"
)

type checkedCompose struct {
	Services map[string]struct {
		Environment map[string]string
		Command     []string
	}
}

func TestGeneratedCloudGlobalContractsAllWorkerCounts(t *testing.T) {
	var edges []EdgeDeployment
	for i := 0; i < 13; i++ {
		edges = append(edges, EdgeDeployment{EdgeID: fmt.Sprintf("edge-%d", i), EdgeNumber: i, SensorCount: 1, MQTTPort: 18830 + i})
	}
	for _, workers := range []int{1, 2, 4, 6} {
		t.Run(fmt.Sprintf("W%d", workers), func(t *testing.T) {
			cfg, err := experiment.Load(fmt.Sprintf("../../experiments/cloud-scale-w%d.yaml", workers))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Cloud.Workers != workers || cfg.Cloud.WindowSize.Duration() != 15*time.Minute || cfg.Edge.WindowSize.Duration() != 5*time.Minute {
				t.Fatal("experiment strategy changed")
			}
			local := buildCompose(edges, experiment.BuildEffective(cfg, time.Now().UTC()))
			generated := buildDistributedComposes(edges, cfg)
			contents := []string{local}
			for _, file := range generated {
				contents = append(contents, file.Content)
			}
			workerInstances := 0
			globals := 0
			for _, content := range contents {
				var doc checkedCompose
				if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
					t.Fatal(err)
				}
				for name, service := range doc.Services {
					env := service.Environment
					if name == "global-aggregator" {
						globals++
						if env["SOURCE_PARTITION_COUNT"] != "6" || env["KAFKA_INPUT_TOPIC"] != "cloud-partition-aggregates" {
							t.Fatalf("Global config: %v", env)
						}
						for key := range env {
							if strings.Contains(key, "EDGE") || strings.Contains(key, "WATERMARK") || strings.Contains(key, "WINDOW_SIZE") {
								t.Fatalf("Global knows Edge or window policy: %s", key)
							}
						}
					}
					if strings.HasPrefix(name, "cloud-worker-") {
						workerInstances++
						if env["SOURCE_PARTITION_COUNT"] != "6" || env["KAFKA_OUTPUT_TOPIC"] != "cloud-partition-aggregates" {
							t.Fatalf("Worker config: %v", env)
						}
						m, err := cloudworker.BuildMembership(strings.Split(env["CLOUD_EXPECTED_EDGE_IDS"], ","))
						if err != nil || len(m) != 6 {
							t.Fatalf("membership: %v %v", m, err)
						}
					}
					if name == "kafka-init" {
						command := strings.Join(service.Command, " ")
						if !strings.Contains(command, "--partitions 6") || strings.Contains(command, "KAFKA_PARTITIONS") {
							t.Fatal("input partition count is not fixed")
						}
					}
				}
			}
			if globals != 2 || workerInstances != 2*workers {
				t.Fatalf("local/distributed instances: Global=%d Worker=%d", globals, workerInstances)
			}
		})
	}
}
