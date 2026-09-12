package main

import (
	"continuum/internal/experiment"
	"fmt"
	"gopkg.in/yaml.v3"
	"strings"
	"testing"
	"time"
)

type checkedCompose struct {
	Services map[string]struct {
		Environment   map[string]string
		Command       []string
		Image         string
		Entrypoint    []string
		ContainerName string `yaml:"container_name"`
		Restart       string
		Ports         []string
		Networks      []string
		DependsOn     map[string]struct{ Condition string } `yaml:"depends_on"`
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
			coordinators := 0
			for _, content := range contents {
				var doc checkedCompose
				if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
					t.Fatal(err)
				}
				for name, service := range doc.Services {
					env := service.Environment
					if _, exists := env["CLOUD_EXPECTED_EDGE_IDS"]; exists {
						t.Fatal("Cloud membership must not be deployed")
					}
					if name == "partition-coordinator" {
						coordinators++
						if service.ContainerName != name || len(service.Entrypoint) != 1 || service.Entrypoint[0] != "/app/partition-coordinator" || service.Restart != "no" || len(service.Ports) != 0 {
							t.Fatalf("coordinator lifecycle: %+v", service)
						}
						if service.Image != doc.Services["edge-0"].Image || env["KAFKA_BROKER"] != doc.Services["edge-0"].Environment["KAFKA_BROKER"] || env["KAFKA_TOPIC"] != "edge-aggregates" {
							t.Fatal("coordinator must share Edge image and Kafka target")
						}
						var expected []string
						for _, edge := range edges {
							expected = append(expected, edge.EdgeID)
						}
						if env["COORDINATOR_EXPECTED_EDGE_IDS"] != strings.Join(expected, ",") {
							t.Fatalf("producer mapping: %v", env)
						}
					}
					if strings.HasPrefix(name, "edge-") {
						if env["EDGE_COMPLETION_URL"] != "http://partition-coordinator:8081/completed" || service.DependsOn["partition-coordinator"].Condition != "service_healthy" {
							t.Fatalf("Edge completion contract: %+v", service)
						}
						shared := false
						for _, network := range service.Networks {
							for _, coordinatorNetwork := range doc.Services["partition-coordinator"].Networks {
								if network == coordinatorNetwork {
									shared = true
								}
							}
						}
						if !shared {
							t.Fatal("Edge cannot reach coordinator")
						}
					}
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
						if env["CLOUD_WATERMARK_DELAY"] != "5m0s" {
							t.Fatalf("watermark delay: %v", env)
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
			if globals != 2 || workerInstances != 2*workers || coordinators != 2 {
				t.Fatalf("local/distributed instances: Global=%d Worker=%d", globals, workerInstances)
			}
		})
	}
}

func TestExplicitZeroWatermarkRendered(t *testing.T) {
	zero := experiment.Duration(0)
	cfg := experiment.Config{Cloud: experiment.CloudConfig{Workers: 1, WatermarkDelay: &zero}}
	contents := []string{buildCompose(nil, experiment.BuildEffective(cfg, time.Now()))}
	for _, file := range buildDistributedComposes(nil, cfg) {
		contents = append(contents, file.Content)
	}
	for _, content := range contents {
		var doc checkedCompose
		if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
			t.Fatal(err)
		}
		for name, service := range doc.Services {
			if strings.HasPrefix(name, "cloud-worker-") && service.Environment["CLOUD_WATERMARK_DELAY"] != "0s" {
				t.Fatal("explicit zero lost")
			}
		}
	}
}
