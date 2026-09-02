package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"continuum/internal/experiment"
)

func TestBuildComposeMatchesPreRefactorOutput(t *testing.T) {
	compose := buildCompose(testEdges(2), testEffectiveConfig(2))
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(compose)))
	const expectedChecksum = "520efbc0ccdeee2e01ec06614775841a6733f456db0cd2467b1716ed9a3ca30f"

	if checksum != expectedChecksum {
		t.Fatalf(
			"Compose diverso dalla baseline byte-for-byte: checksum=%s",
			checksum,
		)
	}

	for _, expected := range []string{
		"  kafka:\n",
		"  kafka-init:\n",
		"  cloud-worker-0:\n",
		"  global-aggregator:\n",
		"  mqtt-edge-0:\n",
		"  edge-0:\n",
		"  simulator-edge-0:\n",
		"networks:\n",
		"volumes:\n  kafka-data:\n",
		"REPLAY_START_AT: \"2026-08-31T12:00:10Z\"",
		"ACCELERATION_FACTOR: \"2500\"",
		"TELEMETRY_QUEUE_CAPACITY: \"321\"",
		"EDGE_INGRESS_QUEUE_CAPACITY: \"654\"",
	} {
		if !strings.Contains(compose, expected) {
			t.Errorf("Compose senza %q", expected)
		}
	}
}

func TestBuildComposeUsesEffectiveExperimentConfig(t *testing.T) {
	edges := testEdges(13)
	effective := testEffectiveConfig(2)
	compose := buildCompose(edges, effective)

	if count := strings.Count(compose, "    image: continuum-simulator:local\n"); count != 13 {
		t.Fatalf("istanze Simulator=%d, attese 13", count)
	}
	if count := strings.Count(compose, "    image: continuum-edge:local\n"); count != 13 {
		t.Fatalf("istanze Edge=%d, attese 13", count)
	}
	if count := strings.Count(compose, "    image: eclipse-mosquitto:2\n"); count != 13 {
		t.Fatalf("istanze Mosquitto=%d, attese 13", count)
	}
	if count := strings.Count(compose, "    image: continuum-cloud-worker:local\n"); count != 2 {
		t.Fatalf("istanze Cloud Worker=%d, attese 2", count)
	}
	if count := strings.Count(compose, "    image: continuum-global-aggregator:local\n"); count != 1 {
		t.Fatalf("istanze Global Aggregator=%d, attesa 1", count)
	}

	globalSimulatorEnvironment := []string{
		"REPLAY_EPOCH: \"2025-01-01T00:00:00Z\"",
		"REPLAY_START_AT: \"2026-08-31T12:00:10Z\"",
		"ACCELERATION_FACTOR: \"2500\"",
		"MAX_EVENTS: \"42\"",
		"TELEMETRY_QUEUE_CAPACITY: \"321\"",
	}
	for _, expected := range globalSimulatorEnvironment {
		if count := strings.Count(compose, "      "+expected+"\n"); count != 13 {
			t.Errorf("configurazione globale %q=%d, attesa 13", expected, count)
		}
	}

	for _, edgeDeployment := range edges {
		edgeID := edgeDeployment.EdgeID
		simulator := composeServiceBlock(t, compose, "simulator-"+edgeID)
		for _, expected := range []string{
			"SITE_ID: \"" + edgeID + "\"",
			"MQTT_ENDPOINT: \"tcp://mqtt-" + edgeID + ":1883\"",
			"REPLAY_FILE: \"/app/dataset/derived/replay_by_edge/" + edgeID + ".csv\"",
			"- zone-" + edgeID,
		} {
			if !strings.Contains(simulator, expected) {
				t.Errorf("Simulator %s senza %q", edgeID, expected)
			}
		}

		edge := composeServiceBlock(t, compose, edgeID)
		for _, expected := range []string{
			"WINDOW_SIZE: \"5m0s\"",
			"EDGE_INGRESS_QUEUE_CAPACITY: \"654\"",
			"http://localhost:8080/readyz",
		} {
			if !strings.Contains(edge, expected) {
				t.Errorf("Edge %s senza %q", edgeID, expected)
			}
		}
	}

	for workerNumber := 0; workerNumber < 2; workerNumber++ {
		workerID := fmt.Sprintf("cloud-worker-%d", workerNumber)
		worker := composeServiceBlock(t, compose, workerID)
		for _, expected := range []string{
			"KAFKA_GROUP_ID: \"cloud-workers\"",
			"CLOUD_WINDOW_SIZE: \"15m0s\"",
			"WORKER_ID: \"" + workerID + "\"",
		} {
			if !strings.Contains(worker, expected) {
				t.Errorf("Cloud Worker %s senza %q", workerID, expected)
			}
		}
	}

	global := composeServiceBlock(t, compose, "global-aggregator")
	for _, expected := range []string{
		"KAFKA_INPUT_TOPIC: \"cloud-edge-aggregates\"",
		"KAFKA_GROUP_ID: \"global-aggregator\"",
		"GLOBAL_WINDOW_SIZE: \"15m0s\"",
		"EXPECTED_EDGE_IDS: \"edge-0,edge-1,edge-2,edge-3,edge-4,edge-5,edge-6,edge-7,edge-8,edge-9,edge-10,edge-11,edge-12\"",
		"restart: \"no\"",
	} {
		if !strings.Contains(global, expected) {
			t.Errorf("Global Aggregator senza %q", expected)
		}
	}

	for _, forbidden := range []string{
		"${ACCELERATION_FACTOR",
		"${MAX_EVENTS",
		"${REPLAY_START_AT",
		"${TELEMETRY_QUEUE_CAPACITY",
		"${EDGE_WINDOW_SIZE",
		"${EDGE_INGRESS_QUEUE_CAPACITY",
		"${CLOUD_WINDOW_SIZE",
	} {
		if strings.Contains(compose, forbidden) {
			t.Errorf("override esterno sperimentale rimasto nel Compose: %s", forbidden)
		}
	}
}

func TestRunDeploygenLoadsYAMLAndWritesEffectiveConfig(t *testing.T) {
	temp := t.TempDir()
	experimentPath := filepath.Join(temp, "custom.yaml")
	topologyPath := filepath.Join(temp, "topology.csv")
	outputPath := filepath.Join(temp, "compose.yml")
	artifactsRoot := filepath.Join(temp, "artifacts")

	writeTestFile(t, experimentPath, `experiment:
  name: custom
workload:
  acceleration_factor: 77.5
  max_events: 9
  start_lead_time: 12s
simulator:
  telemetry_queue_capacity: 111
edge:
  window_size: 5m
  ingress_queue_capacity: 222
cloud:
  workers: 2
  window_size: 20m
`)
	writeTestTopology(t, topologyPath, 13)

	t.Setenv("ACCELERATION_FACTOR", "999999")
	t.Setenv("MAX_EVENTS", "999999")
	t.Setenv("TELEMETRY_QUEUE_CAPACITY", "999999")
	t.Setenv("EDGE_INGRESS_QUEUE_CAPACITY", "999999")

	var output bytes.Buffer
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	err := runDeploygen(
		[]string{"--experiment", experimentPath},
		deploygenOptions{
			TopologyPath:  topologyPath,
			OutputPath:    outputPath,
			ArtifactsRoot: artifactsRoot,
			Now:           func() time.Time { return now },
			Stdout:        &output,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	compose := readTestFile(t, outputPath)
	for _, expected := range []string{
		"ACCELERATION_FACTOR: \"77.5\"",
		"MAX_EVENTS: \"9\"",
		"REPLAY_START_AT: \"2026-08-31T12:00:12Z\"",
		"TELEMETRY_QUEUE_CAPACITY: \"111\"",
		"EDGE_INGRESS_QUEUE_CAPACITY: \"222\"",
		"WINDOW_SIZE: \"5m0s\"",
		"CLOUD_WINDOW_SIZE: \"20m0s\"",
		"GLOBAL_WINDOW_SIZE: \"20m0s\"",
		"EXPECTED_EDGE_IDS: \"edge-0,edge-1,edge-2,edge-3,edge-4,edge-5,edge-6,edge-7,edge-8,edge-9,edge-10,edge-11,edge-12\"",
	} {
		if !strings.Contains(compose, expected) {
			t.Errorf("Compose senza %q", expected)
		}
	}
	if strings.Contains(compose, "999999") {
		t.Fatal("una environment esterna ha sovrascritto lo YAML")
	}
	if count := strings.Count(compose, "    image: continuum-cloud-worker:local\n"); count != 2 {
		t.Fatalf("Cloud Worker=%d, attesi 2", count)
	}
	if count := strings.Count(compose, "    image: continuum-global-aggregator:local\n"); count != 1 {
		t.Fatalf("Global Aggregator=%d, atteso 1", count)
	}

	effectivePath := filepath.Join(artifactsRoot, "custom", "effective-config.yaml")
	effective := readTestFile(t, effectivePath)
	for _, expected := range []string{
		"name: custom",
		"acceleration_factor: 77.5",
		"replay_start_at: \"2026-08-31T12:00:12Z\"",
		"workers: 2",
	} {
		if !strings.Contains(effective, expected) {
			t.Errorf("effective config senza %q:\n%s", expected, effective)
		}
	}

	summary := output.String()
	for _, expected := range []string{
		"Experiment: custom",
		"replay start at: 2026-08-31T12:00:12Z",
		"telemetry queue capacity: 111",
		"ingress queue capacity: 222",
		"workers: 2",
	} {
		if !strings.Contains(summary, expected) {
			t.Errorf("summary senza %q:\n%s", expected, summary)
		}
	}
}

func testEffectiveConfig(workers int) experiment.EffectiveConfig {
	return experiment.EffectiveConfig{
		Experiment: experiment.ExperimentConfig{Name: "test"},
		Workload: experiment.EffectiveWorkloadConfig{
			AccelerationFactor: 2500,
			MaxEvents:          42,
			StartLeadTime:      experiment.Duration(10 * time.Second),
			ReplayStartAt:      "2026-08-31T12:00:10Z",
		},
		Simulator: experiment.SimulatorConfig{TelemetryQueueCapacity: 321},
		Edge: experiment.EdgeConfig{
			WindowSize:           experiment.Duration(5 * time.Minute),
			IngressQueueCapacity: 654,
		},
		Cloud: experiment.CloudConfig{
			Workers:    workers,
			WindowSize: experiment.Duration(15 * time.Minute),
		},
	}
}

func testEdges(count int) []EdgeDeployment {
	edges := make([]EdgeDeployment, 0, count)
	for edgeNumber := 0; edgeNumber < count; edgeNumber++ {
		edges = append(edges, EdgeDeployment{
			EdgeID:      fmt.Sprintf("edge-%d", edgeNumber),
			EdgeNumber:  edgeNumber,
			SensorCount: 11,
			MQTTPort:    mqttBasePort + edgeNumber,
		})
	}
	return edges
}

func writeTestTopology(t *testing.T, path string, edges int) {
	t.Helper()
	var builder strings.Builder
	builder.WriteString("sensor_id,edge_id\n")
	for edge := 0; edge < edges; edge++ {
		fmt.Fprintf(&builder, "sensor-%d,edge-%d\n", edge, edge)
	}
	writeTestFile(t, path, builder.String())
}

func writeTestFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func composeServiceBlock(
	t *testing.T,
	compose string,
	service string,
) string {
	t.Helper()

	lines := strings.Split(compose, "\n")
	start := -1

	for index, line := range lines {
		if line == "  "+service+":" {
			start = index
			break
		}
	}

	if start == -1 {
		t.Fatalf("servizio %s non trovato", service)
	}

	end := len(lines)
	for index := start + 1; index < len(lines); index++ {
		line := lines[index]
		isNextService := strings.HasPrefix(line, "  ") &&
			!strings.HasPrefix(line, "    ")
		isTopLevel := line != "" &&
			!strings.HasPrefix(line, " ")

		if strings.TrimSpace(line) != "" &&
			(isNextService || isTopLevel) {
			end = index
			break
		}
	}

	return strings.Join(lines[start:end], "\n")
}
