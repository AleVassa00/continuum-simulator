package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"continuum/internal/experiment"
)

func TestBuildComposeMatchesExpectedOutput(t *testing.T) {
	compose := buildCompose(testEdges(2), testEffectiveConfig(2))
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(compose)))
	const expectedChecksum = "5b5bdc83523efa38ce5ce428c22fac97ae4699982e002fe01c62bc823c9a38fc"

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
		"GLOBAL_WATERMARK_DELAY: \"15m0s\"",
		"GLOBAL_EDGE_IDLE_TIMEOUT: \"5s\"",
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
		"${GLOBAL_EDGE_IDLE_TIMEOUT",
	} {
		if strings.Contains(compose, forbidden) {
			t.Errorf("override esterno sperimentale rimasto nel Compose: %s", forbidden)
		}
	}
}

func TestBuildDistributedComposesSeparatesHostsAndUsesRuntimeAddresses(t *testing.T) {
	composes := buildDistributedComposes(testEdges(13), testConfig(3))

	if len(composes) != 4 {
		t.Fatalf("Compose distributed=%d, attesi 4", len(composes))
	}

	cloudCore := distributedComposeContent(t, composes, cloudCoreComposeFilename)
	workers := distributedComposeContent(t, composes, workersComposeFilename)
	edge := distributedComposeContent(t, composes, edgeComposeFilename)
	simulator := distributedComposeContent(t, composes, simulatorComposeFilename)

	for _, expected := range []string{
		"  kafka:\n",
		"  kafka-init:\n",
		"  global-aggregator:\n",
		"image: \"continuum-kafka:${DEPLOYMENT_ID:?DEPLOYMENT_ID is required}\"",
		"image: \"continuum-global-aggregator:${DEPLOYMENT_ID:?DEPLOYMENT_ID is required}\"",
		"INTERNAL://kafka:29092",
		"EXTERNAL://${KAFKA_ADVERTISED_HOST:?KAFKA_ADVERTISED_HOST is required}:9092",
		"--topic edge-aggregates",
		"--partitions ${KAFKA_PARTITIONS:-6}",
		"--topic cloud-edge-aggregates",
		"--partitions 1",
		"KAFKA_BROKER: \"kafka:29092\"",
	} {
		if !strings.Contains(cloudCore, expected) {
			t.Errorf("cloud core senza %q", expected)
		}
	}
	advertisedLine := composeLineContaining(t, cloudCore, "KAFKA_ADVERTISED_LISTENERS:")
	if strings.Contains(advertisedLine, "localhost:9092") {
		t.Errorf("Kafka distributed pubblicizza localhost: %s", advertisedLine)
	}
	for _, forbidden := range []string{
		"continuum-cloud-worker:local",
		"continuum-edge:local",
		"continuum-simulator:local",
		"continuum-mosquitto:",
	} {
		if strings.Contains(cloudCore, forbidden) {
			t.Errorf("cloud core contiene servizio di un altro host: %s", forbidden)
		}
	}

	if count := strings.Count(workers, "    image: \"continuum-cloud-worker:${DEPLOYMENT_ID:?DEPLOYMENT_ID is required}\"\n"); count != 3 {
		t.Fatalf("Cloud Worker distributed=%d, attesi 3", count)
	}
	for workerNumber := 0; workerNumber < 3; workerNumber++ {
		workerID := fmt.Sprintf("cloud-worker-%d", workerNumber)
		worker := composeServiceBlock(t, workers, workerID)
		for _, expected := range []string{
			"KAFKA_BROKER: \"${CLOUD_KAFKA_HOST}:9092\"",
			"KAFKA_GROUP_ID: \"cloud-workers\"",
			"WORKER_ID: \"" + workerID + "\"",
			"restart: \"no\"",
		} {
			if !strings.Contains(worker, expected) {
				t.Errorf("Cloud Worker %s senza %q", workerID, expected)
			}
		}
	}
	if strings.Contains(workers, "depends_on:") {
		t.Error("workers contiene depends_on cross-host")
	}
	for _, forbidden := range []string{
		"  kafka:\n",
		"  kafka-init:\n",
		"global-aggregator",
		"continuum-edge:local",
		"continuum-simulator:local",
	} {
		if strings.Contains(workers, forbidden) {
			t.Errorf("workers contiene servizio di un altro host: %q", forbidden)
		}
	}

	if count := strings.Count(edge, "    image: \"continuum-edge:${DEPLOYMENT_ID:?DEPLOYMENT_ID is required}\"\n"); count != 13 {
		t.Fatalf("Edge distributed=%d, attesi 13", count)
	}
	if count := strings.Count(edge, "    image: \"continuum-mosquitto:${DEPLOYMENT_ID:?DEPLOYMENT_ID is required}\"\n"); count != 13 {
		t.Fatalf("Mosquitto distributed=%d, attesi 13", count)
	}
	for _, edgeDeployment := range testEdges(13) {
		edgeID := edgeDeployment.EdgeID
		mqtt := composeServiceBlock(t, edge, "mqtt-"+edgeID)
		if expected := fmt.Sprintf("- \"%d:1883\"", edgeDeployment.MQTTPort); !strings.Contains(mqtt, expected) {
			t.Errorf("Mosquitto %s senza %q", edgeID, expected)
		}

		edgeService := composeServiceBlock(t, edge, edgeID)
		for _, expected := range []string{
			"MQTT_BROKER: \"tcp://mqtt-" + edgeID + ":1883\"",
			"KAFKA_BROKER: \"${CLOUD_KAFKA_HOST}:9092\"",
			"http://localhost:8080/readyz",
			"restart: \"no\"",
		} {
			if !strings.Contains(edgeService, expected) {
				t.Errorf("Edge %s senza %q", edgeID, expected)
			}
		}
		if strings.Contains(edgeService, "kafka-init") || strings.Contains(edgeService, "      kafka:") {
			t.Errorf("Edge %s contiene depends_on verso Kafka remoto", edgeID)
		}
	}
	for _, forbidden := range []string{
		"  kafka:\n",
		"  kafka-init:\n",
		"continuum-cloud-worker:local",
		"continuum-simulator:local",
		"global-aggregator",
	} {
		if strings.Contains(edge, forbidden) {
			t.Errorf("edge Compose contiene servizio di un altro host: %q", forbidden)
		}
	}

	if count := strings.Count(simulator, "    image: \"continuum-simulator:${DEPLOYMENT_ID:?DEPLOYMENT_ID is required}\"\n"); count != 13 {
		t.Fatalf("Simulator distributed=%d, attesi 13", count)
	}
	for _, edgeDeployment := range testEdges(13) {
		edgeID := edgeDeployment.EdgeID
		simulatorService := composeServiceBlock(t, simulator, "simulator-"+edgeID)
		for _, expected := range []string{
			"SITE_ID: \"" + edgeID + "\"",
			fmt.Sprintf("MQTT_ENDPOINT: \"tcp://${EDGE_HOST}:%d\"", edgeDeployment.MQTTPort),
			"REPLAY_FILE: \"/app/dataset/derived/replay_by_edge/" + edgeID + ".csv\"",
			"REPLAY_START_AT: \"${REPLAY_START_AT:?REPLAY_START_AT is required}\"",
		} {
			if !strings.Contains(simulatorService, expected) {
				t.Errorf("Simulator %s senza %q", edgeID, expected)
			}
		}
	}
	for _, forbidden := range []string{
		"depends_on:",
		"2026-08-31T12:00:10Z",
		"continuum-edge:local",
		"eclipse-mosquitto:2",
		"  kafka:\n",
	} {
		if strings.Contains(simulator, forbidden) {
			t.Errorf("simulator Compose contiene valore o servizio vietato: %q", forbidden)
		}
	}
}

func TestRunDeploygenDistributedWritesComposesAndManifestWithoutMaterializingReplayStart(t *testing.T) {
	temp := t.TempDir()
	experimentPath := filepath.Join(temp, "custom.yaml")
	topologyPath := filepath.Join(temp, "topology.csv")
	distributedOutputDir := filepath.Join(temp, "distributed")
	localOutputPath := filepath.Join(temp, "local.yml")
	artifactsRoot := filepath.Join(temp, "artifacts")

	writeTestFile(t, experimentPath, `experiment:
  name: distributed-test
workload:
  acceleration_factor: 1000
  max_events: 10
  start_lead_time: 90s
simulator:
  telemetry_queue_capacity: 1000
edge:
  window_size: 5m
  ingress_queue_capacity: 1000
cloud:
  workers: 6
  window_size: 15m
global: {}
`)
	writeTestTopology(t, topologyPath, 13)

	var output bytes.Buffer
	err := runDeploygen(
		[]string{"--mode", distributedMode, "--experiment", experimentPath},
		deploygenOptions{
			TopologyPath:         topologyPath,
			OutputPath:           localOutputPath,
			DistributedOutputDir: distributedOutputDir,
			ArtifactsRoot:        artifactsRoot,
			Now: func() time.Time {
				t.Fatal("la modalita distributed non deve calcolare REPLAY_START_AT")
				return time.Time{}
			},
			Stdout: &output,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(distributedOutputDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("file distributed=%d, attesi 5", len(entries))
	}
	for _, filename := range []string{
		cloudCoreComposeFilename,
		workersComposeFilename,
		edgeComposeFilename,
		simulatorComposeFilename,
	} {
		if _, err := os.Stat(filepath.Join(distributedOutputDir, filename)); err != nil {
			t.Errorf("file %s non generato: %v", filename, err)
		}
	}
	manifestPayload := readTestFile(t, filepath.Join(distributedOutputDir, generationManifestFilename))
	var manifest composeGenerationManifest
	if err := json.Unmarshal([]byte(manifestPayload), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 1 ||
		len(manifest.ConfigSHA256) != 64 ||
		len(manifest.TopologySHA256) != 64 {
		t.Fatalf("generation manifest inatteso: %#v", manifest)
	}
	loadedConfig, err := experiment.Load(experimentPath)
	if err != nil {
		t.Fatal(err)
	}
	expectedConfigSHA256, err := experiment.Fingerprint(loadedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ConfigSHA256 != expectedConfigSHA256 {
		t.Fatalf("config SHA=%s, atteso %s", manifest.ConfigSHA256, expectedConfigSHA256)
	}
	topologyPayload, err := os.ReadFile(topologyPath)
	if err != nil {
		t.Fatal(err)
	}
	expectedTopologySHA256 := fmt.Sprintf("%x", sha256.Sum256(topologyPayload))
	if manifest.TopologySHA256 != expectedTopologySHA256 {
		t.Fatalf("topology SHA=%s, atteso %s", manifest.TopologySHA256, expectedTopologySHA256)
	}
	if len(manifest.ComposeSHA256) != 4 {
		t.Fatalf("checksum Compose=%d, attesi 4", len(manifest.ComposeSHA256))
	}
	if strings.Contains(manifest.ResolvedConfigYAML, "replay_start_at") {
		t.Fatal("generation manifest contiene REPLAY_START_AT prematuro")
	}
	for filename, expectedDigest := range manifest.ComposeSHA256 {
		payload := readTestFile(t, filepath.Join(distributedOutputDir, filename))
		actualDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(payload)))
		if actualDigest != expectedDigest {
			t.Errorf("checksum %s=%s, atteso %s", filename, actualDigest, expectedDigest)
		}
	}
	if _, err := os.Stat(localOutputPath); !os.IsNotExist(err) {
		t.Errorf("output locale generato in modalita distributed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(artifactsRoot, "distributed-test", "effective-config.yaml")); !os.IsNotExist(err) {
		t.Errorf("effective config materializzata senza vero REPLAY_START_AT: %v", err)
	}

	workers := readTestFile(t, filepath.Join(distributedOutputDir, workersComposeFilename))
	if count := strings.Count(workers, "    image: \"continuum-cloud-worker:${DEPLOYMENT_ID:?DEPLOYMENT_ID is required}\"\n"); count != 6 {
		t.Fatalf("Cloud Worker=%d, attesi 6", count)
	}
	simulator := readTestFile(t, filepath.Join(distributedOutputDir, simulatorComposeFilename))
	if !strings.Contains(simulator, "${REPLAY_START_AT:?REPLAY_START_AT is required}") {
		t.Fatal("REPLAY_START_AT non e richiesto a runtime")
	}
	if strings.Contains(simulator, "0001-01-01") {
		t.Fatal("trovato timestamp fittizio nel Compose distributed")
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
global:
  watermark_delay: 3m
  edge_idle_timeout: 7s
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
		"GLOBAL_WATERMARK_DELAY: \"3m0s\"",
		"GLOBAL_EDGE_IDLE_TIMEOUT: \"7s\"",
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
		"watermark_delay: 3m0s",
		"edge_idle_timeout: 7s",
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
		"edge idle timeout: 7s",
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
		Global: experiment.GlobalConfig{
			WatermarkDelay:  experiment.Duration(15 * time.Minute),
			EdgeIdleTimeout: experiment.Duration(5 * time.Second),
		},
	}
}

func testConfig(workers int) experiment.Config {
	return experiment.Config{
		Experiment: experiment.ExperimentConfig{Name: "test"},
		Workload: experiment.WorkloadConfig{
			AccelerationFactor: 2500,
			MaxEvents:          42,
			StartLeadTime:      experiment.Duration(10 * time.Second),
		},
		Simulator: experiment.SimulatorConfig{
			TelemetryQueueCapacity: 321,
			StartLateTolerance:     experiment.Duration(10 * time.Second),
		},
		Edge: experiment.EdgeConfig{
			WindowSize:           experiment.Duration(5 * time.Minute),
			IngressQueueCapacity: 654,
		},
		Cloud: experiment.CloudConfig{
			Workers:    workers,
			WindowSize: experiment.Duration(15 * time.Minute),
		},
		Global: experiment.GlobalConfig{
			WatermarkDelay:  experiment.Duration(15 * time.Minute),
			EdgeIdleTimeout: experiment.Duration(5 * time.Second),
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

func distributedComposeContent(
	t *testing.T,
	composes []generatedCompose,
	filename string,
) string {
	t.Helper()
	for _, compose := range composes {
		if compose.Filename == filename {
			return compose.Content
		}
	}
	t.Fatalf("Compose %s non trovato", filename)
	return ""
}

func composeLineContaining(t *testing.T, compose string, value string) string {
	t.Helper()
	for _, line := range strings.Split(compose, "\n") {
		if strings.Contains(line, value) {
			return line
		}
	}
	t.Fatalf("riga contenente %q non trovata", value)
	return ""
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
