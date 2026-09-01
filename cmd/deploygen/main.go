package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"continuum/internal/experiment"
)

const (
	defaultTopologyPath   = "dataset/output/kmeans_topology.csv"
	defaultOutputPath     = "deploy/compose/continuum.generated.yml"
	defaultExperimentPath = "experiments/baseline.yaml"
	defaultArtifactsRoot  = "artifacts/experiments"
	deploymentReplayEpoch = "2025-01-01T00:00:00Z"

	mqttBasePort = 18830
)

type EdgeDeployment struct {
	EdgeID      string
	EdgeNumber  int
	SensorCount int
	MQTTPort    int
}

type deploygenOptions struct {
	TopologyPath  string
	OutputPath    string
	ArtifactsRoot string
	Now           func() time.Time
	Stdout        io.Writer
}

func main() {
	if err := runDeploygen(os.Args[1:], deploygenOptions{
		TopologyPath:  defaultTopologyPath,
		OutputPath:    defaultOutputPath,
		ArtifactsRoot: defaultArtifactsRoot,
		Now:           time.Now,
		Stdout:        os.Stdout,
	}); err != nil {
		panic(err)
	}
}

func runDeploygen(args []string, options deploygenOptions) error {
	flags := flag.NewFlagSet("deploygen", flag.ContinueOnError)
	flags.SetOutput(options.Stdout)
	experimentPath := flags.String(
		"experiment",
		defaultExperimentPath,
		"percorso della configurazione YAML dell'esperimento",
	)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("argomenti posizionali non supportati: %s", strings.Join(flags.Args(), " "))
	}

	config, err := experiment.Load(*experimentPath)
	if err != nil {
		return err
	}
	edges, err := loadEdges(options.TopologyPath)
	if err != nil {
		return err
	}
	if len(edges) == 0 {
		return fmt.Errorf("nessun Edge trovato nella topologia")
	}

	replayStartAt := options.Now().UTC().Add(
		config.Workload.StartLeadTime.Duration(),
	)
	effective := experiment.BuildEffective(config, replayStartAt)
	compose := buildCompose(edges, effective)

	if err := os.MkdirAll(filepath.Dir(options.OutputPath), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(options.OutputPath, []byte(compose), 0644); err != nil {
		return err
	}

	effectivePath := filepath.Join(
		options.ArtifactsRoot,
		config.Experiment.Name,
		"effective-config.yaml",
	)
	if err := experiment.WriteEffective(effectivePath, effective); err != nil {
		return err
	}

	printExperimentSummary(options.Stdout, effective, effectivePath)

	fmt.Fprintf(
		options.Stdout,
		"Topologia letta: %d Edge\n\n",
		len(edges),
	)

	for _, edge := range edges {
		fmt.Fprintf(
			options.Stdout,
			"%s -> sensors=%d mqtt=tcp://mqtt-%s:1883 host_port=%d simulator=simulator-%s\n",
			edge.EdgeID,
			edge.SensorCount,
			edge.EdgeID,
			edge.MQTTPort,
			edge.EdgeID,
		)
	}

	fmt.Fprintf(
		options.Stdout,
		"\nGenerato: %s\n",
		options.OutputPath,
	)

	return nil
}

func printExperimentSummary(
	output io.Writer,
	config experiment.EffectiveConfig,
	effectivePath string,
) {
	fmt.Fprintf(output, "Experiment: %s\n\n", config.Experiment.Name)
	fmt.Fprintln(output, "Workload:")
	fmt.Fprintf(output, "  acceleration factor: %s\n", formatFloat(config.Workload.AccelerationFactor))
	fmt.Fprintf(output, "  max events: %d\n", config.Workload.MaxEvents)
	fmt.Fprintf(output, "  replay start lead time: %s\n", config.Workload.StartLeadTime)
	fmt.Fprintf(output, "  replay start at: %s\n\n", config.Workload.ReplayStartAt)
	fmt.Fprintln(output, "Simulator:")
	fmt.Fprintf(output, "  telemetry queue capacity: %d\n\n", config.Simulator.TelemetryQueueCapacity)
	fmt.Fprintln(output, "Edge:")
	fmt.Fprintf(output, "  ingress queue capacity: %d\n", config.Edge.IngressQueueCapacity)
	fmt.Fprintf(output, "  window: %s\n\n", config.Edge.WindowSize)
	fmt.Fprintln(output, "Cloud:")
	fmt.Fprintf(output, "  workers: %d\n", config.Cloud.Workers)
	fmt.Fprintf(output, "  window: %s\n\n", config.Cloud.WindowSize)
	fmt.Fprintf(output, "Effective config: %s\n\n", effectivePath)
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func loadEdges(
	path string,
) ([]EdgeDeployment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','

	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := buildColumnIndex(header)

	edgeIDIndex, err := requiredColumn(
		columns,
		"edge_id",
	)
	if err != nil {
		return nil, err
	}

	sensorCounts := make(
		map[string]int,
	)

	for {
		row, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		edgeID := strings.TrimSpace(
			row[edgeIDIndex],
		)

		if edgeID == "" {
			return nil,
				fmt.Errorf(
					"edge_id vuoto nella topologia",
				)
		}

		sensorCounts[edgeID]++
	}

	edges := make(
		[]EdgeDeployment,
		0,
		len(sensorCounts),
	)

	for edgeID, sensorCount := range sensorCounts {
		edgeNumber, err := parseEdgeNumber(
			edgeID,
		)
		if err != nil {
			return nil, err
		}

		edges = append(
			edges,
			EdgeDeployment{
				EdgeID:      edgeID,
				EdgeNumber:  edgeNumber,
				SensorCount: sensorCount,
				MQTTPort:    mqttBasePort + edgeNumber,
			},
		)
	}

	sort.Slice(
		edges,
		func(i int, j int) bool {
			return edges[i].EdgeNumber <
				edges[j].EdgeNumber
		},
	)

	return edges, nil
}

func parseEdgeNumber(
	edgeID string,
) (int, error) {
	const prefix = "edge-"

	if !strings.HasPrefix(
		edgeID,
		prefix,
	) {
		return 0,
			fmt.Errorf(
				"edge_id non valido %q",
				edgeID,
			)
	}

	value := strings.TrimPrefix(
		edgeID,
		prefix,
	)

	edgeNumber, err := strconv.Atoi(
		value,
	)
	if err != nil {
		return 0,
			fmt.Errorf(
				"edge_id non valido %q: %w",
				edgeID,
				err,
			)
	}

	if edgeNumber < 0 {
		return 0,
			fmt.Errorf(
				"numero Edge negativo in %q",
				edgeID,
			)
	}

	return edgeNumber, nil
}

func buildColumnIndex(
	header []string,
) map[string]int {
	columns := make(
		map[string]int,
	)

	for index, name := range header {
		columns[strings.TrimSpace(name)] = index
	}

	return columns
}

func requiredColumn(
	columns map[string]int,
	name string,
) (int, error) {
	index, found := columns[name]

	if !found {
		return 0,
			fmt.Errorf(
				"colonna %q non trovata",
				name,
			)
	}

	return index, nil
}

func buildCompose(
	edges []EdgeDeployment,
	config experiment.EffectiveConfig,
) string {
	var builder strings.Builder

	builder.WriteString(
		"# Code generated by cmd/deploygen. DO NOT EDIT.\n",
	)

	builder.WriteString(
		"# Source: dataset/output/kmeans_topology.csv\n",
	)

	fmt.Fprintf(
		&builder,
		"# Experiment: %s\n\n",
		config.Experiment.Name,
	)

	builder.WriteString(
		"services:\n\n",
	)

	// Kafka
	builder.WriteString(
		`  kafka:
    image: apache/kafka:4.3.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"

      KAFKA_LISTENERS: "INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:29092,EXTERNAL://localhost:9092"

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"

      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

    volumes:
      - kafka-data:/var/lib/kafka/data

    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 5s
      timeout: 5s
      retries: 20

    restart: unless-stopped

    networks:
      - continuum-backbone


  kafka-init:
    image: apache/kafka:4.3.0
    container_name: kafka-init

    depends_on:
      kafka:
        condition: service_healthy

    command:
      - /bin/bash
      - -c
      - |
        /opt/kafka/bin/kafka-topics.sh \
          --bootstrap-server kafka:29092 \
          --create \
          --if-not-exists \
          --topic edge-aggregates \
          --partitions ${KAFKA_PARTITIONS:-6} \
          --replication-factor 1

        /opt/kafka/bin/kafka-topics.sh \
          --bootstrap-server kafka:29092 \
          --create \
          --if-not-exists \
          --topic cloud-edge-aggregates \
          --partitions 1 \
          --replication-factor 1

    networks:
      - continuum-backbone

`,
	)
	for workerNumber := 0; workerNumber < config.Cloud.Workers; workerNumber++ {
		workerID := fmt.Sprintf("cloud-worker-%d", workerNumber)
		fmt.Fprintf(
			&builder,
			`  %s:
    image: continuum-cloud-worker:local
    container_name: %s

    environment:
      KAFKA_BROKER: "kafka:29092"
      KAFKA_INPUT_TOPIC: "edge-aggregates"
      KAFKA_OUTPUT_TOPIC: "cloud-edge-aggregates"
      KAFKA_GROUP_ID: "cloud-workers"
      CLOUD_WINDOW_SIZE: "%s"
      WORKER_ID: "%s"

    depends_on:
      kafka-init:
        condition: service_completed_successfully

    restart: unless-stopped

    networks:
      - continuum-backbone

`,
			workerID,
			workerID,
			config.Cloud.WindowSize,
			workerID,
		)
	}

	expectedEdgeIDs := make([]string, len(edges))
	for index, edge := range edges {
		expectedEdgeIDs[index] = edge.EdgeID
	}
	fmt.Fprintf(
		&builder,
		`  global-aggregator:
    image: continuum-global-aggregator:local
    container_name: global-aggregator

    environment:
      KAFKA_BROKER: "kafka:29092"
      KAFKA_INPUT_TOPIC: "cloud-edge-aggregates"
      KAFKA_GROUP_ID: "global-aggregator"
      GLOBAL_WINDOW_SIZE: "%s"
      EXPECTED_EDGE_IDS: "%s"

    depends_on:
      kafka-init:
        condition: service_completed_successfully

    restart: "no"

    networks:
      - continuum-backbone

`,
		config.Cloud.WindowSize,
		strings.Join(expectedEdgeIDs, ","),
	)

	// Zone Edge
	for _, edge := range edges {
		mqttService := "mqtt-" + edge.EdgeID
		simulatorService := "simulator-" + edge.EdgeID
		zoneNetwork := "zone-" + edge.EdgeID

		fmt.Fprintf(
			&builder,
			"  # %s: %d sensors\n",
			edge.EdgeID,
			edge.SensorCount,
		)

		// Mosquitto della zona
		fmt.Fprintf(
			&builder,
			"  %s:\n",
			mqttService,
		)

		builder.WriteString(
			"    image: eclipse-mosquitto:2\n",
		)

		fmt.Fprintf(
			&builder,
			"    container_name: %s\n",
			mqttService,
		)

		builder.WriteString(
			"    ports:\n",
		)

		fmt.Fprintf(
			&builder,
			"      - \"%d:1883\"\n",
			edge.MQTTPort,
		)

		builder.WriteString(
			"    volumes:\n",
		)

		builder.WriteString(
			"      - ../mosquitto/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro\n",
		)

		builder.WriteString(
			"    healthcheck:\n",
		)

		builder.WriteString(
			"      test: [\"CMD\", \"mosquitto_pub\", \"-h\", \"localhost\", \"-p\", \"1883\", \"-t\", \"healthcheck\", \"-m\", \"ping\"]\n",
		)

		builder.WriteString(
			"      interval: 5s\n",
		)

		builder.WriteString(
			"      timeout: 3s\n",
		)

		builder.WriteString(
			"      retries: 10\n",
		)

		builder.WriteString(
			"    restart: unless-stopped\n",
		)

		builder.WriteString(
			"    networks:\n",
		)

		fmt.Fprintf(
			&builder,
			"      - %s\n\n",
			zoneNetwork,
		)

		// Edge della zona
		fmt.Fprintf(
			&builder,
			"  %s:\n",
			edge.EdgeID,
		)

		builder.WriteString(
			"    image: continuum-edge:local\n",
		)

		fmt.Fprintf(
			&builder,
			"    container_name: %s\n",
			edge.EdgeID,
		)

		builder.WriteString(
			"    environment:\n",
		)

		fmt.Fprintf(
			&builder,
			"      EDGE_ID: \"%s\"\n",
			edge.EdgeID,
		)

		fmt.Fprintf(
			&builder,
			"      MQTT_BROKER: \"tcp://%s:1883\"\n",
			mqttService,
		)

		fmt.Fprintf(
			&builder,
			"      WINDOW_SIZE: \"%s\"\n",
			config.Edge.WindowSize,
		)

		fmt.Fprintf(
			&builder,
			"      EDGE_INGRESS_QUEUE_CAPACITY: \"%d\"\n",
			config.Edge.IngressQueueCapacity,
		)

		builder.WriteString(
			"      KAFKA_BROKER: \"kafka:29092\"\n",
		)

		builder.WriteString(
			"      KAFKA_TOPIC: \"edge-aggregates\"\n",
		)

		builder.WriteString(
			"    depends_on:\n",
		)

		fmt.Fprintf(
			&builder,
			"      %s:\n",
			mqttService,
		)

		builder.WriteString(
			"        condition: service_healthy\n",
		)

		builder.WriteString(
			"      kafka-init:\n",
		)

		builder.WriteString(
			"        condition: service_completed_successfully\n",
		)

		builder.WriteString(
			"    healthcheck:\n",
		)

		builder.WriteString(
			"      test: [\"CMD-SHELL\", \"wget -q -O - http://localhost:8080/readyz >/dev/null 2>&1\"]\n",
		)

		builder.WriteString(
			"      interval: 2s\n",
		)

		builder.WriteString(
			"      timeout: 1s\n",
		)

		builder.WriteString(
			"      retries: 15\n",
		)

		builder.WriteString(
			"    restart: unless-stopped\n",
		)

		builder.WriteString(
			"    networks:\n",
		)

		fmt.Fprintf(
			&builder,
			"      - %s\n",
			zoneNetwork,
		)

		builder.WriteString(
			"      - continuum-backbone\n\n",
		)

		// Simulator della zona. Tutte le istanze usano la stessa image;
		// il routing verso il broker locale e configurato dal deployment.
		fmt.Fprintf(
			&builder,
			"  %s:\n",
			simulatorService,
		)

		builder.WriteString(
			"    image: continuum-simulator:local\n",
		)

		fmt.Fprintf(
			&builder,
			"    container_name: %s\n",
			simulatorService,
		)

		builder.WriteString(
			"    profiles: [\"replay\"]\n",
		)

		builder.WriteString(
			"    environment:\n",
		)

		fmt.Fprintf(
			&builder,
			"      SITE_ID: \"%s\"\n",
			edge.EdgeID,
		)

		fmt.Fprintf(
			&builder,
			"      MQTT_ENDPOINT: \"tcp://%s:1883\"\n",
			mqttService,
		)

		fmt.Fprintf(
			&builder,
			"      REPLAY_FILE: \"/app/dataset/derived/replay_by_edge/%s.csv\"\n",
			edge.EdgeID,
		)

		fmt.Fprintf(
			&builder,
			"      MAX_EVENTS: \"%d\"\n",
			config.Workload.MaxEvents,
		)

		fmt.Fprintf(
			&builder,
			"      REPLAY_EPOCH: \"%s\"\n",
			deploymentReplayEpoch,
		)

		fmt.Fprintf(
			&builder,
			"      REPLAY_START_AT: \"%s\"\n",
			config.Workload.ReplayStartAt,
		)

		fmt.Fprintf(
			&builder,
			"      ACCELERATION_FACTOR: \"%s\"\n",
			formatFloat(config.Workload.AccelerationFactor),
		)

		fmt.Fprintf(
			&builder,
			"      TELEMETRY_QUEUE_CAPACITY: \"%d\"\n",
			config.Simulator.TelemetryQueueCapacity,
		)

		builder.WriteString(
			"    volumes:\n",
		)

		builder.WriteString(
			"      - ../../dataset:/app/dataset:ro\n",
		)

		builder.WriteString(
			"    depends_on:\n",
		)

		fmt.Fprintf(
			&builder,
			"      %s:\n",
			mqttService,
		)

		builder.WriteString(
			"        condition: service_healthy\n",
		)

		fmt.Fprintf(
			&builder,
			"      %s:\n",
			edge.EdgeID,
		)

		builder.WriteString(
			"        condition: service_healthy\n",
		)

		builder.WriteString(
			"    restart: \"no\"\n",
		)

		builder.WriteString(
			"    networks:\n",
		)

		fmt.Fprintf(
			&builder,
			"      - %s\n\n",
			zoneNetwork,
		)
	}

	// Reti
	builder.WriteString(
		"networks:\n",
	)

	builder.WriteString(
		"  continuum-backbone:\n",
	)

	builder.WriteString(
		"    driver: bridge\n",
	)

	for _, edge := range edges {
		fmt.Fprintf(
			&builder,
			"  zone-%s:\n",
			edge.EdgeID,
		)

		builder.WriteString(
			"    driver: bridge\n",
		)
	}

	// Volumi persistenti
	builder.WriteString(
		"\nvolumes:\n",
	)

	builder.WriteString(
		"  kafka-data:\n",
	)

	return builder.String()
}
