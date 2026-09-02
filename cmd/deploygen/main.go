package main

import (
	_ "embed"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
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

type composeCloudWorker struct {
	WorkerID string
}

type composeEdge struct {
	EdgeDeployment
	MQTTService      string
	SimulatorService string
	ZoneNetwork      string
}

type composeTemplateData struct {
	ExperimentName string

	CloudWorkers    []composeCloudWorker
	CloudWindowSize string
	ExpectedEdgeIDs string

	Edges                    []composeEdge
	EdgeWindowSize           string
	EdgeIngressQueueCapacity int

	MaxEvents              int
	ReplayEpoch            string
	ReplayStartAt          string
	AccelerationFactor     string
	TelemetryQueueCapacity int
}

type deploygenOptions struct {
	TopologyPath  string
	OutputPath    string
	ArtifactsRoot string
	Now           func() time.Time
	Stdout        io.Writer
}

//go:embed compose.tmpl
var composeTemplateSource string

var composeTemplate = template.Must(
	template.New("compose").Parse(
		strings.ReplaceAll(composeTemplateSource, "\r\n", "\n"),
	),
)

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
	cloudWorkers := make([]composeCloudWorker, 0, config.Cloud.Workers)
	for workerNumber := 0; workerNumber < config.Cloud.Workers; workerNumber++ {
		cloudWorkers = append(cloudWorkers, composeCloudWorker{
			WorkerID: fmt.Sprintf("cloud-worker-%d", workerNumber),
		})
	}

	composeEdges := make([]composeEdge, 0, len(edges))
	expectedEdgeIDs := make([]string, 0, len(edges))
	for _, edge := range edges {
		composeEdges = append(composeEdges, composeEdge{
			EdgeDeployment:   edge,
			MQTTService:      "mqtt-" + edge.EdgeID,
			SimulatorService: "simulator-" + edge.EdgeID,
			ZoneNetwork:      "zone-" + edge.EdgeID,
		})
		expectedEdgeIDs = append(expectedEdgeIDs, edge.EdgeID)
	}

	data := composeTemplateData{
		ExperimentName: config.Experiment.Name,

		CloudWorkers:    cloudWorkers,
		CloudWindowSize: config.Cloud.WindowSize.String(),
		ExpectedEdgeIDs: strings.Join(expectedEdgeIDs, ","),

		Edges:                    composeEdges,
		EdgeWindowSize:           config.Edge.WindowSize.String(),
		EdgeIngressQueueCapacity: config.Edge.IngressQueueCapacity,

		MaxEvents:              config.Workload.MaxEvents,
		ReplayEpoch:            deploymentReplayEpoch,
		ReplayStartAt:          config.Workload.ReplayStartAt,
		AccelerationFactor:     formatFloat(config.Workload.AccelerationFactor),
		TelemetryQueueCapacity: config.Simulator.TelemetryQueueCapacity,
	}

	var builder strings.Builder
	if err := composeTemplate.Execute(&builder, data); err != nil {
		panic(fmt.Errorf("rendering Docker Compose fallito: %w", err))
	}

	return builder.String()
}
