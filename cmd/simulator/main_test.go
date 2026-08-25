package main

import (
	"encoding/csv"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"continuum/internal/model"
)

func TestReplaySitePublishesAssignedSensor(t *testing.T) {
	reader := replayReader(
		"101;BME280;1;45.0;9.0;2025-01-01T10:00:00Z;100000;20;50",
	)

	topology := map[string]SensorAssignment{
		"101": {EdgeID: "edge-3"},
	}

	var published []model.SensorEvent

	count, err := replaySite(
		reader,
		topology,
		SimulatorConfig{SiteID: "edge-3"},
		func(_ string, event model.SensorEvent) error {
			published = append(published, event)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 || len(published) != 1 {
		t.Fatalf("pubblicazioni=%d, eventi catturati=%d", count, len(published))
	}

	if published[0].Sequence != 1 || published[0].EventID != "101-1" {
		t.Fatalf("sequence o event_id inattesi: %#v", published[0])
	}
}

func TestReplaySiteIgnoresSensorFromAnotherSite(t *testing.T) {
	reader := replayReader(
		"101;BME280;1;45.0;9.0;2025-01-01T10:00:00Z;100000;20;50",
	)

	topology := map[string]SensorAssignment{
		"101": {EdgeID: "edge-4"},
	}

	publishCalls := 0

	count, err := replaySite(
		reader,
		topology,
		SimulatorConfig{SiteID: "edge-3"},
		func(_ string, _ model.SensorEvent) error {
			publishCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if count != 0 || publishCalls != 0 {
		t.Fatalf("pubblicazioni=%d, chiamate publisher=%d", count, publishCalls)
	}
}

func TestMaxEventsCountsOnlyPublishedSiteEvents(t *testing.T) {
	reader := replayReader(
		"401;BME280;4;45.0;9.0;2025-01-01T10:00:00Z;100000;20;50",
		"301;BME280;3;45.0;9.0;2025-01-01T10:00:01Z;100000;21;51",
		"402;BME280;4;45.0;9.0;2025-01-01T10:00:02Z;100000;22;52",
		"301;BME280;3;45.0;9.0;2025-01-01T10:00:03Z;100000;23;53",
		"302;BME280;3;45.0;9.0;2025-01-01T10:00:04Z;100000;24;54",
	)

	topology := map[string]SensorAssignment{
		"301": {EdgeID: "edge-3"},
		"302": {EdgeID: "edge-3"},
		"401": {EdgeID: "edge-4"},
		"402": {EdgeID: "edge-4"},
	}

	var eventIDs []string

	count, err := replaySite(
		reader,
		topology,
		SimulatorConfig{
			SiteID:    "edge-3",
			MaxEvents: 2,
		},
		func(_ string, event model.SensorEvent) error {
			eventIDs = append(eventIDs, event.EventID)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Fatalf("pubblicazioni=%d, attese 2", count)
	}

	if strings.Join(eventIDs, ",") != "301-1,301-2" {
		t.Fatalf("event_id pubblicati inattesi: %v", eventIDs)
	}
}

func TestTelemetryTopic(t *testing.T) {
	if got := telemetryTopic("87575"); got != "sensors/87575/telemetry" {
		t.Fatalf("topic=%q", got)
	}
}

func TestLoadSimulatorConfigRequiresSiteID(t *testing.T) {
	_, err := loadSimulatorConfig(
		envFrom(map[string]string{
			"MQTT_ENDPOINT": "tcp://mqtt-edge-3:1883",
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "SITE_ID") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestLoadSimulatorConfigRequiresMQTTEndpoint(t *testing.T) {
	_, err := loadSimulatorConfig(
		envFrom(map[string]string{
			"SITE_ID": "edge-3",
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "MQTT_ENDPOINT") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestSimulatorHasNoEdgeDerivedBrokerRouting(t *testing.T) {
	file := parseSimulatorSource(t)

	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}

		if function.Name.Name == "brokerAddress" ||
			function.Name.Name == "getMQTTClient" {
			t.Fatalf("funzione di routing multi-broker ancora presente: %s", function.Name.Name)
		}
	}

	ast.Inspect(
		file,
		func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok && identifier.Name == "mqttBasePort" {
				t.Fatal("mqttBasePort non deve esistere nel Simulator")
			}

			return true
		},
	)
}

func TestSimulatorCreatesExactlyOneMQTTClient(t *testing.T) {
	file := parseSimulatorSource(t)
	newClientCalls := 0

	ast.Inspect(
		file,
		func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}

			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "NewClient" {
				newClientCalls++
			}

			return true
		},
	)

	if newClientCalls != 1 {
		t.Fatalf("chiamate mqtt.NewClient=%d, attesa 1", newClientCalls)
	}
}

func replayReader(rows ...string) *csv.Reader {
	const header = "sensor_id;sensor_type;location;lat;lon;timestamp;pressure;temperature;humidity"

	reader := csv.NewReader(
		strings.NewReader(
			header + "\n" + strings.Join(rows, "\n") + "\n",
		),
	)
	reader.Comma = ';'

	return reader
}

func envFrom(values map[string]string) func(string) string {
	return func(name string) string {
		return values[name]
	}
}

func parseSimulatorSource(t *testing.T) *ast.File {
	t.Helper()

	file, err := parser.ParseFile(
		token.NewFileSet(),
		"main.go",
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("lettura main.go fallita: %v", err)
	}

	return file
}
