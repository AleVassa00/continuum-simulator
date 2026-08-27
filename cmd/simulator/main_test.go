package main

import (
	"encoding/csv"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"continuum/internal/model"
)

func TestReplayPublishesEveryShardRowWithPerSensorSequence(t *testing.T) {
	reader := replayReader(
		"101;BME280;1;45.0;9.0;2025-01-01T10:00:00Z;100000;20;50",
		"102;BME280;2;45.0;9.0;2025-01-01T10:00:01Z;100001;21;51",
		"101;BME280;1;45.0;9.0;2025-01-01T10:00:02Z;100002;22;52",
	)

	var events []model.SensorEvent

	count, err := replaySite(
		reader,
		SimulatorConfig{SiteID: "edge-3"},
		func(_ string, event model.SensorEvent) error {
			events = append(events, event)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if count != 3 || len(events) != 3 {
		t.Fatalf("pubblicazioni=%d, eventi catturati=%d", count, len(events))
	}

	for index, expected := range []struct {
		sensorID string
		sequence uint64
		eventID  string
	}{
		{sensorID: "101", sequence: 1, eventID: "101-1"},
		{sensorID: "102", sequence: 1, eventID: "102-1"},
		{sensorID: "101", sequence: 2, eventID: "101-2"},
	} {
		actual := events[index]

		if actual.SensorID != expected.sensorID ||
			actual.Sequence != expected.sequence ||
			actual.EventID != expected.eventID {
			t.Fatalf("evento %d inatteso: %#v", index, actual)
		}
	}
}

func TestMaxEventsLimitsOnlyThisSimulatorInstance(t *testing.T) {
	reader := replayReader(
		"301;BME280;3;45.0;9.0;2025-01-01T10:00:00Z;100000;20;50",
		"302;BME280;3;45.0;9.0;2025-01-01T10:00:01Z;100001;21;51",
		"303;BME280;3;45.0;9.0;2025-01-01T10:00:02Z;100002;22;52",
	)

	publishCalls := 0

	count, err := replaySite(
		reader,
		SimulatorConfig{
			SiteID:    "edge-3",
			MaxEvents: 2,
		},
		func(_ string, _ model.SensorEvent) error {
			publishCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 || publishCalls != 2 {
		t.Fatalf("pubblicazioni=%d, chiamate publisher=%d", count, publishCalls)
	}
}

func TestReplayRejectsDecreasingTimestamp(t *testing.T) {
	reader := replayReader(
		"301;BME280;3;45.0;9.0;2025-01-01T10:00:02Z;100000;20;50",
		"302;BME280;3;45.0;9.0;2025-01-01T10:00:01Z;100001;21;51",
	)

	_, err := replaySite(
		reader,
		SimulatorConfig{SiteID: "edge-3"},
		func(_ string, _ model.SensorEvent) error {
			return nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "non ordinato") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestTelemetryTopic(t *testing.T) {
	if got := telemetryTopic("87575"); got != "sensors/87575/telemetry" {
		t.Fatalf("topic=%q", got)
	}
}

func TestLoadSimulatorConfigRequiresEveryRuntimeValue(t *testing.T) {
	valid := map[string]string{
		"SITE_ID":       "edge-3",
		"MQTT_ENDPOINT": "tcp://mqtt-edge-3:1883",
		"REPLAY_FILE":   "/app/dataset/derived/replay_by_edge/edge-3.csv",
	}

	for _, missing := range []string{
		"SITE_ID",
		"MQTT_ENDPOINT",
		"REPLAY_FILE",
	} {
		t.Run(
			missing,
			func(t *testing.T) {
				values := make(map[string]string, len(valid))
				for name, value := range valid {
					values[name] = value
				}
				delete(values, missing)

				_, err := loadSimulatorConfig(envFrom(values))
				if err == nil || !strings.Contains(err.Error(), missing) {
					t.Fatalf("errore inatteso: %v", err)
				}
			},
		)
	}
}

func TestOpenReplayFileReportsMissingFile(t *testing.T) {
	missingPath := t.TempDir() + "/missing.csv"

	_, err := openReplayFile(missingPath)
	if err == nil || !strings.Contains(err.Error(), "REPLAY_FILE") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestSimulatorHasNoRuntimeTopologyDependency(t *testing.T) {
	file := parseSimulatorSource(t)
	forbiddenIdentifiers := map[string]struct{}{
		"SensorAssignment": {},
		"belongsToSite":    {},
		"brokerAddress":    {},
		"getMQTTClient":    {},
	}
	forbiddenStrings := []string{
		"kmeans_topology.csv",
		"macroarea_id",
	}

	ast.Inspect(
		file,
		func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.Ident:
				if _, forbidden := forbiddenIdentifiers[value.Name]; forbidden {
					t.Fatalf("dipendenza topologica runtime presente: %s", value.Name)
				}

			case *ast.BasicLit:
				if value.Kind != token.STRING {
					return true
				}

				literal, err := strconv.Unquote(value.Value)
				if err != nil {
					t.Fatalf("stringa Go non valida %s: %v", value.Value, err)
				}

				for _, forbidden := range forbiddenStrings {
					if strings.Contains(literal, forbidden) {
						t.Fatalf("riferimento runtime vietato: %s", forbidden)
					}
				}
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
