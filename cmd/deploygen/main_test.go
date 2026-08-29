package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestBuildComposeCreatesReadySiteScopedSimulators(t *testing.T) {
	edges := make([]EdgeDeployment, 0, 13)
	for edgeNumber := 0; edgeNumber < 13; edgeNumber++ {
		edges = append(edges, EdgeDeployment{
			EdgeID:      fmt.Sprintf("edge-%d", edgeNumber),
			EdgeNumber:  edgeNumber,
			SensorCount: 11,
			MQTTPort:    mqttBasePort + edgeNumber,
		})
	}

	compose := buildCompose(edges)
	globalReplayEnvironment := []string{
		"REPLAY_EPOCH: \"${REPLAY_EPOCH:-2025-01-01T00:00:00Z}\"",
		"REPLAY_START_AT: \"${REPLAY_START_AT:-}\"",
		"ACCELERATION_FACTOR: \"${ACCELERATION_FACTOR:-1000}\"",
		"TELEMETRY_QUEUE_CAPACITY: \"${TELEMETRY_QUEUE_CAPACITY:-1000}\"",
	}

	if count := strings.Count(
		compose,
		"    image: continuum-simulator:local\n",
	); count != len(edges) {
		t.Fatalf("istanze continuum-simulator=%d, attese %d", count, len(edges))
	}

	for _, expected := range globalReplayEnvironment {
		if count := strings.Count(
			compose,
			"      "+expected+"\n",
		); count != len(edges) {
			t.Errorf("configurazioni globali %q=%d, attese %d", expected, count, len(edges))
		}
	}

	for _, edgeDeployment := range edges {
		edgeID := edgeDeployment.EdgeID
		t.Run(
			edgeID,
			func(t *testing.T) {
				simulator := composeServiceBlock(
					t,
					compose,
					"simulator-"+edgeID,
				)

				for _, expected := range []string{
					"image: continuum-simulator:local",
					"SITE_ID: \"" + edgeID + "\"",
					"MQTT_ENDPOINT: \"tcp://mqtt-" + edgeID + ":1883\"",
					"REPLAY_FILE: \"/app/dataset/derived/replay_by_edge/" + edgeID + ".csv\"",
					"MAX_EVENTS: \"${MAX_EVENTS:-0}\"",
					"mqtt-" + edgeID + ":\n        condition: service_healthy",
					edgeID + ":\n        condition: service_healthy",
					"- zone-" + edgeID,
				} {
					if !strings.Contains(simulator, expected) {
						t.Errorf("configurazione Simulator senza %q", expected)
					}
				}

				for _, expected := range globalReplayEnvironment {
					if count := strings.Count(simulator, expected); count != 1 {
						t.Errorf("configurazione globale %q presente %d volte, attesa 1", expected, count)
					}
				}

				if count := strings.Count(
					simulator,
					"MQTT_ENDPOINT:",
				); count != 1 {
					t.Errorf("MQTT_ENDPOINT presenti=%d, atteso 1", count)
				}

				if strings.Contains(simulator, "localhost") ||
					strings.Contains(simulator, "continuum-backbone") {
					t.Errorf("Simulator non isolato nella propria zona:\n%s", simulator)
				}

				edge := composeServiceBlock(
					t,
					compose,
					edgeID,
				)

				for _, expected := range []string{
					"healthcheck:",
					"EDGE_INGRESS_QUEUE_CAPACITY: \"${EDGE_INGRESS_QUEUE_CAPACITY:-1000}\"",
					"http://localhost:8080/readyz",
					"interval: 2s",
					"timeout: 1s",
				} {
					if !strings.Contains(edge, expected) {
						t.Errorf("healthcheck Edge senza %q", expected)
					}
				}
			},
		)
	}
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
