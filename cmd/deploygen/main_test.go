package main

import (
	"strings"
	"testing"
)

func TestBuildComposeCreatesReadySiteScopedSimulators(t *testing.T) {
	edges := []EdgeDeployment{
		{
			EdgeID:      "edge-0",
			EdgeNumber:  0,
			SensorCount: 11,
			MQTTPort:    18830,
		},
		{
			EdgeID:      "edge-12",
			EdgeNumber:  12,
			SensorCount: 11,
			MQTTPort:    18842,
		},
	}

	compose := buildCompose(edges)

	if count := strings.Count(
		compose,
		"    image: continuum-simulator:local\n",
	); count != len(edges) {
		t.Fatalf("istanze continuum-simulator=%d, attese %d", count, len(edges))
	}

	for _, edgeID := range []string{"edge-0", "edge-12"} {
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
					"mqtt-" + edgeID + ":\n        condition: service_healthy",
					edgeID + ":\n        condition: service_healthy",
					"- zone-" + edgeID,
				} {
					if !strings.Contains(simulator, expected) {
						t.Errorf("configurazione Simulator senza %q", expected)
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
