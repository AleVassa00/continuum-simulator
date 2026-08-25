package main

import (
	"strings"
	"testing"
)

func TestBuildComposeCreatesOneConfiguredSimulatorPerEdge(t *testing.T) {
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

	for _, expected := range []string{
		"  simulator-edge-0:\n",
		"      SITE_ID: \"edge-0\"\n",
		"      MQTT_ENDPOINT: \"tcp://mqtt-edge-0:1883\"\n",
		"      - zone-edge-0\n",
		"  simulator-edge-12:\n",
		"      SITE_ID: \"edge-12\"\n",
		"      MQTT_ENDPOINT: \"tcp://mqtt-edge-12:1883\"\n",
		"      - zone-edge-12\n",
		"      - ../../dataset:/app/dataset:ro\n",
	} {
		if !strings.Contains(compose, expected) {
			t.Errorf("configurazione generata senza %q", expected)
		}
	}

	if count := strings.Count(
		compose,
		"    image: continuum-simulator:local\n",
	); count != len(edges) {
		t.Fatalf("istanze continuum-simulator=%d, attese %d", count, len(edges))
	}

	if strings.Contains(compose, "MQTT_ENDPOINT: \"tcp://localhost:") {
		t.Fatal("il Simulator non deve ricevere localhost come endpoint MQTT")
	}
}
