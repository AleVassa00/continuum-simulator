package config

import "testing"

func TestLoadProjectConfig(t *testing.T) {
	cfg, err := Load("../../config/project.yml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Transport.SimulatorToEdge.Protocol != "mqtt" {
		t.Fatalf(
			"simulator transport = %q, want mqtt",
			cfg.Transport.SimulatorToEdge.Protocol,
		)
	}
	if cfg.Experiment.ReplaySpeedup != 1_000 {
		t.Fatalf(
			"replay speedup = %v, want 1000",
			cfg.Experiment.ReplaySpeedup,
		)
	}
}

func TestLoadAppliesMQTTBrokerEnvironmentOverride(t *testing.T) {
	t.Setenv("CONTINUUM_MQTT_BROKER_URL", "mqtt://mosquitto:1883")

	cfg, err := Load("../../config/project.yml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if got := cfg.Transport.SimulatorToEdge.BrokerURL; got != "mqtt://mosquitto:1883" {
		t.Fatalf("MQTT broker URL = %q, want mqtt://mosquitto:1883", got)
	}
}
