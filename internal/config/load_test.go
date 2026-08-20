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
