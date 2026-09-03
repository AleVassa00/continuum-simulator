package experiment

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadBaseline(t *testing.T) {
	config, err := Load(filepath.Join("..", "..", "experiments", "baseline.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if config.Experiment.Name != "baseline" ||
		config.Workload.AccelerationFactor != 10000 ||
		config.Workload.MaxEvents != 0 ||
		config.Workload.StartLeadTime.Duration() != 90*time.Second ||
		config.Simulator.TelemetryQueueCapacity != 1000 ||
		config.Edge.WindowSize.Duration() != 5*time.Minute ||
		config.Edge.IngressQueueCapacity != 1000 ||
		config.Cloud.Workers != 1 ||
		config.Cloud.WindowSize.Duration() != 15*time.Minute ||
		config.Global.WatermarkDelay.Duration() != 15*time.Minute ||
		config.Global.EdgeIdleTimeout.Duration() != 5*time.Second {
		t.Fatalf("baseline inattesa: %#v", config)
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	_, err := Decode(strings.NewReader(strings.Replace(
		validYAML(),
		"telemetry_queue_capacity",
		"telemtry_queue_capacity",
		1,
	)))
	if err == nil || !strings.Contains(err.Error(), "telemtry_queue_capacity") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		field  string
	}{
		{"empty_name", func(c *Config) { c.Experiment.Name = " " }, "experiment.name"},
		{"zero_factor", func(c *Config) { c.Workload.AccelerationFactor = 0 }, "acceleration_factor"},
		{"negative_factor", func(c *Config) { c.Workload.AccelerationFactor = -1 }, "acceleration_factor"},
		{"nan_factor", func(c *Config) { c.Workload.AccelerationFactor = math.NaN() }, "acceleration_factor"},
		{"inf_factor", func(c *Config) { c.Workload.AccelerationFactor = math.Inf(1) }, "acceleration_factor"},
		{"negative_max_events", func(c *Config) { c.Workload.MaxEvents = -1 }, "max_events"},
		{"zero_lead_time", func(c *Config) { c.Workload.StartLeadTime = 0 }, "start_lead_time"},
		{"zero_simulator_queue", func(c *Config) { c.Simulator.TelemetryQueueCapacity = 0 }, "telemetry_queue_capacity"},
		{"zero_edge_window", func(c *Config) { c.Edge.WindowSize = 0 }, "edge.window_size"},
		{"zero_edge_queue", func(c *Config) { c.Edge.IngressQueueCapacity = 0 }, "ingress_queue_capacity"},
		{"zero_workers", func(c *Config) { c.Cloud.Workers = 0 }, "cloud.workers"},
		{"zero_cloud_window", func(c *Config) { c.Cloud.WindowSize = 0 }, "cloud.window_size"},
		{"non_multiple_windows", func(c *Config) { c.Cloud.WindowSize = Duration(14 * time.Minute) }, "multiplo"},
		{"negative_global_idle_timeout", func(c *Config) { c.Global.EdgeIdleTimeout = -1 }, "edge_idle_timeout"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config)
			err := config.Validate()
			if err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}
}

func TestWriteEffectiveUsesActualReplayStart(t *testing.T) {
	start := time.Date(2026, time.August, 31, 12, 0, 0, 123, time.UTC)
	effective := BuildEffective(validConfig(), start)
	path := filepath.Join(t.TempDir(), "baseline", "effective-config.yaml")

	if err := WriteEffective(path, effective); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	text := string(payload)
	for _, expected := range []string{
		"name: baseline",
		"start_lead_time: 10s",
		"replay_start_at: \"" + start.Format(time.RFC3339Nano) + "\"",
		"telemetry_queue_capacity: 1000",
		"ingress_queue_capacity: 1000",
		"workers: 1",
		"watermark_delay: 15m0s",
		"edge_idle_timeout: 5s",
	} {
		if !strings.Contains(text, expected) {
			t.Errorf("effective config senza %q:\n%s", expected, text)
		}
	}
}

func TestBuildEffectiveDefaultsGlobalWatermarkAndIdleTimeout(t *testing.T) {
	config := validConfig()
	config.Global = GlobalConfig{}

	effective := BuildEffective(config, time.Now())
	if effective.Global.WatermarkDelay.Duration() != config.Cloud.WindowSize.Duration() ||
		effective.Global.EdgeIdleTimeout.Duration() != 5*time.Second {
		t.Fatalf("default globali inattesi: %#v", effective.Global)
	}
}

func TestResolveDefaultsDoesNotRequireReplayStart(t *testing.T) {
	config := validConfig()
	config.Simulator.StartLateTolerance = 0
	config.Global = GlobalConfig{}

	resolved := ResolveDefaults(config)

	if resolved.Simulator.StartLateTolerance.Duration() != 10*time.Second {
		t.Fatalf("start late tolerance inattesa: %s", resolved.Simulator.StartLateTolerance)
	}
	if resolved.Global.WatermarkDelay.Duration() != config.Cloud.WindowSize.Duration() {
		t.Fatalf("watermark delay inatteso: %s", resolved.Global.WatermarkDelay)
	}
	if resolved.Global.EdgeIdleTimeout.Duration() != 5*time.Second {
		t.Fatalf("edge idle timeout inatteso: %s", resolved.Global.EdgeIdleTimeout)
	}
}

func validConfig() Config {
	return Config{
		Experiment: ExperimentConfig{Name: "baseline"},
		Workload: WorkloadConfig{
			AccelerationFactor: 1000,
			MaxEvents:          0,
			StartLeadTime:      Duration(10 * time.Second),
		},
		Simulator: SimulatorConfig{TelemetryQueueCapacity: 1000},
		Edge: EdgeConfig{
			WindowSize:           Duration(5 * time.Minute),
			IngressQueueCapacity: 1000,
		},
		Cloud: CloudConfig{
			Workers:    1,
			WindowSize: Duration(15 * time.Minute),
		},
		Global: GlobalConfig{
			WatermarkDelay:  Duration(15 * time.Minute),
			EdgeIdleTimeout: Duration(5 * time.Second),
		},
	}
}

func validYAML() string {
	return `experiment:
  name: baseline
workload:
  acceleration_factor: 1000
  max_events: 0
  start_lead_time: 10s
simulator:
  telemetry_queue_capacity: 1000
edge:
  window_size: 5m
  ingress_queue_capacity: 1000
cloud:
  workers: 1
  window_size: 15m
global:
  watermark_delay: 15m
  edge_idle_timeout: 5s
`
}
