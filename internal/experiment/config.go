package experiment

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

type Config struct {
	Experiment ExperimentConfig `yaml:"experiment"`
	Workload   WorkloadConfig   `yaml:"workload"`
	Simulator  SimulatorConfig  `yaml:"simulator"`
	Edge       EdgeConfig       `yaml:"edge"`
	Cloud      CloudConfig      `yaml:"cloud"`
}

type ExperimentConfig struct {
	Name string `yaml:"name"`
}

type WorkloadConfig struct {
	AccelerationFactor float64  `yaml:"acceleration_factor"`
	MaxEvents          int      `yaml:"max_events"`
	StartLeadTime      Duration `yaml:"start_lead_time"`
}

type SimulatorConfig struct {
	TelemetryQueueCapacity int `yaml:"telemetry_queue_capacity"`
}

type EdgeConfig struct {
	WindowSize           Duration `yaml:"window_size"`
	IngressQueueCapacity int      `yaml:"ingress_queue_capacity"`
}

type CloudConfig struct {
	Workers    int      `yaml:"workers"`
	WindowSize Duration `yaml:"window_size"`
}

type EffectiveConfig struct {
	Experiment ExperimentConfig        `yaml:"experiment"`
	Workload   EffectiveWorkloadConfig `yaml:"workload"`
	Simulator  SimulatorConfig         `yaml:"simulator"`
	Edge       EdgeConfig              `yaml:"edge"`
	Cloud      CloudConfig             `yaml:"cloud"`
}

type EffectiveWorkloadConfig struct {
	AccelerationFactor float64  `yaml:"acceleration_factor"`
	MaxEvents          int      `yaml:"max_events"`
	StartLeadTime      Duration `yaml:"start_lead_time"`
	ReplayStartAt      string   `yaml:"replay_start_at"`
}

func (duration Duration) Duration() time.Duration {
	return time.Duration(duration)
}

func (duration Duration) String() string {
	return duration.Duration().String()
}

func (duration *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("durata YAML deve essere un valore scalare")
	}

	parsed, err := time.ParseDuration(strings.TrimSpace(node.Value))
	if err != nil {
		return fmt.Errorf("durata non valida %q: %w", node.Value, err)
	}

	*duration = Duration(parsed)
	return nil
}

func (duration Duration) MarshalYAML() (interface{}, error) {
	return duration.String(), nil
}

func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("apertura experiment config %q fallita: %w", path, err)
	}
	defer file.Close()

	config, err := Decode(file)
	if err != nil {
		return Config{}, fmt.Errorf("experiment config %q non valida: %w", path, err)
	}

	return config, nil
}

func Decode(reader io.Reader) (Config, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)

	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decodifica YAML fallita: %w", err)
	}

	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("sono ammessi un solo documento YAML")
		}
		return Config{}, fmt.Errorf("contenuto YAML aggiuntivo non valido: %w", err)
	}

	if err := config.Validate(); err != nil {
		return Config{}, err
	}

	return config, nil
}

func (config Config) Validate() error {
	if strings.TrimSpace(config.Experiment.Name) == "" {
		return fmt.Errorf("experiment.name non puo essere vuoto")
	}

	factor := config.Workload.AccelerationFactor
	if factor <= 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return fmt.Errorf("workload.acceleration_factor deve essere finito e maggiore di zero")
	}
	if config.Workload.MaxEvents < 0 {
		return fmt.Errorf("workload.max_events non puo essere negativo")
	}
	if config.Workload.StartLeadTime.Duration() <= 0 {
		return fmt.Errorf("workload.start_lead_time deve essere maggiore di zero")
	}
	if config.Simulator.TelemetryQueueCapacity <= 0 {
		return fmt.Errorf("simulator.telemetry_queue_capacity deve essere maggiore di zero")
	}
	if config.Edge.WindowSize.Duration() <= 0 {
		return fmt.Errorf("edge.window_size deve essere maggiore di zero")
	}
	if config.Edge.IngressQueueCapacity <= 0 {
		return fmt.Errorf("edge.ingress_queue_capacity deve essere maggiore di zero")
	}
	if config.Cloud.Workers <= 0 {
		return fmt.Errorf("cloud.workers deve essere maggiore di zero")
	}
	if config.Cloud.WindowSize.Duration() <= 0 {
		return fmt.Errorf("cloud.window_size deve essere maggiore di zero")
	}
	if config.Cloud.WindowSize.Duration()%config.Edge.WindowSize.Duration() != 0 {
		return fmt.Errorf(
			"cloud.window_size %s deve essere un multiplo esatto di edge.window_size %s",
			config.Cloud.WindowSize,
			config.Edge.WindowSize,
		)
	}

	return nil
}

func BuildEffective(config Config, replayStartAt time.Time) EffectiveConfig {
	return EffectiveConfig{
		Experiment: config.Experiment,
		Workload: EffectiveWorkloadConfig{
			AccelerationFactor: config.Workload.AccelerationFactor,
			MaxEvents:          config.Workload.MaxEvents,
			StartLeadTime:      config.Workload.StartLeadTime,
			ReplayStartAt:      replayStartAt.UTC().Format(time.RFC3339Nano),
		},
		Simulator: config.Simulator,
		Edge:      config.Edge,
		Cloud:     config.Cloud,
	}
}

func WriteEffective(path string, config EffectiveConfig) error {
	payload, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("serializzazione effective config fallita: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("creazione directory effective config fallita: %w", err)
	}
	if err := os.WriteFile(path, payload, 0644); err != nil {
		return fmt.Errorf("scrittura effective config %q fallita: %w", path, err)
	}

	return nil
}
