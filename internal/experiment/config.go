package experiment

import (
	"continuum/internal/cloudworker"
	"continuum/internal/model"
	"crypto/sha256"
	"encoding/hex"
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

type KafkaConfig struct {
	Partitions *int `yaml:"partitions,omitempty"`
}

// yaml.v3 otherwise coerces fractional scalars to int. A partition count must
// be an integer, and a supplied zero must not be confused with an absent value.
func (config *KafkaConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("kafka must be a mapping")
	}
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if key.Value != "partitions" {
			return fmt.Errorf("unknown kafka field %q", key.Value)
		}
		if config.Partitions != nil {
			return fmt.Errorf("duplicate kafka.partitions")
		}
		if value.Tag != "!!int" {
			return fmt.Errorf("kafka.partitions must be an integer")
		}
		var count int
		if err := value.Decode(&count); err != nil {
			return err
		}
		config.Partitions = &count
	}
	return nil
}

func (config KafkaConfig) ResolvedPartitions() int {
	if config.Partitions != nil {
		return *config.Partitions
	}
	return model.DefaultSourcePartitionCount
}

type Config struct {
	Kafka      KafkaConfig      `yaml:"kafka"`
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
	StartLeadTime      Duration `yaml:"start_lead_time"`
}

type SimulatorConfig struct {
	TelemetryQueueCapacity int      `yaml:"telemetry_queue_capacity"`
	StartLateTolerance     Duration `yaml:"start_late_tolerance,omitempty"`
}

type EdgeConfig struct {
	WindowSize           Duration `yaml:"window_size"`
	IngressQueueCapacity int      `yaml:"ingress_queue_capacity"`
}

type CloudConfig struct {
	ConsumerCommitBatchSize *CommitBatchSize `yaml:"consumer_commit_batch_size,omitempty"`
	Workers                 int              `yaml:"workers"`
	WindowSize              Duration         `yaml:"window_size"`
}

// CommitBatchSize rejects fractional YAML values instead of truncating them.
type CommitBatchSize int

func (size *CommitBatchSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag != "!!int" {
		return fmt.Errorf("cloud.consumer_commit_batch_size must be a positive integer")
	}
	var value int
	if err := node.Decode(&value); err != nil {
		return err
	}
	if value <= 0 {
		return fmt.Errorf("cloud.consumer_commit_batch_size must be a positive integer")
	}
	*size = CommitBatchSize(value)
	return nil
}

func (config CloudConfig) ResolvedConsumerCommitBatchSize() int {
	if config.ConsumerCommitBatchSize != nil {
		return int(*config.ConsumerCommitBatchSize)
	}
	return cloudworker.DefaultConsumerCommitBatchSize
}

type EffectiveConfig struct {
	Kafka      KafkaConfig             `yaml:"kafka"`
	Experiment ExperimentConfig        `yaml:"experiment"`
	Workload   EffectiveWorkloadConfig `yaml:"workload"`
	Simulator  SimulatorConfig         `yaml:"simulator"`
	Edge       EdgeConfig              `yaml:"edge"`
	Cloud      CloudConfig             `yaml:"cloud"`
}

type EffectiveWorkloadConfig struct {
	AccelerationFactor float64  `yaml:"acceleration_factor"`
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
	if config.Cloud.ResolvedConsumerCommitBatchSize() <= 0 {
		return fmt.Errorf("cloud.consumer_commit_batch_size must be a positive integer")
	}
	if config.Kafka.ResolvedPartitions() <= 0 {
		return fmt.Errorf("kafka.partitions deve essere maggiore di zero")
	}
	if strings.TrimSpace(config.Experiment.Name) == "" {
		return fmt.Errorf("experiment.name non puo essere vuoto")
	}

	factor := config.Workload.AccelerationFactor
	if factor <= 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return fmt.Errorf("workload.acceleration_factor deve essere finito e maggiore di zero")
	}
	if config.Workload.StartLeadTime.Duration() <= 0 {
		return fmt.Errorf("workload.start_lead_time deve essere maggiore di zero")
	}
	if config.Simulator.TelemetryQueueCapacity <= 0 {
		return fmt.Errorf("simulator.telemetry_queue_capacity deve essere maggiore di zero")
	}
	if config.Simulator.StartLateTolerance.Duration() < 0 {
		return fmt.Errorf("simulator.start_late_tolerance non puo essere negativo")
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

func ResolveDefaults(config Config) Config {
	if config.Cloud.ConsumerCommitBatchSize == nil {
		size := CommitBatchSize(config.Cloud.ResolvedConsumerCommitBatchSize())
		config.Cloud.ConsumerCommitBatchSize = &size
	}
	if config.Kafka.Partitions == nil {
		count := config.Kafka.ResolvedPartitions()
		config.Kafka.Partitions = &count
	}
	simulator := config.Simulator
	if simulator.StartLateTolerance.Duration() <= 0 {
		simulator.StartLateTolerance = Duration(10 * time.Second)
	}

	config.Simulator = simulator
	return config
}

func MarshalResolved(config Config) ([]byte, error) {
	payload, err := yaml.Marshal(ResolveDefaults(config))
	if err != nil {
		return nil, fmt.Errorf("serializzazione experiment config risolta fallita: %w", err)
	}

	return payload, nil
}

func Fingerprint(config Config) (string, error) {
	payload, err := MarshalResolved(config)
	if err != nil {
		return "", err
	}

	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func BuildEffective(config Config, replayStartAt time.Time) EffectiveConfig {
	config = ResolveDefaults(config)

	return EffectiveConfig{
		Kafka:      config.Kafka,
		Experiment: config.Experiment,
		Workload: EffectiveWorkloadConfig{
			AccelerationFactor: config.Workload.AccelerationFactor,
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
