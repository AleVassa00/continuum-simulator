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
	"regexp"
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
	Network    *NetworkConfig   `yaml:"network,omitempty"`
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
	WindowSize                Duration               `yaml:"window_size"`
	IngressQueueCapacity      int                    `yaml:"ingress_queue_capacity"`
	KafkaProducerBatchSize    *EdgeProducerBatchSize `yaml:"kafka_producer_batch_size,omitempty"`
	KafkaProducerBatchMaxWait *Duration              `yaml:"kafka_producer_batch_max_wait,omitempty"`
}

const (
	DefaultEdgeKafkaProducerBatchSize    = 1
	DefaultEdgeKafkaProducerBatchMaxWait = 100 * time.Millisecond
)

// EdgeProducerBatchSize rejects fractional YAML values instead of truncating them.
type EdgeProducerBatchSize int

func (size *EdgeProducerBatchSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag != "!!int" {
		return fmt.Errorf("edge.kafka_producer_batch_size must be a positive integer")
	}
	var value int
	if err := node.Decode(&value); err != nil {
		return err
	}
	if value <= 0 {
		return fmt.Errorf("edge.kafka_producer_batch_size must be a positive integer")
	}
	*size = EdgeProducerBatchSize(value)
	return nil
}

func (config EdgeConfig) ResolvedKafkaProducerBatchSize() int {
	if config.KafkaProducerBatchSize != nil {
		return int(*config.KafkaProducerBatchSize)
	}
	return DefaultEdgeKafkaProducerBatchSize
}

func (config EdgeConfig) ResolvedKafkaProducerBatchMaxWait() time.Duration {
	if config.KafkaProducerBatchMaxWait != nil {
		return config.KafkaProducerBatchMaxWait.Duration()
	}
	return DefaultEdgeKafkaProducerBatchMaxWait
}

type CloudConfig struct {
	ConsumerCommitBatchSize *CommitBatchSize `yaml:"consumer_commit_batch_size,omitempty"`
	MaxEdgeWatermarkSkew    *Duration        `yaml:"max_edge_watermark_skew,omitempty"`
	Workers                 int              `yaml:"workers"`
	WindowSize              Duration         `yaml:"window_size"`
}

const NetworkRateUnlimited = NetworkRate("unlimited")

var networkRatePattern = regexp.MustCompile(`^[1-9][0-9]*(kbit|mbit|gbit)$`)

type NetworkRate string

func (rate *NetworkRate) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("network rate deve essere una stringa come 500kbit, 10mbit oppure unlimited")
	}

	value := NetworkRate(strings.ToLower(strings.TrimSpace(node.Value)))
	if value != NetworkRateUnlimited && !networkRatePattern.MatchString(string(value)) {
		return fmt.Errorf("network rate %q non valido: usare un intero positivo seguito da kbit, mbit o gbit, oppure unlimited", node.Value)
	}

	*rate = value
	return nil
}

func (rate NetworkRate) Resolved() NetworkRate {
	if strings.TrimSpace(string(rate)) == "" {
		return NetworkRateUnlimited
	}
	return rate
}

type NetworkConfig struct {
	Enabled         bool              `yaml:"enabled"`
	SimulatorToEdge NetworkLinkConfig `yaml:"simulator_to_edge,omitempty"`
	EdgeToKafka     NetworkLinkConfig `yaml:"edge_to_kafka,omitempty"`
}

type NetworkLinkConfig struct {
	Enabled bool        `yaml:"enabled"`
	Delay   Duration    `yaml:"delay,omitempty"`
	Rate    NetworkRate `yaml:"rate,omitempty"`
}

func (config NetworkLinkConfig) ResolvedRate() NetworkRate {
	return config.Rate.Resolved()
}

func (config Config) ResolvedNetwork() NetworkConfig {
	if config.Network == nil {
		return NetworkConfig{
			SimulatorToEdge: NetworkLinkConfig{Rate: NetworkRateUnlimited},
			EdgeToKafka:     NetworkLinkConfig{Rate: NetworkRateUnlimited},
		}
	}

	resolved := *config.Network
	resolved.SimulatorToEdge.Rate = resolved.SimulatorToEdge.ResolvedRate()
	resolved.EdgeToKafka.Rate = resolved.EdgeToKafka.ResolvedRate()
	return resolved
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

func (config CloudConfig) ResolvedMaxEdgeWatermarkSkew() time.Duration {
	if config.MaxEdgeWatermarkSkew != nil {
		return config.MaxEdgeWatermarkSkew.Duration()
	}
	return cloudworker.DefaultMaxEdgeWatermarkSkew
}

type EffectiveConfig struct {
	Kafka      KafkaConfig             `yaml:"kafka"`
	Experiment ExperimentConfig        `yaml:"experiment"`
	Workload   EffectiveWorkloadConfig `yaml:"workload"`
	Simulator  SimulatorConfig         `yaml:"simulator"`
	Edge       EdgeConfig              `yaml:"edge"`
	Cloud      CloudConfig             `yaml:"cloud"`
	Network    *NetworkConfig          `yaml:"network,omitempty"`
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
	if config.Cloud.ResolvedMaxEdgeWatermarkSkew() <= 0 {
		return fmt.Errorf("cloud.max_edge_watermark_skew deve essere maggiore di zero")
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
	if config.Edge.ResolvedKafkaProducerBatchSize() <= 0 {
		return fmt.Errorf("edge.kafka_producer_batch_size must be a positive integer")
	}
	if config.Edge.ResolvedKafkaProducerBatchMaxWait() <= 0 {
		return fmt.Errorf("edge.kafka_producer_batch_max_wait deve essere maggiore di zero")
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
	if err := validateNetwork(config.ResolvedNetwork()); err != nil {
		return err
	}

	return nil
}

func validateNetwork(config NetworkConfig) error {
	links := []struct {
		name   string
		config NetworkLinkConfig
	}{
		{name: "network.simulator_to_edge", config: config.SimulatorToEdge},
		{name: "network.edge_to_kafka", config: config.EdgeToKafka},
	}

	enabledLinks := 0
	for _, link := range links {
		if link.config.Delay.Duration() < 0 {
			return fmt.Errorf("%s.delay non puo essere negativo", link.name)
		}
		rate := link.config.ResolvedRate()
		if rate != NetworkRateUnlimited && !networkRatePattern.MatchString(string(rate)) {
			return fmt.Errorf("%s.rate %q non valido", link.name, rate)
		}
		if !link.config.Enabled {
			continue
		}
		enabledLinks++
		if link.config.Delay.Duration() == 0 && rate == NetworkRateUnlimited {
			return fmt.Errorf("%s abilitato senza delay o limite di banda", link.name)
		}
	}

	if config.Enabled && enabledLinks == 0 {
		return fmt.Errorf("network.enabled=true richiede almeno un collegamento abilitato")
	}
	return nil
}

func ResolveDefaults(config Config) Config {
	if config.Edge.KafkaProducerBatchSize == nil {
		size := EdgeProducerBatchSize(config.Edge.ResolvedKafkaProducerBatchSize())
		config.Edge.KafkaProducerBatchSize = &size
	}
	if config.Edge.KafkaProducerBatchMaxWait == nil {
		duration := Duration(config.Edge.ResolvedKafkaProducerBatchMaxWait())
		config.Edge.KafkaProducerBatchMaxWait = &duration
	}
	if config.Cloud.ConsumerCommitBatchSize == nil {
		size := CommitBatchSize(config.Cloud.ResolvedConsumerCommitBatchSize())
		config.Cloud.ConsumerCommitBatchSize = &size
	}
	if config.Cloud.MaxEdgeWatermarkSkew == nil {
		duration := Duration(config.Cloud.ResolvedMaxEdgeWatermarkSkew())
		config.Cloud.MaxEdgeWatermarkSkew = &duration
	}
	if config.Kafka.Partitions == nil {
		count := config.Kafka.ResolvedPartitions()
		config.Kafka.Partitions = &count
	}
	if config.Network != nil {
		resolvedNetwork := config.ResolvedNetwork()
		config.Network = &resolvedNetwork
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
		Network:   config.Network,
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
