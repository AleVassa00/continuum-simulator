package config

const CurrentVersion = 1

type Config struct {
	Version    int              `yaml:"version"`
	Dataset    DatasetConfig    `yaml:"dataset"`
	Experiment ExperimentConfig `yaml:"experiment"`
	Processing ProcessingConfig `yaml:"processing"`
	Transport  TransportConfig  `yaml:"transport"`
	Cloud      CloudConfig      `yaml:"cloud"`
}

type DatasetConfig struct {
	Name              string              `yaml:"name"`
	ReferenceFile     string              `yaml:"reference_file"`
	ReplayFile        string              `yaml:"replay_file"`
	TopologyFile      string              `yaml:"topology_file"`
	Delimiter         string              `yaml:"delimiter"`
	TimestampLayout   string              `yaml:"timestamp_layout"`
	TimestampTimezone string              `yaml:"timestamp_timezone"`
	Columns           ColumnConfig        `yaml:"columns"`
	Measurements      []MeasurementConfig `yaml:"measurements"`
}

type ColumnConfig struct {
	SensorID   string `yaml:"sensor_id"`
	LocationID string `yaml:"location_id"`
	Timestamp  string `yaml:"timestamp"`
	Latitude   string `yaml:"latitude"`
	Longitude  string `yaml:"longitude"`
}

type MeasurementConfig struct {
	Name       string     `yaml:"name"`
	Column     string     `yaml:"column"`
	Unit       string     `yaml:"unit"`
	Normalizer string     `yaml:"normalizer"`
	ValidRange ValueRange `yaml:"valid_range"`
}

type ValueRange struct {
	Min float64 `yaml:"min"`
	Max float64 `yaml:"max"`
}

type ExperimentConfig struct {
	Warmup                Duration `yaml:"warmup"`
	Measurement           Duration `yaml:"measurement"`
	DrainTimeout          Duration `yaml:"drain_timeout"`
	TargetEventsPerSecond int      `yaml:"target_events_per_second"`
	ReplaySpeedup         float64  `yaml:"replay_speedup"`
	Seed                  int64    `yaml:"seed"`
}

type ProcessingConfig struct {
	Edge  EdgeProcessingConfig  `yaml:"edge"`
	Cloud CloudProcessingConfig `yaml:"cloud"`
}

type EdgeProcessingConfig struct {
	WindowSize      Duration `yaml:"window_size"`
	AllowedLateness Duration `yaml:"allowed_lateness"`
}

type CloudProcessingConfig struct {
	WindowSize Duration `yaml:"window_size"`
}

type TransportConfig struct {
	SimulatorToEdge MQTTConfig  `yaml:"simulator_to_edge"`
	EdgeToCloud     KafkaConfig `yaml:"edge_to_cloud"`
}

type MQTTConfig struct {
	Protocol       string   `yaml:"protocol"`
	TopicTemplate  string   `yaml:"topic_template"`
	QoS            byte     `yaml:"qos"`
	Retain         bool     `yaml:"retain"`
	KeepAlive      Duration `yaml:"keep_alive"`
	ConnectTimeout Duration `yaml:"connect_timeout"`
}

type KafkaConfig struct {
	Protocol     string   `yaml:"protocol"`
	Brokers      []string `yaml:"brokers"`
	Topic        string   `yaml:"topic"`
	Partitions   int      `yaml:"partitions"`
	PartitionKey string   `yaml:"partition_key"`
	Delivery     string   `yaml:"delivery"`
}

type CloudConfig struct {
	ConsumerGroup string        `yaml:"consumer_group"`
	Storage       StorageConfig `yaml:"storage"`
}

type StorageConfig struct {
	Driver string `yaml:"driver"`
	DSNEnv string `yaml:"dsn_env"`
}
