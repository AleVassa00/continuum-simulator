package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/envutil"
)

type CloudWorkerConfig struct {
	ConsumerCommitBatchSize int
	SourcePartitionCount    int
	Membership              map[int][]string
	KafkaBroker             string
	KafkaOutputBroker       string
	InputTopic              string
	OutputTopic             string
	GroupID                 string
	WorkerID                string
	WindowSize              time.Duration
	MaxEdgeWatermarkSkew    time.Duration
}

func loadCloudWorkerConfig() (CloudWorkerConfig, error) {
	kafkaBroker := envutil.Required("KAFKA_BROKER")
	kafkaOutputBroker := envutil.OrDefault("KAFKA_OUTPUT_BROKER", kafkaBroker)

	inputTopic := loadInputTopic()
	outputTopic := envutil.OrDefault("KAFKA_OUTPUT_TOPIC", "cloud-partition-aggregates")
	groupID := envutil.OrDefault("KAFKA_GROUP_ID", "cloud-workers")
	workerID := loadWorkerID()
	windowSize, err := loadCloudWindowSize()
	if err != nil {
		return CloudWorkerConfig{}, err
	}
	maxEdgeWatermarkSkew, err := loadMaxEdgeWatermarkSkew()
	if err != nil {
		return CloudWorkerConfig{}, err
	}
	count, err := envutil.SourcePartitionCount()
	if err != nil {
		return CloudWorkerConfig{}, err
	}
	edgePartitionValue := strings.TrimSpace(os.Getenv("CLOUD_EDGE_PARTITIONS"))
	if edgePartitionValue == "" {
		return CloudWorkerConfig{}, fmt.Errorf("CLOUD_EDGE_PARTITIONS is required")
	}
	edgePartitions, err := parseEdgePartitions(edgePartitionValue)
	if err != nil {
		return CloudWorkerConfig{}, err
	}
	membership, err := cloudworker.BuildMembership(edgePartitions, count)
	if err != nil {
		return CloudWorkerConfig{}, err
	}
	batchSize, err := strconv.Atoi(envutil.OrDefault("CLOUD_CONSUMER_COMMIT_BATCH_SIZE", strconv.Itoa(cloudworker.DefaultConsumerCommitBatchSize)))
	if err != nil || batchSize <= 0 {
		return CloudWorkerConfig{}, fmt.Errorf("CLOUD_CONSUMER_COMMIT_BATCH_SIZE must be a positive integer")
	}

	return CloudWorkerConfig{
		ConsumerCommitBatchSize: batchSize,
		SourcePartitionCount:    count,
		Membership:              membership,
		KafkaBroker:             kafkaBroker,
		KafkaOutputBroker:       kafkaOutputBroker,
		InputTopic:              inputTopic,
		OutputTopic:             outputTopic,
		GroupID:                 groupID,
		WorkerID:                workerID,
		WindowSize:              windowSize,
		MaxEdgeWatermarkSkew:    maxEdgeWatermarkSkew,
	}, nil
}

func parseEdgePartitions(value string) (map[string]int, error) {
	assignments := make(map[string]int)
	for _, entry := range strings.Split(value, ",") {
		parts := strings.Split(entry, ":")
		if len(parts) != 2 || strings.TrimSpace(parts[0]) != parts[0] || parts[0] == "" {
			return nil, fmt.Errorf("CLOUD_EDGE_PARTITIONS entry non valida %q", entry)
		}
		partition, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("CLOUD_EDGE_PARTITIONS partition non valida in %q: %w", entry, err)
		}
		if _, duplicate := assignments[parts[0]]; duplicate {
			return nil, fmt.Errorf("CLOUD_EDGE_PARTITIONS contiene Edge duplicato %q", parts[0])
		}
		assignments[parts[0]] = partition
	}
	return assignments, nil
}

func loadMaxEdgeWatermarkSkew() (time.Duration, error) {
	value := envutil.OrDefault("CLOUD_MAX_EDGE_WATERMARK_SKEW", cloudworker.DefaultMaxEdgeWatermarkSkew.String())
	maxSkew, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("CLOUD_MAX_EDGE_WATERMARK_SKEW non valida %q: %w", value, err)
	}
	if maxSkew <= 0 {
		return 0, fmt.Errorf("CLOUD_MAX_EDGE_WATERMARK_SKEW deve essere maggiore di zero")
	}
	return maxSkew, nil
}

func loadCloudWindowSize() (
	time.Duration,
	error,
) {
	value := envutil.OrDefault(
		"CLOUD_WINDOW_SIZE",
		cloudworker.DefaultWindowSize.String(),
	)

	windowSize, err := time.ParseDuration(
		value,
	)
	if err != nil {
		return 0,
			fmt.Errorf(
				"CLOUD_WINDOW_SIZE non valida %q: %w",
				value,
				err,
			)
	}

	if windowSize <= 0 {
		return 0,
			fmt.Errorf(
				"CLOUD_WINDOW_SIZE deve essere maggiore di zero",
			)
	}

	return windowSize, nil
}

func loadInputTopic() string {
	if value := strings.TrimSpace(
		os.Getenv("KAFKA_INPUT_TOPIC"),
	); value != "" {
		return value
	}

	if value := strings.TrimSpace(
		os.Getenv("KAFKA_TOPIC"),
	); value != "" {
		fmt.Println(
			"KAFKA_TOPIC e deprecata per il Cloud Worker; usare KAFKA_INPUT_TOPIC",
		)

		return value
	}

	return "edge-aggregates"
}

func loadWorkerID() string {
	if value := strings.TrimSpace(
		os.Getenv("WORKER_ID"),
	); value != "" {
		return value
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "cloud-worker"
	}

	return hostname
}
