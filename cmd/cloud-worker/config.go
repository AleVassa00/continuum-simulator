package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/envutil"
	"continuum/internal/model"
	"strconv"
)

type CloudWorkerConfig struct {
	KafkaBroker    string
	InputTopic     string
	OutputTopic    string
	GroupID        string
	WorkerID       string
	WindowSize     time.Duration
	WatermarkDelay time.Duration
}

func loadCloudWorkerConfig() (CloudWorkerConfig, error) {
	kafkaBroker := envutil.Required("KAFKA_BROKER")

	inputTopic := loadInputTopic()
	outputTopic := envutil.OrDefault("KAFKA_OUTPUT_TOPIC", "cloud-partition-aggregates")
	groupID := envutil.OrDefault("KAFKA_GROUP_ID", "cloud-workers")
	workerID := loadWorkerID()
	windowSize, err := loadCloudWindowSize()
	if err != nil {
		return CloudWorkerConfig{}, err
	}
	count, err := strconv.Atoi(envutil.OrDefault("SOURCE_PARTITION_COUNT", "6"))
	if err != nil || count != model.SourcePartitionCount {
		return CloudWorkerConfig{}, fmt.Errorf("SOURCE_PARTITION_COUNT must be 6")
	}
	watermarkDelay, err := loadCloudWatermarkDelay()
	if err != nil {
		return CloudWorkerConfig{}, err
	}

	return CloudWorkerConfig{
		KafkaBroker:    kafkaBroker,
		InputTopic:     inputTopic,
		OutputTopic:    outputTopic,
		GroupID:        groupID,
		WorkerID:       workerID,
		WindowSize:     windowSize,
		WatermarkDelay: watermarkDelay,
	}, nil
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

func loadCloudWatermarkDelay() (time.Duration, error) {
	value := envutil.OrDefault("CLOUD_WATERMARK_DELAY", cloudworker.DefaultWatermarkDelay.String())
	delay, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid CLOUD_WATERMARK_DELAY %q: %w", value, err)
	}
	if delay < 0 {
		return 0, fmt.Errorf("CLOUD_WATERMARK_DELAY must be nonnegative")
	}
	return delay, nil
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
