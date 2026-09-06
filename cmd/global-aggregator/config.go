package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"continuum/internal/envutil"
)

const defaultGlobalEdgeIdleTimeout = 5 * time.Second

type GlobalAggregatorConfig struct {
	KafkaBroker     string
	InputTopic      string
	GroupID         string
	WindowSize      time.Duration
	WatermarkDelay  time.Duration
	EdgeIdleTimeout time.Duration
	ExpectedEdgeIDs []string
}

func loadGlobalAggregatorConfig() (GlobalAggregatorConfig, error) {
	kafkaBroker := envutil.Required("KAFKA_BROKER")
	inputTopic := envutil.OrDefault(
		"KAFKA_INPUT_TOPIC",
		"cloud-edge-aggregates",
	)
	groupID := envutil.OrDefault(
		"KAFKA_GROUP_ID",
		"global-aggregator",
	)
	windowSize, err := loadGlobalWindowSize()
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}
	watermarkDelay, err := loadGlobalWatermarkDelay(windowSize)
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}
	edgeIdleTimeout, err := loadGlobalEdgeIdleTimeout()
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}
	expectedEdgeIDs, err := loadExpectedEdgeIDs()
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}

	return GlobalAggregatorConfig{
		KafkaBroker:     kafkaBroker,
		InputTopic:      inputTopic,
		GroupID:         groupID,
		WindowSize:      windowSize,
		WatermarkDelay:  watermarkDelay,
		EdgeIdleTimeout: edgeIdleTimeout,
		ExpectedEdgeIDs: expectedEdgeIDs,
	}, nil
}

func loadGlobalWindowSize() (time.Duration, error) {
	value := envutil.OrDefault("GLOBAL_WINDOW_SIZE", "15m")
	windowSize, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"GLOBAL_WINDOW_SIZE non valida %q: %w",
			value,
			err,
		)
	}
	if windowSize <= 0 {
		return 0, fmt.Errorf(
			"GLOBAL_WINDOW_SIZE deve essere maggiore di zero",
		)
	}
	return windowSize, nil
}

func loadGlobalWatermarkDelay(
	defaultDelay time.Duration,
) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("GLOBAL_WATERMARK_DELAY"))
	if value == "" {
		return defaultDelay, nil
	}
	delay, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("GLOBAL_WATERMARK_DELAY non valida %q: %w", value, err)
	}
	if delay <= 0 {
		return 0, fmt.Errorf("GLOBAL_WATERMARK_DELAY deve essere maggiore di zero")
	}
	return delay, nil
}

func loadGlobalEdgeIdleTimeout() (time.Duration, error) {
	value := envutil.OrDefault(
		"GLOBAL_EDGE_IDLE_TIMEOUT",
		defaultGlobalEdgeIdleTimeout.String(),
	)
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"GLOBAL_EDGE_IDLE_TIMEOUT non valido %q: %w",
			value,
			err,
		)
	}
	if timeout <= 0 {
		return 0, fmt.Errorf(
			"GLOBAL_EDGE_IDLE_TIMEOUT deve essere maggiore di zero",
		)
	}
	return timeout, nil
}

func loadExpectedEdgeIDs() ([]string, error) {
	value := strings.TrimSpace(os.Getenv("EXPECTED_EDGE_IDS"))
	if value == "" {
		return nil, fmt.Errorf("variabile EXPECTED_EDGE_IDS non impostata")
	}
	parts := strings.Split(value, ",")
	edgeIDs := make([]string, len(parts))
	for index, part := range parts {
		edgeIDs[index] = strings.TrimSpace(part)
	}
	return edgeIDs, nil
}
