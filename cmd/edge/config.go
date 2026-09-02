package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const defaultEdgeIngressQueueCapacity = 1000

type EdgeConfig struct {
	EdgeID               string
	MQTTBroker           string
	KafkaBroker          string
	KafkaTopic           string
	WindowSize           time.Duration
	IngressQueueCapacity int
}

func loadEdgeConfig(getenv func(string) string) (EdgeConfig, error) {
	edgeID := strings.TrimSpace(
		getenv("EDGE_ID"),
	)
	if edgeID == "" {
		return EdgeConfig{}, fmt.Errorf("variabile EDGE_ID non impostata")
	}

	mqttBroker := strings.TrimSpace(
		getenv("MQTT_BROKER"),
	)
	if mqttBroker == "" {
		return EdgeConfig{}, fmt.Errorf("variabile MQTT_BROKER non impostata")
	}

	kafkaBroker := strings.TrimSpace(
		getenv("KAFKA_BROKER"),
	)
	if kafkaBroker == "" {
		return EdgeConfig{}, fmt.Errorf("variabile KAFKA_BROKER non impostata")
	}

	kafkaTopic := strings.TrimSpace(
		getenv("KAFKA_TOPIC"),
	)
	if kafkaTopic == "" {
		return EdgeConfig{}, fmt.Errorf("variabile KAFKA_TOPIC non impostata")
	}

	windowSize, err := loadWindowSize(getenv)
	if err != nil {
		return EdgeConfig{}, err
	}

	ingressCapacity, err := loadEdgeIngressQueueCapacity(getenv)
	if err != nil {
		return EdgeConfig{}, err
	}

	return EdgeConfig{
		EdgeID:               edgeID,
		MQTTBroker:           mqttBroker,
		KafkaBroker:          kafkaBroker,
		KafkaTopic:           kafkaTopic,
		WindowSize:           windowSize,
		IngressQueueCapacity: ingressCapacity,
	}, nil
}

func loadWindowSize(
	getenv func(string) string,
) (
	time.Duration,
	error,
) {
	value := strings.TrimSpace(
		getenv("WINDOW_SIZE"),
	)

	if value == "" {
		return 5 * time.Minute, nil
	}

	windowSize, err := time.ParseDuration(
		value,
	)
	if err != nil {
		return 0,
			fmt.Errorf(
				"WINDOW_SIZE non valida %q: %w",
				value,
				err,
			)
	}

	if windowSize <= 0 {
		return 0,
			fmt.Errorf(
				"WINDOW_SIZE deve essere maggiore di zero",
			)
	}

	return windowSize, nil
}

func loadEdgeIngressQueueCapacity(
	getenv func(string) string,
) (int, error) {
	value := strings.TrimSpace(
		getenv("EDGE_INGRESS_QUEUE_CAPACITY"),
	)
	if value == "" {
		return defaultEdgeIngressQueueCapacity, nil
	}

	capacity, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf(
			"EDGE_INGRESS_QUEUE_CAPACITY non valida %q: %w",
			value,
			err,
		)
	}
	if capacity <= 0 {
		return 0, fmt.Errorf(
			"EDGE_INGRESS_QUEUE_CAPACITY deve essere maggiore di zero",
		)
	}

	return capacity, nil
}
