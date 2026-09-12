package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultEdgeIngressQueueCapacity = 1000
const defaultWindowSize = 5 * time.Minute

type EdgeConfig struct {
	EdgeID               string
	MQTTBroker           string
	KafkaBroker          string
	KafkaTopic           string
	WindowSize           time.Duration
	IngressQueueCapacity int
	CompletionURL        string
}

func loadEdgeConfig() (EdgeConfig, error) {
	edgeID := strings.TrimSpace(os.Getenv("EDGE_ID"))
	if edgeID == "" {
		return EdgeConfig{}, fmt.Errorf("variabile EDGE_ID non impostata")
	}

	mqttBroker := strings.TrimSpace(os.Getenv("MQTT_BROKER"))
	if mqttBroker == "" {
		return EdgeConfig{}, fmt.Errorf("variabile MQTT_BROKER non impostata")
	}

	kafkaBroker := strings.TrimSpace(os.Getenv("KAFKA_BROKER"))
	if kafkaBroker == "" {
		return EdgeConfig{}, fmt.Errorf("variabile KAFKA_BROKER non impostata")
	}

	kafkaTopic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if kafkaTopic == "" {
		return EdgeConfig{}, fmt.Errorf("variabile KAFKA_TOPIC non impostata")
	}

	windowSize, err := loadWindowSize()
	if err != nil {
		return EdgeConfig{}, err
	}

	ingressCapacity, err := loadEdgeIngressQueueCapacity()
	if err != nil {
		return EdgeConfig{}, err
	}

	completionURL := strings.TrimSpace(os.Getenv("EDGE_COMPLETION_URL"))
	if completionURL == "" {
		completionURL = "http://partition-coordinator:8081/completed"
	}
	endpoint, err := url.Parse(completionURL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil {
		return EdgeConfig{}, fmt.Errorf("EDGE_COMPLETION_URL must be an HTTP(S) endpoint without credentials")
	}

	return EdgeConfig{
		EdgeID:               edgeID,
		MQTTBroker:           mqttBroker,
		KafkaBroker:          kafkaBroker,
		KafkaTopic:           kafkaTopic,
		WindowSize:           windowSize,
		IngressQueueCapacity: ingressCapacity,
		CompletionURL:        completionURL,
	}, nil
}

func loadWindowSize() (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("WINDOW_SIZE"))

	if value == "" {
		return defaultWindowSize, nil
	}

	windowSize, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("WINDOW_SIZE non valida %q: %w", value, err)
	}

	if windowSize <= 0 {
		return 0, fmt.Errorf("WINDOW_SIZE deve essere maggiore di zero")
	}

	return windowSize, nil
}

func loadEdgeIngressQueueCapacity() (int, error) {
	value := strings.TrimSpace(os.Getenv("EDGE_INGRESS_QUEUE_CAPACITY"))
	if value == "" {
		return defaultEdgeIngressQueueCapacity, nil
	}

	capacity, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("EDGE_INGRESS_QUEUE_CAPACITY non valida %q: %w", value, err)
	}
	if capacity <= 0 {
		return 0, fmt.Errorf("EDGE_INGRESS_QUEUE_CAPACITY deve essere maggiore di zero")
	}

	return capacity, nil
}
