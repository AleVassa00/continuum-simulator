package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"continuum/internal/experiment"
)

const defaultEdgeIngressQueueCapacity = 1000
const defaultWindowSize = 5 * time.Minute

type EdgeConfig struct {
	EdgeID                    string
	MQTTBroker                string
	KafkaBroker               string
	KafkaTopic                string
	KafkaPartition            int
	SourcePartitionCount      int
	KafkaProducerBatchSize    int
	KafkaProducerBatchMaxWait time.Duration
	WindowSize                time.Duration
	IngressQueueCapacity      int
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
	kafkaPartition, sourcePartitionCount, err := loadKafkaPartitionAssignment()
	if err != nil {
		return EdgeConfig{}, err
	}
	kafkaProducerBatchSize, err := loadPositiveInteger(
		"KAFKA_PRODUCER_BATCH_SIZE",
		experiment.DefaultEdgeKafkaProducerBatchSize,
	)
	if err != nil {
		return EdgeConfig{}, err
	}
	kafkaProducerBatchMaxWait, err := loadPositiveDuration(
		"KAFKA_PRODUCER_BATCH_MAX_WAIT",
		experiment.DefaultEdgeKafkaProducerBatchMaxWait,
	)
	if err != nil {
		return EdgeConfig{}, err
	}

	return EdgeConfig{
		EdgeID:                    edgeID,
		MQTTBroker:                mqttBroker,
		KafkaBroker:               kafkaBroker,
		KafkaTopic:                kafkaTopic,
		KafkaPartition:            kafkaPartition,
		SourcePartitionCount:      sourcePartitionCount,
		KafkaProducerBatchSize:    kafkaProducerBatchSize,
		KafkaProducerBatchMaxWait: kafkaProducerBatchMaxWait,
		WindowSize:                windowSize,
		IngressQueueCapacity:      ingressCapacity,
	}, nil
}

func loadKafkaPartitionAssignment() (int, int, error) {
	partitionValue := strings.TrimSpace(os.Getenv("KAFKA_PARTITION"))
	if partitionValue == "" {
		return 0, 0, fmt.Errorf("variabile KAFKA_PARTITION non impostata")
	}
	partition, err := strconv.Atoi(partitionValue)
	if err != nil {
		return 0, 0, fmt.Errorf("KAFKA_PARTITION non valida %q: %w", partitionValue, err)
	}

	countValue := strings.TrimSpace(os.Getenv("SOURCE_PARTITION_COUNT"))
	if countValue == "" {
		return 0, 0, fmt.Errorf("variabile SOURCE_PARTITION_COUNT non impostata")
	}
	count, err := strconv.Atoi(countValue)
	if err != nil || count <= 0 {
		return 0, 0, fmt.Errorf("SOURCE_PARTITION_COUNT deve essere un intero positivo")
	}
	if partition < 0 || partition >= count {
		return 0, 0, fmt.Errorf("KAFKA_PARTITION %d fuori dall'intervallo [0,%d)", partition, count)
	}

	return partition, count, nil
}

func loadPositiveInteger(name string, defaultValue int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s deve essere un intero positivo", name)
	}
	return parsed, nil
}

func loadPositiveDuration(name string, defaultValue time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s non valida %q: %w", name, value, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s deve essere maggiore di zero", name)
	}
	return parsed, nil
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
