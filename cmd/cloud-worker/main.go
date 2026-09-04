package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/envutil"
)

func main() {
	if err := runCloudWorker(); err != nil {
		panic(err)
	}
}

func runCloudWorker() error {
	kafkaBroker := envutil.Required("KAFKA_BROKER")

	inputTopic := loadInputTopic()
	outputTopic := envutil.OrDefault("KAFKA_OUTPUT_TOPIC", "cloud-edge-aggregates")
	groupID := envutil.OrDefault("KAFKA_GROUP_ID", "cloud-workers")
	workerID := loadWorkerID()
	windowSize, err := loadCloudWindowSize()
	if err != nil {
		return err
	}

	aggregator, err := cloudworker.NewWindowAggregator(
		windowSize,
	)
	if err != nil {
		return err
	}

	reader := newKafkaReader(
		kafkaBroker,
		inputTopic,
		groupID,
	)
	defer func() {
		if err := reader.Close(); err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka reader: %v\n",
				workerID,
				err,
			)
		}
	}()

	writer := newKafkaWriter(
		kafkaBroker,
		outputTopic,
	)
	defer func() {
		if err := writer.Close(); err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka writer: %v\n",
				workerID,
				err,
			)
		}
	}()

	fmt.Printf(
		"Avvio Cloud Worker %s\n",
		workerID,
	)
	fmt.Printf(
		"Kafka broker: %s\n",
		kafkaBroker,
	)
	fmt.Printf(
		"Input topic: %s\n",
		inputTopic,
	)
	fmt.Printf(
		"Output topic: %s\n",
		outputTopic,
	)
	fmt.Printf(
		"Cloud window: %s\n",
		windowSize,
	)
	fmt.Printf(
		"Consumer group: %s\n\n",
		groupID,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := consume(
		ctx,
		reader,
		writer,
		aggregator,
		workerID,
	); err != nil {
		return err
	}

	if err := flushWindows(
		writer,
		aggregator,
		workerID,
	); err != nil {
		return err
	}

	fmt.Printf(
		"\nArresto Cloud Worker %s\n",
		workerID,
	)

	return nil
}

func loadCloudWindowSize() (
	time.Duration,
	error,
) {
	value := envutil.OrDefault(
		"CLOUD_WINDOW_SIZE",
		"15m",
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
