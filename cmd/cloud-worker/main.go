package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"continuum/internal/cloudworker"

	"github.com/segmentio/kafka-go"
)

func main() {
	if err := runCloudWorker(); err != nil {
		panic(err)
	}
}

func runCloudWorker() error {
	config, err := loadCloudWorkerConfig()
	if err != nil {
		return err
	}

	aggregator, err := cloudworker.NewWindowAggregator(
		config.WindowSize,
	)
	if err != nil {
		return err
	}

	reader := newKafkaReader(
		config.KafkaBroker,
		config.InputTopic,
		config.GroupID,
	)
	defer func() {
		if err := reader.Close(); err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka reader: %v\n",
				config.WorkerID,
				err,
			)
		}
	}()

	writer := newKafkaWriter(
		config.KafkaBroker,
		config.OutputTopic,
	)
	defer func() {
		if err := writer.Close(); err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka writer: %v\n",
				config.WorkerID,
				err,
			)
		}
	}()

	fmt.Printf(
		"Avvio Cloud Worker %s\n",
		config.WorkerID,
	)
	fmt.Printf(
		"Kafka broker: %s\n",
		config.KafkaBroker,
	)
	fmt.Printf(
		"Input topic: %s\n",
		config.InputTopic,
	)
	fmt.Printf(
		"Output topic: %s\n",
		config.OutputTopic,
	)
	fmt.Printf(
		"Cloud window: %s\n",
		config.WindowSize,
	)
	fmt.Printf(
		"Consumer group: %s\n\n",
		config.GroupID,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	processor := &CloudMessageProcessor{
		aggregator:  aggregator,
		outputTopic: writer.Topic,
		workerID:    config.WorkerID,
		publishMessage: func(
			ctx context.Context,
			message kafka.Message,
		) error {
			return writer.WriteMessages(ctx, message)
		},
		endedEdges: make(map[string]bool),
	}

	if err := consume(
		ctx,
		reader,
		processor,
	); err != nil {
		return err
	}

	if err := flushWindows(
		processor,
	); err != nil {
		return err
	}

	fmt.Printf(
		"\nArresto Cloud Worker %s\n",
		config.WorkerID,
	)

	return nil
}
