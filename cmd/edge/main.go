package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	if err := runEdge(); err != nil {
		panic(err)
	}
}

func runEdge() error {
	config, err := loadEdgeConfig(os.Getenv)
	if err != nil {
		return err
	}

	ingress, err := newEdgeIngressQueue(config.IngressQueueCapacity)
	if err != nil {
		return err
	}
	stats := &EdgeStats{}

	kafkaWriter := newKafkaWriter(
		config.KafkaBroker,
		config.KafkaTopic,
	)

	aggregator := &WindowAggregator{
		edgeID:     config.EdgeID,
		windowSize: config.WindowSize,
		kafkaTopic: config.KafkaTopic,
		publishMessage: func(
			ctx context.Context,
			message kafka.Message,
		) error {
			return kafkaWriter.WriteMessages(
				ctx,
				message,
			)
		},
		stats: stats,
	}

	processor := &EdgeProcessor{
		edgeID:     config.EdgeID,
		ingress:    ingress,
		aggregator: aggregator,
		stats:      stats,
		now:        time.Now,
	}
	processorDone := make(chan error, 1)
	go func() {
		processorDone <- processor.Run()
	}()

	readiness := &ReadinessState{}
	readinessServer, err := startReadinessServer(
		readiness,
		config.EdgeID,
	)
	if err != nil {
		return err
	}
	defer stopReadinessServer(
		readinessServer,
		config.EdgeID,
	)

	fmt.Printf(
		"Avvio Edge %s\n",
		config.EdgeID,
	)
	fmt.Printf(
		"Broker MQTT: %s\n",
		config.MQTTBroker,
	)
	fmt.Printf(
		"Window size: %s\n",
		config.WindowSize,
	)
	fmt.Printf(
		"Kafka broker: %s\n",
		config.KafkaBroker,
	)
	fmt.Printf(
		"Kafka topic: %s\n\n",
		config.KafkaTopic,
	)
	fmt.Printf(
		"Edge ingress queue capacity: %d\n\n",
		config.IngressQueueCapacity,
	)

	subscriptions := &SubscriptionCoordinator{}
	client, err := connectEdgeMQTTClient(
		config,
		ingress,
		stats,
		readiness,
		subscriptions,
	)
	if err != nil {
		return err
	}

	shutdownContext, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	processorErr, processorFinished := waitForShutdown(
		shutdownContext,
		processorDone,
	)

	fmt.Printf(
		"\nArresto %s...\n",
		config.EdgeID,
	)

	readiness.MarkNotReady()
	subscriptions.Invalidate()
	client.Disconnect(250)
	ingress.Close()
	if !processorFinished {
		processorErr = <-processorDone
	}

	if processorErr == nil {
		processorErr = aggregator.Flush()
	}
	if processorErr != nil {
		fmt.Printf(
			"%s: processing fallito: %v\n",
			config.EdgeID,
			processorErr,
		)
	}

	if err := kafkaWriter.Close(); err != nil {
		fmt.Printf(
			"%s: errore chiusura Kafka writer: %v\n",
			config.EdgeID,
			err,
		)
	}

	printEdgeSummary(
		config.EdgeID,
		stats.SnapshotWithQueue(ingress),
	)

	return processorErr
}

func newKafkaWriter(
	broker string,
	topic string,
) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(
			broker,
		),

		Topic: topic,

		Balancer: &kafka.Hash{},

		RequiredAcks: kafka.RequireAll,

		MaxAttempts: 1,

		BatchSize: 1,

		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,

		Async: false,
	}
}

func waitForShutdown(
	ctx context.Context,
	processorDone <-chan error,
) (processorErr error, processorFinished bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case err := <-processorDone:
		return err, true
	}
}
