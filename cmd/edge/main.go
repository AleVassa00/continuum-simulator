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
	config, err := loadEdgeConfig()
	if err != nil {
		return err
	}

	ingress := newEdgeIngress(config.IngressQueueCapacity)
	stats := &EdgeStats{}

	kafkaWriter := newKafkaWriter(
		config.KafkaBroker,
		config.KafkaTopic,
	)

	output := make(chan EdgeOutputRecord)
	egressStopped := make(chan struct{})

	aggregator := &WindowAggregator{
		edgeID:        config.EdgeID,
		windowSize:    config.WindowSize,
		output:        output,
		egressStopped: egressStopped,
	}

	processor := &EdgeProcessor{
		edgeID:     config.EdgeID,
		ingress:    ingress,
		aggregator: aggregator,
		stats:      stats,
	}

	kafkaEgress := &KafkaEgress{
		edgeID: config.EdgeID,
		writer: kafkaWriter,
		input:  output,
		stats:  stats,
	}

	processorDone := make(chan error, 1)
	go func() {
		processorDone <- processor.Run()
	}()

	kafkaDone := make(chan error, 1)
	go func() {
		kafkaDone <- kafkaEgress.Run()
		close(egressStopped)
	}()

	readiness := &ReadinessState{}
	readinessServer, err := startReadinessServer(readiness, config.EdgeID)
	if err != nil {
		return err
	}
	defer stopReadinessServer(readinessServer, config.EdgeID)

	fmt.Printf("Avvio Edge %s\n", config.EdgeID)
	fmt.Printf("Broker MQTT: %s\n", config.MQTTBroker)
	fmt.Printf("Window size: %s\n", config.WindowSize)
	fmt.Printf("Kafka broker: %s\n", config.KafkaBroker)
	fmt.Printf("Kafka topic: %s\n\n", config.KafkaTopic)
	fmt.Printf("Edge ingress queue capacity: %d\n\n", config.IngressQueueCapacity)

	subscriptions := &SubscriptionCoordinator{}
	client, err := connectEdgeMQTTClient(config, ingress, stats, readiness, subscriptions)
	if err != nil {
		return err
	}

	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var processorErr error
	var kafkaErr error
	processorFinished := false
	kafkaFinished := false

	select {
	case <-shutdownContext.Done():
	case processorErr = <-processorDone:
		processorFinished = true
	case kafkaErr = <-kafkaDone:
		kafkaFinished = true
	}

	fmt.Printf("\nArresto %s...\n", config.EdgeID)

	readiness.MarkNotReady()
	subscriptions.Invalidate()
	client.Disconnect(250)
	ingress.Close()

	if !processorFinished {
		processorErr = <-processorDone
	}

	if processorErr == nil && kafkaErr == nil {
		processorErr = aggregator.Flush()
	}

	close(output)

	if !kafkaFinished {
		kafkaErr = <-kafkaDone
	}

	if processorErr != nil {
		fmt.Printf("%s: processing fallito: %v\n", config.EdgeID, processorErr)
	}
	if kafkaErr != nil {
		fmt.Printf("%s: Kafka egress fallito: %v\n", config.EdgeID, kafkaErr)
	}

	if err := kafkaWriter.Close(); err != nil {
		fmt.Printf("%s: errore chiusura Kafka writer: %v\n", config.EdgeID, err)
	}

	printEdgeSummary(config.EdgeID, stats.SnapshotWithQueue(ingress))

	if processorErr != nil {
		return processorErr
	}
	return kafkaErr
}

func newKafkaWriter(broker string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1,
		BatchSize:    1,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		Async:        false,
	}
}
