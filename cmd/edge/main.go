package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

	readiness := &ReadinessState{}
	readinessServer, err := startReadinessServer(readiness, config.EdgeID)
	if err != nil {
		return err
	}
	defer stopReadinessServer(readinessServer, config.EdgeID)

	ingress := newEdgeIngress(config.IngressQueueCapacity)
	stats := &EdgeStats{}

	kafkaWriter := newKafkaWriter(config.KafkaBroker, config.KafkaTopic)
	writerClosed := false
	defer func() {
		if writerClosed {
			return
		}
		if err := kafkaWriter.Close(); err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka writer: %v\n",
				config.EdgeID,
				err,
			)
		}
	}()

	aggregator := &WindowAggregator{
		edgeID:     config.EdgeID,
		windowSize: config.WindowSize,
	}

	pipeline := startEdgePipeline(ingress, aggregator, kafkaWriter, stats)

	fmt.Printf("Avvio Edge %s\n", config.EdgeID)
	fmt.Printf("Broker MQTT: %s\n", config.MQTTBroker)
	fmt.Printf("Window size: %s\n", config.WindowSize)
	fmt.Printf("Kafka broker: %s\n", config.KafkaBroker)
	fmt.Printf("Kafka topic: %s\n\n", config.KafkaTopic)
	fmt.Printf(
		"Edge ingress queue capacity: %d\n\n",
		config.IngressQueueCapacity,
	)

	subscriptions := &SubscriptionCoordinator{}

	client, connectErr := connectEdgeMQTTClient(config, ingress, stats, readiness, subscriptions)
	if connectErr == nil {
		shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		pipeline.waitForTermination(shutdownContext)
		fmt.Printf("\nArresto %s...\n", config.EdgeID)
	}

	stopEdgeIngress(ingress, client, readiness, subscriptions)
	pipelineErr := pipeline.drain(config.EdgeID, connectErr != nil)
	if connectErr != nil {
		return connectErr
	}

	printEdgeSummary(config.EdgeID, stats.SnapshotWithQueue(ingress))
	printEdgeStatsJSON(config.EdgeID, stats.SnapshotWithQueue(ingress))
	if pipelineErr != nil {
		return pipelineErr
	}
	if stats.endOfReplayProcessed.Load() != 1 {
		return fmt.Errorf("Edge %s stopped without Simulator EOS; no completion notified", config.EdgeID)
	}
	// All synchronous WriteMessages calls have succeeded and the producer is closed
	// before control-plane completion can authorize a source-partition terminal marker.
	writerClosed = true
	if err := kafkaWriter.Close(); err != nil {
		return fmt.Errorf("Edge %s: Kafka writer close failed: %w", config.EdgeID, err)
	}
	if err := notifyEdgeCompletion(context.Background(), config.CompletionURL, config.EdgeID); err != nil {
		return err
	}
	fmt.Printf("EDGE_PRODUCTION_COMPLETED edge=%s\n", config.EdgeID)
	return nil
}
