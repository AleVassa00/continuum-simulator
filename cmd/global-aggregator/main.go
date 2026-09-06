package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"continuum/internal/globalaggregator"
)

func main() {
	config, err := loadGlobalAggregatorConfig()
	if err != nil {
		panic(err)
	}

	aggregator, err := globalaggregator.New(
		config.ExpectedEdgeIDs,
		config.WindowSize,
		config.WatermarkDelay,
		config.EdgeIdleTimeout,
		newJSONLogSink(os.Stdout),
	)
	if err != nil {
		panic(err)
	}
	processor := &GlobalMessageProcessor{aggregator: aggregator}

	reader := newKafkaReader(config.KafkaBroker, config.InputTopic, config.GroupID)
	defer func() {
		if err := reader.Close(); err != nil {
			fmt.Printf("Global Aggregator: errore chiusura Kafka reader: %v\n", err)
		}
	}()

	fmt.Println("Avvio Global Aggregator")
	fmt.Printf("Kafka broker: %s\n", config.KafkaBroker)
	fmt.Printf("Input topic: %s\n", config.InputTopic)
	fmt.Printf("Consumer group: %s\n", config.GroupID)
	fmt.Printf("Global window: %s\n", config.WindowSize)
	fmt.Printf("Watermark delay: %s\n", config.WatermarkDelay)
	fmt.Printf("Edge idle timeout: %s\n", config.EdgeIdleTimeout)
	fmt.Printf("Expected Edge: %s\n\n", strings.Join(config.ExpectedEdgeIDs, ","))

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	completed, err := consume(
		ctx,
		reader,
		processor,
		watermarkAdvanceCheckInterval(config.EdgeIdleTimeout),
	)
	if err != nil {
		panic(err)
	}
	if completed {
		fmt.Println("GLOBAL_REPLAY_COMPLETED")
		return
	}
	fmt.Println("Global Aggregator arrestato prima dell'EndOfReplay globale")
}
