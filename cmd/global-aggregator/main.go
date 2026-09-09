package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"continuum/internal/globalaggregator"
	"continuum/internal/model"
)

func main() {
	config, err := loadGlobalAggregatorConfig()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Global sink: %s\n", config.SinkType)
	sink, cleanup, err := newGlobalAggregateSink(context.Background(), config)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	aggregator, err := globalaggregator.New(sink)
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
	fmt.Printf("Expected source partitions: %d\n", model.SourcePartitionCount)

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
