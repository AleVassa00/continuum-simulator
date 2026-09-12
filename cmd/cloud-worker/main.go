package main

import (
	"context"
	"continuum/internal/model"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := validateKafkaTopology(ctx, config.KafkaBroker, config.InputTopic, model.SourcePartitionCount); err != nil {
		return err
	}
	writer := newKafkaWriter(config.KafkaBroker, config.OutputTopic)
	defer writer.Close()

	fmt.Printf("Avvio Cloud Worker %s: %s -> %s window=%s partitions=%d\n", config.WorkerID, config.InputTopic, config.OutputTopic, config.WindowSize, model.SourcePartitionCount)
	// Shutdown does not certify open windows. Only deterministic progress/EOS can.
	return consume(ctx, config, func(ctx context.Context, m kafka.Message) error { return writer.WriteMessages(ctx, m) })
}
