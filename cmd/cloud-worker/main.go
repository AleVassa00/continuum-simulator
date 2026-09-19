package main

import (
	"context"
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

	if err := validateKafkaTopology(ctx, config.KafkaBroker, config.InputTopic, config.SourcePartitionCount); err != nil {
		return err
	}
	writer := newKafkaWriter(config.KafkaOutputBroker, config.OutputTopic)
	defer writer.Close()

	fmt.Printf("Avvio Cloud Worker %s: %s@%s -> %s@%s window=%s max_edge_watermark_skew=%s partitions=%d\n", config.WorkerID, config.InputTopic, config.KafkaBroker, config.OutputTopic, config.KafkaOutputBroker, config.WindowSize, config.MaxEdgeWatermarkSkew, config.SourcePartitionCount)
	// Solo progresso ed EOS chiudono le finestre aperte.
	return consume(ctx, config, func(ctx context.Context, m kafka.Message) error { return writer.WriteMessages(ctx, m) })
}
