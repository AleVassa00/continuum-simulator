package main

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

// Stato dei due stadi concorrenti dell'Edge.
type edgePipeline struct {
	processorDone <-chan error
	kafkaDone     <-chan error

	processorErr error
	kafkaErr     error

	processorFinished bool
	kafkaFinished     bool
}

func startEdgePipeline(
	ingress *EdgeIngress,
	aggregator *WindowAggregator,
	kafkaWriter *kafka.Writer,
	stats *EdgeStats,
	batchSize int,
	batchMaxWait time.Duration,
) *edgePipeline {
	output := make(chan EdgeOutputRecord)
	egressStopped := make(chan struct{})

	kafkaEgress := &KafkaEgress{
		edgeID:        aggregator.edgeID,
		topic:         kafkaWriter.Topic,
		writeMessages: kafkaWriter.WriteMessages,
		input:         output,
		stats:         stats,
		batchSize:     batchSize,
		batchMaxWait:  batchMaxWait,
	}

	kafkaDone := make(chan error, 1)
	go func() {
		err := kafkaEgress.Run()
		close(egressStopped)
		kafkaDone <- err
	}()

	processorDone := make(chan error, 1)
	go func() {
		processorDone <- runEdgeLoop(ingress, aggregator, output, egressStopped, stats)
	}()

	return &edgePipeline{
		processorDone: processorDone,
		kafkaDone:     kafkaDone,
	}
}

func (pipeline *edgePipeline) waitForTermination(shutdownContext context.Context) {
	select {
	case <-shutdownContext.Done():

	case pipeline.processorErr = <-pipeline.processorDone:
		pipeline.processorFinished = true

	case pipeline.kafkaErr = <-pipeline.kafkaDone:
		pipeline.kafkaFinished = true
	}
}

func stopEdgeIngress(ingress *EdgeIngress, client mqtt.Client, readiness *ReadinessState, subscriptions *SubscriptionCoordinator) {
	readiness.MarkNotReady()
	subscriptions.Invalidate()

	if client != nil {
		client.Disconnect(250)
	}

	// Chiude l'ingresso dopo aver invalidato le callback MQTT.
	ingress.Close()
}

func (pipeline *edgePipeline) drain(edgeID string, startupFailed bool) error {
	if !pipeline.processorFinished {
		pipeline.processorErr = <-pipeline.processorDone
	}

	if !pipeline.kafkaFinished {
		pipeline.kafkaErr = <-pipeline.kafkaDone
	}

	phase := ""
	if startupFailed {
		phase = " durante startup"
	}

	if pipeline.processorErr != nil {
		fmt.Printf("%s: processing fallito%s: %v\n", edgeID, phase, pipeline.processorErr)
	}

	if pipeline.kafkaErr != nil {
		fmt.Printf("%s: Kafka egress fallito%s: %v\n", edgeID, phase, pipeline.kafkaErr)
	}

	if pipeline.processorErr != nil {
		return pipeline.processorErr
	}

	return pipeline.kafkaErr
}
