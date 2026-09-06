package main

import (
	"fmt"
	"strings"

	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

type CloudMessageProcessor struct {
	aggregator     *cloudworker.WindowAggregator
	outputTopic    string
	workerID       string
	publishMessage KafkaMessagePublisher
	endedEdges     map[string]bool
}

func (processor *CloudMessageProcessor) Process(message kafka.Message) error {
	recordType, err := kafkautil.ParseRecordType(message.Headers)
	if err != nil {
		return err
	}

	switch recordType {
	case model.RecordTypeEdgeAggregate:
		return processor.processEdgeAggregate(message)

	case model.RecordTypeEndOfReplay:
		return processor.processEndOfReplay(message)

	default:
		return fmt.Errorf(
			"record_type Kafka sconosciuto %q",
			recordType,
		)
	}
}

func (processor *CloudMessageProcessor) processEdgeAggregate(message kafka.Message) error {
	input, err := decodeEdgeAggregate(message.Value)
	if err != nil {
		return err
	}
	if string(message.Key) != input.EdgeID {
		return fmt.Errorf("EdgeAggregate key Kafka=%q non coerente con edge_id=%q", message.Key, input.EdgeID)
	}

	if processor.endedEdges == nil {
		processor.endedEdges = make(map[string]bool)
	}
	if processor.endedEdges[input.EdgeID] {
		return fmt.Errorf("violazione invariant terminale: EdgeAggregate %s ricevuto dopo EndOfReplay edge=%s", input.AggregateID, input.EdgeID)
	}

	output, err := processor.aggregator.Add(input)
	if err != nil {
		return fmt.Errorf("elaborazione aggregate_id=%s fallita: %w", input.AggregateID, err)
	}

	if output == nil {
		return nil
	}

	return processor.publishCloudAggregate(*output, false)
}

func (processor *CloudMessageProcessor) processEndOfReplay(message kafka.Message) error {
	edgeID := string(message.Key)
	if strings.TrimSpace(edgeID) == "" {
		return fmt.Errorf("key Kafka EOS mancante o vuota")
	}

	if processor.endedEdges == nil {
		processor.endedEdges = make(map[string]bool)
	}
	if processor.endedEdges[edgeID] {
		fmt.Printf("%s: EndOfReplay duplicato edge=%s ignorato\n", processor.workerID, edgeID)
		return nil
	}

	if output, found := processor.aggregator.FlushEdge(edgeID); found {
		if err := processor.publishCloudAggregate(*output, true); err != nil {
			return fmt.Errorf("flush finale Cloud edge=%s fallito: %w", edgeID, err)
		}
	}

	if err := processor.publishEndOfReplay(edgeID); err != nil {
		return err
	}

	processor.endedEdges[edgeID] = true

	return nil
}

func decodeEdgeAggregate(payload []byte) (model.EdgeAggregate, error) {
	aggregate, err := avrocodec.DecodeEdgeAggregate(payload)
	if err != nil {
		return model.EdgeAggregate{}, fmt.Errorf("EdgeAggregate Avro non valido: %w", err)
	}

	if err := cloudworker.ValidateEdgeAggregate(aggregate); err != nil {
		return model.EdgeAggregate{}, err
	}

	return aggregate, nil
}
