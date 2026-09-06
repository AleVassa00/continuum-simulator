package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"continuum/internal/cloudworker"
	"continuum/internal/globalaggregator"
	"continuum/internal/kafkautil"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

type GlobalMessageProcessor struct {
	aggregator *globalaggregator.Aggregator
}

func (processor *GlobalMessageProcessor) Process(
	ctx context.Context,
	message kafka.Message,
) (bool, error) {
	recordType, err := kafkautil.ParseRecordType(message.Headers)
	if err != nil {
		return false, err
	}

	switch recordType {
	case model.RecordTypeCloudEdgeAggregate:
		input, err := decodeCloudEdgeAggregate(message.Value)
		if err != nil {
			return false, err
		}
		if string(message.Key) != input.EdgeID {
			return false, fmt.Errorf(
				"CloudEdgeAggregate key Kafka=%q non coerente con edge_id=%q",
				message.Key,
				input.EdgeID,
			)
		}
		if err := processor.aggregator.Add(ctx, input); err != nil {
			if errors.Is(err, globalaggregator.ErrClosedWindow) {
				fmt.Printf("Global Aggregator: late aggregate scartato (%v)\n", err)
				return false, nil
			}
			return false, err
		}
		return false, nil

	case model.RecordTypeEndOfReplay:
		return processor.aggregator.EndReplay(ctx, string(message.Key))

	default:
		return false, fmt.Errorf(
			"record_type Kafka globale sconosciuto %q",
			recordType,
		)
	}
}

func decodeCloudEdgeAggregate(payload []byte) (model.CloudEdgeAggregate, error) {
	var aggregate model.CloudEdgeAggregate
	if err := json.Unmarshal(payload, &aggregate); err != nil {
		return model.CloudEdgeAggregate{}, fmt.Errorf(
			"CloudEdgeAggregate JSON non valido: %w",
			err,
		)
	}
	if err := cloudworker.ValidateCloudEdgeAggregate(aggregate); err != nil {
		return model.CloudEdgeAggregate{}, err
	}
	return aggregate, nil
}
