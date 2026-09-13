package kafkautil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

// PartitionForEdge is used by orchestration, never by Cloud window completeness.
// It must match the Edge writer's kafka.Hash balancer on the configured partitions.
// count must have been validated as positive before starting orchestration.
func PartitionForEdge(edgeID string, count int) int {
	partitions := make([]int, count)
	for p := range partitions {
		partitions[p] = p
	}
	return (&kafka.Hash{}).Balance(kafka.Message{Key: []byte(edgeID)}, partitions...)
}

func ParseRecordType(headers []kafka.Header) (string, error) {
	var recordType string

	for _, header := range headers {
		if header.Key != model.RecordTypeHeader {
			continue
		}
		recordType = strings.TrimSpace(string(header.Value))
	}
	if recordType == "" {
		return "", fmt.Errorf("header Kafka %q vuoto", model.RecordTypeHeader)
	}
	return recordType, nil
}

// CommitMessage esegue il commit di un singolo messaggio Kafka con il timeout specificato.
func CommitMessage(reader *kafka.Reader, message kafka.Message, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return reader.CommitMessages(ctx, message)
}
