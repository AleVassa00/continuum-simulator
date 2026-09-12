package kafkautil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

// ParseRecordType estrae il tipo di record dagli header di un messaggio Kafka.
// PartitionForEdge is used by orchestration, never by Cloud window completeness.
// It must match the Edge writer's kafka.Hash balancer on the fixed six partitions.
func PartitionForEdge(edgeID string) int {
	return (&kafka.Hash{}).Balance(kafka.Message{Key: []byte(edgeID)}, 0, 1, 2, 3, 4, 5)
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
