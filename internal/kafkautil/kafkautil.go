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
