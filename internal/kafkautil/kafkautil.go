package kafkautil

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

// ParseRecordType estrae il tipo di record dagli header di un messaggio Kafka.
func ParseRecordType(headers []kafka.Header) (string, error) {
	var recordType string
	found := false

	for _, header := range headers {
		if header.Key != model.RecordTypeHeader {
			continue
		}

		if found {
			return "", fmt.Errorf(
				"header Kafka %q duplicato",
				model.RecordTypeHeader,
			)
		}

		recordType = strings.TrimSpace(string(header.Value))
		found = true
	}

	if !found || recordType == "" {
		return "", fmt.Errorf(
			"header Kafka %q mancante o vuoto",
			model.RecordTypeHeader,
		)
	}

	return recordType, nil
}

// DecodeEndOfReplay deserializza e valida un record EndOfReplay dal payload Kafka.
func DecodeEndOfReplay(payload []byte) (model.EndOfReplay, error) {
	var record model.EndOfReplay
	if err := json.Unmarshal(payload, &record); err != nil {
		return model.EndOfReplay{},
			fmt.Errorf(
				"EndOfReplay JSON non valido: %w",
				err,
			)
	}

	if err := model.ValidateEndOfReplay(record); err != nil {
		return model.EndOfReplay{}, err
	}

	return record, nil
}

// CommitMessage esegue il commit di un singolo messaggio Kafka con il timeout specificato.
func CommitMessage(
	reader *kafka.Reader,
	message kafka.Message,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		timeout,
	)
	defer cancel()

	return reader.CommitMessages(ctx, message)
}
