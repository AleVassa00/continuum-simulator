package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

func main() {
	kafkaBroker := requiredEnv(
		"KAFKA_BROKER",
	)

	kafkaTopic := requiredEnv(
		"KAFKA_TOPIC",
	)

	groupID := strings.TrimSpace(
		os.Getenv("KAFKA_GROUP_ID"),
	)

	if groupID == "" {
		groupID = "cloud-workers"
	}

	workerID := strings.TrimSpace(
		os.Getenv("WORKER_ID"),
	)

	if workerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			workerID = "cloud-worker"
		} else {
			workerID = hostname
		}
	}

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				kafkaBroker,
			},

			Topic: kafkaTopic,

			GroupID: groupID,

			StartOffset: kafka.FirstOffset,

			MinBytes: 1,

			MaxBytes: 10 * 1024 * 1024,

			MaxWait: 500 * time.Millisecond,
		},
	)

	defer func() {
		err := reader.Close()
		if err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka reader: %v\n",
				workerID,
				err,
			)
		}
	}()

	fmt.Printf(
		"Avvio Cloud Worker %s\n",
		workerID,
	)

	fmt.Printf(
		"Kafka broker: %s\n",
		kafkaBroker,
	)

	fmt.Printf(
		"Kafka topic: %s\n",
		kafkaTopic,
	)

	fmt.Printf(
		"Consumer group: %s\n\n",
		groupID,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	err := consume(
		ctx,
		reader,
		workerID,
	)

	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"\nArresto Cloud Worker %s\n",
		workerID,
	)
}

func consume(
	ctx context.Context,
	reader *kafka.Reader,
	workerID string,
) error {
	for {
		message, err := reader.FetchMessage(
			ctx,
		)

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf(
				"lettura Kafka fallita: %w",
				err,
			)
		}

		aggregate, err := decodeEdgeAggregate(
			message.Value,
		)
		if err != nil {
			return fmt.Errorf(
				"worker=%s partition=%d offset=%d: %w",
				workerID,
				message.Partition,
				message.Offset,
				err,
			)
		}

		err = processEdgeAggregate(
			workerID,
			message,
			aggregate,
		)
		if err != nil {
			return fmt.Errorf(
				"elaborazione aggregate_id=%s fallita: %w",
				aggregate.AggregateID,
				err,
			)
		}

		err = reader.CommitMessages(
			ctx,
			message,
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf(
				"commit Kafka partition=%d offset=%d fallito: %w",
				message.Partition,
				message.Offset,
				err,
			)
		}
	}
}

func decodeEdgeAggregate(
	payload []byte,
) (model.EdgeAggregate, error) {
	var aggregate model.EdgeAggregate

	err := json.Unmarshal(
		payload,
		&aggregate,
	)
	if err != nil {
		return model.EdgeAggregate{},
			fmt.Errorf(
				"EdgeAggregate JSON non valido: %w",
				err,
			)
	}

	err = validateEdgeAggregate(
		aggregate,
	)
	if err != nil {
		return model.EdgeAggregate{},
			err
	}

	return aggregate, nil
}

func validateEdgeAggregate(
	aggregate model.EdgeAggregate,
) error {
	if aggregate.SchemaVersion != 1 {
		return fmt.Errorf(
			"schema_version non supportata: %d",
			aggregate.SchemaVersion,
		)
	}

	if strings.TrimSpace(
		aggregate.AggregateID,
	) == "" {
		return fmt.Errorf(
			"aggregate_id mancante",
		)
	}

	if strings.TrimSpace(
		aggregate.EdgeID,
	) == "" {
		return fmt.Errorf(
			"edge_id mancante",
		)
	}

	if aggregate.WindowStart.IsZero() {
		return fmt.Errorf(
			"window_start mancante",
		)
	}

	if aggregate.WindowEnd.IsZero() {
		return fmt.Errorf(
			"window_end mancante",
		)
	}

	if !aggregate.WindowEnd.After(
		aggregate.WindowStart,
	) {
		return fmt.Errorf(
			"finestra non valida: start=%s end=%s",
			aggregate.WindowStart.Format(time.RFC3339),
			aggregate.WindowEnd.Format(time.RFC3339),
		)
	}

	if aggregate.Events == 0 {
		return fmt.Errorf(
			"aggregato senza eventi",
		)
	}

	err := validateMetricAggregate(
		"temperature",
		aggregate.Events,
		aggregate.Temperature,
	)
	if err != nil {
		return err
	}

	err = validateMetricAggregate(
		"humidity",
		aggregate.Events,
		aggregate.Humidity,
	)
	if err != nil {
		return err
	}

	err = validateMetricAggregate(
		"pressure",
		aggregate.Events,
		aggregate.Pressure,
	)
	if err != nil {
		return err
	}

	return nil
}

func validateMetricAggregate(
	name string,
	events uint64,
	metric model.MetricAggregate,
) error {
	if metric.Valid+metric.Invalid != events {
		return fmt.Errorf(
			"%s: valid(%d) + invalid(%d) != events(%d)",
			name,
			metric.Valid,
			metric.Invalid,
			events,
		)
	}

	if metric.Valid == 0 {
		if metric.Average != nil ||
			metric.Min != nil ||
			metric.Max != nil {
			return fmt.Errorf(
				"%s: statistiche presenti senza misure valide",
				name,
			)
		}

		return nil
	}

	if metric.Average == nil ||
		metric.Min == nil ||
		metric.Max == nil {
		return fmt.Errorf(
			"%s: statistiche mancanti con %d misure valide",
			name,
			metric.Valid,
		)
	}

	if *metric.Min > *metric.Max {
		return fmt.Errorf(
			"%s: min %.2f maggiore di max %.2f",
			name,
			*metric.Min,
			*metric.Max,
		)
	}

	if *metric.Average < *metric.Min ||
		*metric.Average > *metric.Max {
		return fmt.Errorf(
			"%s: average %.2f fuori dal range [%.2f, %.2f]",
			name,
			*metric.Average,
			*metric.Min,
			*metric.Max,
		)
	}

	return nil
}

func processEdgeAggregate(
	workerID string,
	message kafka.Message,
	aggregate model.EdgeAggregate,
) error {
	fmt.Printf(
		"CLOUD_PROCESSED worker=%s partition=%d offset=%d edge=%s aggregate_id=%s window=[%s,%s) events=%d\n",
		workerID,
		message.Partition,
		message.Offset,
		aggregate.EdgeID,
		aggregate.AggregateID,
		aggregate.WindowStart.Format(time.RFC3339),
		aggregate.WindowEnd.Format(time.RFC3339),
		aggregate.Events,
	)

	return nil
}

func requiredEnv(
	name string,
) string {
	value := strings.TrimSpace(
		os.Getenv(name),
	)

	if value == "" {
		panic(
			fmt.Sprintf(
				"variabile %s non impostata",
				name,
			),
		)
	}

	return value
}
