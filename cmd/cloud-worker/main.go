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

	"continuum/internal/cloudworker"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

const operationTimeout = 5 * time.Second

func main() {
	kafkaBroker := requiredEnv(
		"KAFKA_BROKER",
	)

	inputTopic := loadInputTopic()
	outputTopic := envOrDefault(
		"KAFKA_OUTPUT_TOPIC",
		"cloud-edge-aggregates",
	)

	groupID := envOrDefault(
		"KAFKA_GROUP_ID",
		"cloud-workers",
	)

	workerID := loadWorkerID()

	windowSize, err := loadCloudWindowSize()
	if err != nil {
		panic(err)
	}

	aggregator, err := cloudworker.NewWindowAggregator(
		windowSize,
	)
	if err != nil {
		panic(err)
	}

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				kafkaBroker,
			},
			Topic:       inputTopic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10 * 1024 * 1024,
			MaxWait:     500 * time.Millisecond,
		},
	)

	defer func() {
		if err := reader.Close(); err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka reader: %v\n",
				workerID,
				err,
			)
		}
	}()

	writer := newKafkaWriter(
		kafkaBroker,
		outputTopic,
	)

	defer func() {
		if err := writer.Close(); err != nil {
			fmt.Printf(
				"%s: errore chiusura Kafka writer: %v\n",
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
		"Input topic: %s\n",
		inputTopic,
	)
	fmt.Printf(
		"Output topic: %s\n",
		outputTopic,
	)
	fmt.Printf(
		"Cloud window: %s\n",
		windowSize,
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

	err = consume(
		ctx,
		reader,
		writer,
		aggregator,
		workerID,
	)
	if err != nil {
		panic(err)
	}

	if err := flushWindows(
		writer,
		aggregator,
		workerID,
	); err != nil {
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
	writer *kafka.Writer,
	aggregator *cloudworker.WindowAggregator,
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

		input, err := decodeEdgeAggregate(
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

		output, err := aggregator.Add(input)
		if err != nil {
			return fmt.Errorf(
				"elaborazione aggregate_id=%s fallita: %w",
				input.AggregateID,
				err,
			)
		}

		if output != nil {
			if err := publishCloudEdgeAggregate(
				writer,
				*output,
			); err != nil {
				return err
			}

			logPublishedWindow(
				workerID,
				writer.Topic,
				*output,
				false,
			)
		}

		// Lo stato delle finestre e volatile. Gli esperimenti fissano il
		// numero di Worker prima del replay e non lo cambiano durante il run.
		if err := commitMessage(
			reader,
			message,
		); err != nil {
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

	if err := json.Unmarshal(
		payload,
		&aggregate,
	); err != nil {
		return model.EdgeAggregate{},
			fmt.Errorf(
				"EdgeAggregate JSON non valido: %w",
				err,
			)
	}

	if err := cloudworker.ValidateEdgeAggregate(
		aggregate,
	); err != nil {
		return model.EdgeAggregate{}, err
	}

	return aggregate, nil
}

func publishCloudEdgeAggregate(
	writer *kafka.Writer,
	aggregate model.CloudEdgeAggregate,
) error {
	if err := cloudworker.ValidateCloudEdgeAggregate(
		aggregate,
	); err != nil {
		return fmt.Errorf(
			"CloudEdgeAggregate %q non valido: %w",
			aggregate.AggregateID,
			err,
		)
	}

	payload, err := json.Marshal(
		aggregate,
	)
	if err != nil {
		return fmt.Errorf(
			"serializzazione CloudEdgeAggregate %q fallita: %w",
			aggregate.AggregateID,
			err,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancel()

	if err := writer.WriteMessages(
		ctx,
		kafka.Message{
			Key: []byte(
				aggregate.EdgeID,
			),
			Value: payload,
			Time:  aggregate.EmittedAt,
		},
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka aggregate_id=%s topic=%s fallita: %w",
			aggregate.AggregateID,
			writer.Topic,
			err,
		)
	}

	return nil
}

func flushWindows(
	writer *kafka.Writer,
	aggregator *cloudworker.WindowAggregator,
	workerID string,
) error {
	for _, output := range aggregator.Flush() {
		if err := publishCloudEdgeAggregate(
			writer,
			output,
		); err != nil {
			return fmt.Errorf(
				"flush finestra edge=%s fallito: %w",
				output.EdgeID,
				err,
			)
		}

		logPublishedWindow(
			workerID,
			writer.Topic,
			output,
			true,
		)
	}

	return nil
}

func commitMessage(
	reader *kafka.Reader,
	message kafka.Message,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancel()

	return reader.CommitMessages(
		ctx,
		message,
	)
}

func logPublishedWindow(
	workerID string,
	topic string,
	aggregate model.CloudEdgeAggregate,
	partial bool,
) {
	fmt.Printf(
		"CLOUD_WINDOW_PUBLISHED worker=%s edge=%s aggregate_id=%s window=[%s,%s) inputs=%d events=%d partial=%t topic=%s\n",
		workerID,
		aggregate.EdgeID,
		aggregate.AggregateID,
		aggregate.WindowStart.Format(time.RFC3339),
		aggregate.WindowEnd.Format(time.RFC3339),
		aggregate.InputAggregates,
		aggregate.Events,
		partial,
		topic,
	)
}

func newKafkaWriter(
	broker string,
	topic string,
) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(
			broker,
		),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  5,
		BatchSize:    1,
		WriteTimeout: operationTimeout,
		ReadTimeout:  operationTimeout,
		Async:        false,
	}
}

func loadCloudWindowSize() (
	time.Duration,
	error,
) {
	value := envOrDefault(
		"CLOUD_WINDOW_SIZE",
		"15m",
	)

	windowSize, err := time.ParseDuration(
		value,
	)
	if err != nil {
		return 0,
			fmt.Errorf(
				"CLOUD_WINDOW_SIZE non valida %q: %w",
				value,
				err,
			)
	}

	if windowSize <= 0 {
		return 0,
			fmt.Errorf(
				"CLOUD_WINDOW_SIZE deve essere maggiore di zero",
			)
	}

	return windowSize, nil
}

func loadInputTopic() string {
	if value := strings.TrimSpace(
		os.Getenv("KAFKA_INPUT_TOPIC"),
	); value != "" {
		return value
	}

	if value := strings.TrimSpace(
		os.Getenv("KAFKA_TOPIC"),
	); value != "" {
		fmt.Println(
			"KAFKA_TOPIC e deprecata per il Cloud Worker; usare KAFKA_INPUT_TOPIC",
		)

		return value
	}

	return "edge-aggregates"
}

func loadWorkerID() string {
	if value := strings.TrimSpace(
		os.Getenv("WORKER_ID"),
	); value != "" {
		return value
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "cloud-worker"
	}

	return hostname
}

func envOrDefault(
	name string,
	defaultValue string,
) string {
	value := strings.TrimSpace(
		os.Getenv(name),
	)

	if value == "" {
		return defaultValue
	}

	return value
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
