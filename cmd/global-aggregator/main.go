package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/envutil"
	"continuum/internal/globalaggregator"
	"continuum/internal/kafkautil"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

const operationTimeout = 5 * time.Second

type KafkaMessageCommitter func(kafka.Message) error

type GlobalMessageProcessor struct {
	aggregator *globalaggregator.Aggregator
}

func main() {
	kafkaBroker := envutil.Required("KAFKA_BROKER")
	inputTopic := envutil.OrDefault(
		os.Getenv,
		"KAFKA_INPUT_TOPIC",
		"cloud-edge-aggregates",
	)
	groupID := envutil.OrDefault(
		os.Getenv,
		"KAFKA_GROUP_ID",
		"global-aggregator",
	)
	windowSize, err := loadGlobalWindowSize(os.Getenv)
	if err != nil {
		panic(err)
	}
	watermarkDelay, err := loadGlobalWatermarkDelay(os.Getenv, windowSize)
	if err != nil {
		panic(err)
	}
	expectedEdgeIDs, err := loadExpectedEdgeIDs(os.Getenv)
	if err != nil {
		panic(err)
	}

	aggregator, err := globalaggregator.New(
		expectedEdgeIDs,
		windowSize,
		watermarkDelay,
		newJSONLogSink(os.Stdout),
	)
	if err != nil {
		panic(err)
	}
	processor := &GlobalMessageProcessor{aggregator: aggregator}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       inputTopic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 * 1024 * 1024,
		MaxWait:     500 * time.Millisecond,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			fmt.Printf("Global Aggregator: errore chiusura Kafka reader: %v\n", err)
		}
	}()

	fmt.Println("Avvio Global Aggregator")
	fmt.Printf("Kafka broker: %s\n", kafkaBroker)
	fmt.Printf("Input topic: %s\n", inputTopic)
	fmt.Printf("Consumer group: %s\n", groupID)
	fmt.Printf("Global window: %s\n", windowSize)
	fmt.Printf("Watermark delay: %s\n", watermarkDelay)
	fmt.Printf("Expected Edge: %s\n\n", strings.Join(expectedEdgeIDs, ","))

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	completed, err := consume(ctx, reader, processor)
	if err != nil {
		panic(err)
	}
	if completed {
		fmt.Println("GLOBAL_REPLAY_COMPLETED")
		return
	}
	fmt.Println("Global Aggregator arrestato prima dell'EndOfReplay globale")
}

func consume(
	ctx context.Context,
	reader *kafka.Reader,
	processor *GlobalMessageProcessor,
) (bool, error) {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return false, nil
			}
			return false, fmt.Errorf("lettura Kafka globale fallita: %w", err)
		}

		completed, err := processAndCommitMessage(
			ctx,
			message,
			processor,
			func(message kafka.Message) error {
				return kafkautil.CommitMessage(reader, message, operationTimeout)
			},
		)
		if err != nil {
			return false, fmt.Errorf(
				"global partition=%d offset=%d: %w",
				message.Partition,
				message.Offset,
				err,
			)
		}
		if completed {
			return true, nil
		}
	}
}

func processAndCommitMessage(
	ctx context.Context,
	message kafka.Message,
	processor *GlobalMessageProcessor,
	commit KafkaMessageCommitter,
) (bool, error) {
	completed, err := processor.Process(ctx, message)
	if err != nil {
		return false, err
	}
	if err := commit(message); err != nil {
		return false, fmt.Errorf("commit Kafka globale fallito: %w", err)
	}
	return completed, nil
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
		record, err := kafkautil.DecodeEndOfReplay(message.Value)
		if err != nil {
			return false, err
		}
		if string(message.Key) != record.EdgeID {
			return false, fmt.Errorf(
				"EndOfReplay key Kafka=%q non coerente con edge_id=%q",
				message.Key,
				record.EdgeID,
			)
		}
		return processor.aggregator.EndReplay(ctx, record)

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

func newJSONLogSink(writer io.Writer) globalaggregator.GlobalAggregateSink {
	return func(
		_ context.Context,
		aggregate model.GlobalAggregate,
	) error {
		if err := globalaggregator.ValidateGlobalAggregate(aggregate); err != nil {
			return err
		}
		payload, err := json.Marshal(aggregate)
		if err != nil {
			return fmt.Errorf(
				"serializzazione GlobalAggregate %q fallita: %w",
				aggregate.AggregateID,
				err,
			)
		}
		if _, err := fmt.Fprintf(writer, "GLOBAL_AGGREGATE %s\n", payload); err != nil {
			return fmt.Errorf("scrittura log GlobalAggregate fallita: %w", err)
		}
		return nil
	}
}

func loadGlobalWindowSize(
	getenv func(string) string,
) (time.Duration, error) {
	value := envutil.OrDefault(getenv, "GLOBAL_WINDOW_SIZE", "15m")
	windowSize, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"GLOBAL_WINDOW_SIZE non valida %q: %w",
			value,
			err,
		)
	}
	if windowSize <= 0 {
		return 0, fmt.Errorf(
			"GLOBAL_WINDOW_SIZE deve essere maggiore di zero",
		)
	}
	return windowSize, nil
}

func loadGlobalWatermarkDelay(
	getenv func(string) string,
	defaultDelay time.Duration,
) (time.Duration, error) {
	value := strings.TrimSpace(getenv("GLOBAL_WATERMARK_DELAY"))
	if value == "" {
		return defaultDelay, nil
	}
	delay, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("GLOBAL_WATERMARK_DELAY non valida %q: %w", value, err)
	}
	if delay <= 0 {
		return 0, fmt.Errorf("GLOBAL_WATERMARK_DELAY deve essere maggiore di zero")
	}
	return delay, nil
}

func loadExpectedEdgeIDs(
	getenv func(string) string,
) ([]string, error) {
	value := strings.TrimSpace(getenv("EXPECTED_EDGE_IDS"))
	if value == "" {
		return nil, fmt.Errorf("variabile EXPECTED_EDGE_IDS non impostata")
	}
	parts := strings.Split(value, ",")
	edgeIDs := make([]string, len(parts))
	for index, part := range parts {
		edgeIDs[index] = strings.TrimSpace(part)
	}
	return edgeIDs, nil
}
