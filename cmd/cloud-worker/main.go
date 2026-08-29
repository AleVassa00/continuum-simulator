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

type KafkaMessagePublisher func(
	context.Context,
	kafka.Message,
) error

type CloudMessageProcessor struct {
	aggregator     *cloudworker.WindowAggregator
	outputTopic    string
	workerID       string
	publishMessage KafkaMessagePublisher
	now            func() time.Time
	endedEdges     map[string]bool
}

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
	processor := &CloudMessageProcessor{
		aggregator:  aggregator,
		outputTopic: writer.Topic,
		workerID:    workerID,
		publishMessage: func(
			ctx context.Context,
			message kafka.Message,
		) error {
			return writer.WriteMessages(ctx, message)
		},
		now:        time.Now,
		endedEdges: make(map[string]bool),
	}

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

		// Lo stato delle finestre e volatile. Gli esperimenti fissano il
		// numero di Worker prima del replay e non lo cambiano durante il run.
		if err := processAndCommitMessage(
			message,
			processor,
			func(message kafka.Message) error {
				return commitMessage(reader, message)
			},
		); err != nil {
			return fmt.Errorf(
				"worker=%s partition=%d offset=%d: %w",
				workerID,
				message.Partition,
				message.Offset,
				err,
			)
		}
	}
}

func processAndCommitMessage(
	message kafka.Message,
	processor *CloudMessageProcessor,
	commit func(kafka.Message) error,
) error {
	if err := processor.Process(message); err != nil {
		return err
	}

	if err := commit(message); err != nil {
		return fmt.Errorf("commit Kafka fallito: %w", err)
	}

	return nil
}

func (
	processor *CloudMessageProcessor,
) Process(
	message kafka.Message,
) error {
	recordType, err := kafkaRecordType(message.Headers)
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

func (
	processor *CloudMessageProcessor,
) processEdgeAggregate(
	message kafka.Message,
) error {
	input, err := decodeEdgeAggregate(message.Value)
	if err != nil {
		return err
	}
	if string(message.Key) != input.EdgeID {
		return fmt.Errorf(
			"EdgeAggregate key Kafka=%q non coerente con edge_id=%q",
			message.Key,
			input.EdgeID,
		)
	}

	if processor.endedEdges == nil {
		processor.endedEdges = make(map[string]bool)
	}
	if processor.endedEdges[input.EdgeID] {
		return fmt.Errorf(
			"violazione invariant terminale: EdgeAggregate %s ricevuto dopo EndOfReplay edge=%s",
			input.AggregateID,
			input.EdgeID,
		)
	}

	output, err := processor.aggregator.Add(input)
	if err != nil {
		return fmt.Errorf(
			"elaborazione aggregate_id=%s fallita: %w",
			input.AggregateID,
			err,
		)
	}

	if output == nil {
		return nil
	}

	return processor.publishCloudAggregate(*output, false)
}

func (
	processor *CloudMessageProcessor,
) processEndOfReplay(
	message kafka.Message,
) error {
	record, err := decodeEndOfReplay(message.Value)
	if err != nil {
		return err
	}

	if string(message.Key) != record.EdgeID {
		return fmt.Errorf(
			"EndOfReplay key Kafka=%q non coerente con edge_id=%q",
			message.Key,
			record.EdgeID,
		)
	}

	if processor.endedEdges == nil {
		processor.endedEdges = make(map[string]bool)
	}
	if processor.endedEdges[record.EdgeID] {
		fmt.Printf(
			"%s: EndOfReplay duplicato edge=%s ignorato\n",
			processor.workerID,
			record.EdgeID,
		)
		return nil
	}

	if output, found := processor.aggregator.FlushEdge(record.EdgeID); found {
		if err := processor.publishCloudAggregate(*output, true); err != nil {
			return fmt.Errorf(
				"flush finale Cloud edge=%s fallito: %w",
				record.EdgeID,
				err,
			)
		}
	}

	forwarded := record
	forwarded.EmittedAt = processor.now().UTC()
	if err := processor.publishEndOfReplay(forwarded); err != nil {
		return err
	}

	processor.endedEdges[record.EdgeID] = true

	return nil
}

func (
	processor *CloudMessageProcessor,
) publishCloudAggregate(
	aggregate model.CloudEdgeAggregate,
	partial bool,
) error {
	message, err := cloudEdgeAggregateMessage(aggregate)
	if err != nil {
		return err
	}

	if err := writeKafkaMessage(
		processor.publishMessage,
		message,
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka aggregate_id=%s topic=%s fallita: %w",
			aggregate.AggregateID,
			processor.outputTopic,
			err,
		)
	}

	logPublishedWindow(
		processor.workerID,
		processor.outputTopic,
		aggregate,
		partial,
	)

	return nil
}

func (
	processor *CloudMessageProcessor,
) publishEndOfReplay(
	record model.EndOfReplay,
) error {
	message, err := endOfReplayMessage(record)
	if err != nil {
		return err
	}

	if err := writeKafkaMessage(
		processor.publishMessage,
		message,
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka EndOfReplay edge=%s topic=%s fallita: %w",
			record.EdgeID,
			processor.outputTopic,
			err,
		)
	}

	return nil
}

func kafkaRecordType(
	headers []kafka.Header,
) (string, error) {
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

func decodeEndOfReplay(
	payload []byte,
) (model.EndOfReplay, error) {
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
	message, err := cloudEdgeAggregateMessage(aggregate)
	if err != nil {
		return err
	}

	if err := writeKafkaMessage(
		func(
			ctx context.Context,
			message kafka.Message,
		) error {
			return writer.WriteMessages(ctx, message)
		},
		message,
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

func cloudEdgeAggregateMessage(
	aggregate model.CloudEdgeAggregate,
) (kafka.Message, error) {
	if err := cloudworker.ValidateCloudEdgeAggregate(
		aggregate,
	); err != nil {
		return kafka.Message{}, fmt.Errorf(
			"CloudEdgeAggregate %q non valido: %w",
			aggregate.AggregateID,
			err,
		)
	}

	payload, err := json.Marshal(aggregate)
	if err != nil {
		return kafka.Message{}, fmt.Errorf(
			"serializzazione CloudEdgeAggregate %q fallita: %w",
			aggregate.AggregateID,
			err,
		)
	}

	return kafka.Message{
		Key:   []byte(aggregate.EdgeID),
		Value: payload,
		Time:  aggregate.EmittedAt,
		Headers: []kafka.Header{
			{
				Key:   model.RecordTypeHeader,
				Value: []byte(model.RecordTypeCloudEdgeAggregate),
			},
		},
	}, nil
}

func endOfReplayMessage(
	record model.EndOfReplay,
) (kafka.Message, error) {
	if err := model.ValidateEndOfReplay(record); err != nil {
		return kafka.Message{}, err
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return kafka.Message{}, fmt.Errorf(
			"serializzazione EndOfReplay edge=%s fallita: %w",
			record.EdgeID,
			err,
		)
	}

	return kafka.Message{
		Key:   []byte(record.EdgeID),
		Value: payload,
		Time:  record.EmittedAt,
		Headers: []kafka.Header{
			{
				Key:   model.RecordTypeHeader,
				Value: []byte(model.RecordTypeEndOfReplay),
			},
		},
	}, nil
}

func writeKafkaMessage(
	publish KafkaMessagePublisher,
	message kafka.Message,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancel()

	return publish(ctx, message)
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
