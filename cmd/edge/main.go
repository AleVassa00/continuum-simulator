package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"continuum/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

type MetricValue struct {
	Value  float64
	Valid  bool
	Reason string
}

type EdgeMeasurement struct {
	Temperature MetricValue
	Humidity    MetricValue
	Pressure    MetricValue
}

type MetricState struct {
	Valid   uint64
	Invalid uint64

	Sum float64
	Min float64
	Max float64
}

type WindowState struct {
	Start time.Time
	End   time.Time

	Events          uint64
	DuplicateEvents uint64

	SeenEventIDs map[string]struct{}

	Temperature MetricState
	Humidity    MetricState
	Pressure    MetricState
}

type WindowAggregator struct {
	mu sync.Mutex

	edgeID     string
	windowSize time.Duration
	current    *WindowState

	kafkaWriter *kafka.Writer
}

const telemetrySubscriptionTopic = "sensors/+/telemetry"

func main() {
	edgeID := strings.TrimSpace(
		os.Getenv("EDGE_ID"),
	)

	if edgeID == "" {
		panic("variabile EDGE_ID non impostata")
	}

	broker := strings.TrimSpace(
		os.Getenv("MQTT_BROKER"),
	)

	if broker == "" {
		panic("variabile MQTT_BROKER non impostata")
	}

	kafkaBroker := strings.TrimSpace(
		os.Getenv("KAFKA_BROKER"),
	)

	if kafkaBroker == "" {
		panic("variabile KAFKA_BROKER non impostata")
	}

	kafkaTopic := strings.TrimSpace(
		os.Getenv("KAFKA_TOPIC"),
	)

	if kafkaTopic == "" {
		panic("variabile KAFKA_TOPIC non impostata")
	}

	windowSize, err := loadWindowSize()
	if err != nil {
		panic(err)
	}

	kafkaWriter := newKafkaWriter(
		kafkaBroker,
		kafkaTopic,
	)

	aggregator := &WindowAggregator{
		edgeID:      edgeID,
		windowSize:  windowSize,
		kafkaWriter: kafkaWriter,
	}

	fmt.Printf(
		"Avvio Edge %s\n",
		edgeID,
	)

	fmt.Printf(
		"Broker MQTT: %s\n",
		broker,
	)

	fmt.Printf(
		"Window size: %s\n",
		windowSize,
	)

	fmt.Printf(
		"Kafka broker: %s\n",
		kafkaBroker,
	)

	fmt.Printf(
		"Kafka topic: %s\n\n",
		kafkaTopic,
	)

	options := mqtt.NewClientOptions()

	options.AddBroker(
		broker,
	)

	options.SetClientID(
		"edge-consumer-" + edgeID,
	)

	options.SetAutoReconnect(
		true,
	)

	options.SetConnectTimeout(
		5 * time.Second,
	)

	options.SetOnConnectHandler(
		func(client mqtt.Client) {
			fmt.Printf(
				"%s connesso al broker MQTT\n",
				edgeID,
			)

			subscribeToTelemetry(
				client,
				edgeID,
				aggregator,
			)
		},
	)

	options.SetConnectionLostHandler(
		func(
			client mqtt.Client,
			err error,
		) {
			fmt.Printf(
				"%s ha perso la connessione MQTT: %v\n",
				edgeID,
				err,
			)
		},
	)

	client := mqtt.NewClient(
		options,
	)

	token := client.Connect()

	if !token.WaitTimeout(
		5 * time.Second,
	) {
		panic(
			fmt.Sprintf(
				"timeout connessione MQTT per %s",
				edgeID,
			),
		)
	}

	if token.Error() != nil {
		panic(
			fmt.Errorf(
				"connessione MQTT fallita per %s: %w",
				edgeID,
				token.Error(),
			),
		)
	}

	waitForShutdown()

	fmt.Printf(
		"\nArresto %s...\n",
		edgeID,
	)

	client.Disconnect(250)

	aggregator.Flush()

	err = kafkaWriter.Close()
	if err != nil {
		fmt.Printf(
			"%s: errore chiusura Kafka writer: %v\n",
			edgeID,
			err,
		)
	}
}

func newKafkaWriter(
	broker string,
	topic string,
) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(
			broker,
		),

		Topic: topic,

		Balancer: &kafka.Hash{},

		RequiredAcks: kafka.RequireAll,

		MaxAttempts: 5,

		BatchSize: 1,

		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,

		Async: false,
	}
}

func newWindowState(
	start time.Time,
	end time.Time,
) *WindowState {
	return &WindowState{
		Start: start,
		End:   end,

		SeenEventIDs: make(
			map[string]struct{},
		),
	}
}

func loadWindowSize() (
	time.Duration,
	error,
) {
	value := strings.TrimSpace(
		os.Getenv("WINDOW_SIZE"),
	)

	if value == "" {
		return 5 * time.Minute, nil
	}

	windowSize, err := time.ParseDuration(
		value,
	)
	if err != nil {
		return 0,
			fmt.Errorf(
				"WINDOW_SIZE non valida %q: %w",
				value,
				err,
			)
	}

	if windowSize <= 0 {
		return 0,
			fmt.Errorf(
				"WINDOW_SIZE deve essere maggiore di zero",
			)
	}

	return windowSize, nil
}

func subscribeToTelemetry(
	client mqtt.Client,
	edgeID string,
	aggregator *WindowAggregator,
) {
	topic := telemetrySubscriptionTopic

	token := client.Subscribe(
		topic,
		1,
		makeTelemetryHandler(
			edgeID,
			aggregator,
		),
	)

	if !token.WaitTimeout(
		5 * time.Second,
	) {
		fmt.Printf(
			"%s: timeout sottoscrizione MQTT\n",
			edgeID,
		)

		return
	}

	if token.Error() != nil {
		fmt.Printf(
			"%s: errore sottoscrizione MQTT: %v\n",
			edgeID,
			token.Error(),
		)

		return
	}

	fmt.Printf(
		"%s sottoscritto a %s\n\n",
		edgeID,
		topic,
	)
}

func makeTelemetryHandler(
	edgeID string,
	aggregator *WindowAggregator,
) mqtt.MessageHandler {
	return func(
		client mqtt.Client,
		message mqtt.Message,
	) {
		var event model.SensorEvent

		err := json.Unmarshal(
			message.Payload(),
			&event,
		)
		if err != nil {
			fmt.Printf(
				"%s: JSON non valido: %v\n",
				edgeID,
				err,
			)

			return
		}

		err = validateSensorEvent(
			event,
		)
		if err != nil {
			fmt.Printf(
				"%s: evento scartato: %v\n",
				edgeID,
				err,
			)

			return
		}

		measurement := parseMeasurements(
			event,
		)

		err = aggregator.Add(
			event.EventID,
			event.ObservedAt,
			measurement,
		)
		if err != nil {
			fmt.Printf(
				"%s: evento %s scartato: %v\n",
				edgeID,
				event.EventID,
				err,
			)

			return
		}
	}
}

func validateSensorEvent(
	event model.SensorEvent,
) error {
	if event.SchemaVersion != 1 {
		return fmt.Errorf(
			"schema_version non supportata: %d",
			event.SchemaVersion,
		)
	}

	if strings.TrimSpace(
		event.EventID,
	) == "" {
		return fmt.Errorf(
			"event_id mancante",
		)
	}

	if strings.TrimSpace(
		event.SensorID,
	) == "" {
		return fmt.Errorf(
			"sensor_id mancante",
		)
	}

	if event.ObservedAt.IsZero() {
		return fmt.Errorf(
			"observed_at mancante",
		)
	}

	return nil
}

func parseMeasurements(
	event model.SensorEvent,
) EdgeMeasurement {
	return EdgeMeasurement{
		Temperature: parseMetric(
			event.Measurements,
			"temperature",
			-40,
			85,
		),

		Humidity: parseMetric(
			event.Measurements,
			"humidity",
			0,
			100,
		),

		Pressure: parseMetric(
			event.Measurements,
			"pressure",
			30000,
			110000,
		),
	}
}

func parseMetric(
	measurements map[string]string,
	name string,
	minValue float64,
	maxValue float64,
) MetricValue {
	rawValue, found := measurements[name]

	if !found {
		return MetricValue{
			Valid:  false,
			Reason: "misura mancante",
		}
	}

	rawValue = strings.TrimSpace(
		rawValue,
	)

	if rawValue == "" {
		return MetricValue{
			Valid:  false,
			Reason: "misura vuota",
		}
	}

	value, err := strconv.ParseFloat(
		rawValue,
		64,
	)

	if err != nil {
		return MetricValue{
			Valid: false,

			Reason: fmt.Sprintf(
				"valore %q non numerico",
				rawValue,
			),
		}
	}

	if math.IsNaN(value) ||
		math.IsInf(value, 0) {
		return MetricValue{
			Valid: false,

			Reason: fmt.Sprintf(
				"valore numerico non finito %q",
				rawValue,
			),
		}
	}

	if value < minValue ||
		value > maxValue {
		return MetricValue{
			Value: value,
			Valid: false,

			Reason: fmt.Sprintf(
				"valore %.2f fuori dal range [%.2f, %.2f]",
				value,
				minValue,
				maxValue,
			),
		}
	}

	return MetricValue{
		Value: value,
		Valid: true,
	}
}

func (
	aggregator *WindowAggregator,
) Add(
	eventID string,
	observedAt time.Time,
	measurement EdgeMeasurement,
) error {
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	windowStart := observedAt.Truncate(
		aggregator.windowSize,
	)

	windowEnd := windowStart.Add(
		aggregator.windowSize,
	)

	if aggregator.current == nil {
		aggregator.current = newWindowState(
			windowStart,
			windowEnd,
		)
	}

	if windowStart.Before(
		aggregator.current.Start,
	) {
		return fmt.Errorf(
			"evento fuori ordine: event_id=%s observed_at=%s current_window=%s",
			eventID,
			observedAt.Format(time.RFC3339),
			aggregator.current.Start.Format(time.RFC3339),
		)
	}

	if !windowStart.Equal(
		aggregator.current.Start,
	) {
		err := aggregator.emitCurrentWindow()
		if err != nil {
			return err
		}

		aggregator.current = newWindowState(
			windowStart,
			windowEnd,
		)
	}

	if _, found :=
		aggregator.current.SeenEventIDs[eventID]; found {

		aggregator.current.DuplicateEvents++

		return nil
	}

	aggregator.current.SeenEventIDs[eventID] =
		struct{}{}

	aggregator.current.Add(
		measurement,
	)

	return nil
}

func (
	window *WindowState,
) Add(
	measurement EdgeMeasurement,
) {
	window.Events++

	window.Temperature.Add(
		measurement.Temperature,
	)

	window.Humidity.Add(
		measurement.Humidity,
	)

	window.Pressure.Add(
		measurement.Pressure,
	)
}

func (
	metric *MetricState,
) Add(
	value MetricValue,
) {
	if !value.Valid {
		metric.Invalid++

		return
	}

	if metric.Valid == 0 {
		metric.Min = value.Value
		metric.Max = value.Value
	} else {
		metric.Min = min(
			metric.Min,
			value.Value,
		)

		metric.Max = max(
			metric.Max,
			value.Value,
		)
	}

	metric.Sum += value.Value
	metric.Valid++
}

func (
	aggregator *WindowAggregator,
) emitCurrentWindow() error {
	if aggregator.current == nil {
		return nil
	}

	if aggregator.current.Events == 0 {
		return nil
	}

	aggregate := buildEdgeAggregate(
		aggregator.edgeID,
		aggregator.current,
	)

	payload, err := json.Marshal(
		aggregate,
	)
	if err != nil {
		return fmt.Errorf(
			"serializzazione EdgeAggregate fallita: %w",
			err,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	err = aggregator.kafkaWriter.WriteMessages(
		ctx,
		kafka.Message{
			Key: []byte(
				aggregator.edgeID,
			),

			Value: payload,

			Time: aggregate.EmittedAt,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka aggregate_id=%s fallita: %w",
			aggregate.AggregateID,
			err,
		)
	}

	fmt.Printf(
		"KAFKA_PUBLISHED edge=%s aggregate_id=%s events=%d duplicates=%d topic=%s\n",
		aggregate.EdgeID,
		aggregate.AggregateID,
		aggregate.Events,
		aggregate.DuplicateEvents,
		aggregator.kafkaWriter.Topic,
	)

	return nil
}

func (
	aggregator *WindowAggregator,
) Flush() {
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	err := aggregator.emitCurrentWindow()
	if err != nil {
		fmt.Printf(
			"%s: errore flush ultima finestra: %v\n",
			aggregator.edgeID,
			err,
		)

		return
	}

	aggregator.current = nil
}

func buildMetricAggregate(
	state MetricState,
) model.MetricAggregate {
	if state.Valid == 0 {
		return model.MetricAggregate{
			Valid:   0,
			Invalid: state.Invalid,
			Sum:     0,
			Average: nil,
			Min:     nil,
			Max:     nil,
		}
	}

	average := state.Sum /
		float64(state.Valid)

	minimum := state.Min
	maximum := state.Max

	return model.MetricAggregate{
		Valid:   state.Valid,
		Invalid: state.Invalid,
		Sum:     state.Sum,
		Average: &average,
		Min:     &minimum,
		Max:     &maximum,
	}
}

func buildAggregateID(
	edgeID string,
	windowStart time.Time,
	windowEnd time.Time,
) string {
	return fmt.Sprintf(
		"%s:%s:%s",
		edgeID,
		windowStart.UTC().Format(
			time.RFC3339,
		),
		windowEnd.UTC().Format(
			time.RFC3339,
		),
	)
}

func buildEdgeAggregate(
	edgeID string,
	window *WindowState,
) model.EdgeAggregate {
	return model.EdgeAggregate{
		SchemaVersion: model.EdgeAggregateSchemaVersion,

		AggregateID: buildAggregateID(
			edgeID,
			window.Start,
			window.End,
		),

		EdgeID: edgeID,

		WindowStart: window.Start,
		WindowEnd:   window.End,

		Events:          window.Events,
		DuplicateEvents: window.DuplicateEvents,

		Temperature: buildMetricAggregate(
			window.Temperature,
		),

		Humidity: buildMetricAggregate(
			window.Humidity,
		),

		Pressure: buildMetricAggregate(
			window.Pressure,
		),

		EmittedAt: time.Now().UTC(),
	}
}

func waitForShutdown() {
	signals := make(
		chan os.Signal,
		1,
	)

	signal.Notify(
		signals,
		os.Interrupt,
		syscall.SIGTERM,
	)

	<-signals
}
