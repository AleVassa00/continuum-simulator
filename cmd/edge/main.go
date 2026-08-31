package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	Events uint64

	Temperature MetricState
	Humidity    MetricState
	Pressure    MetricState
}

type WindowAggregator struct {
	mu sync.Mutex

	edgeID     string
	windowSize time.Duration
	current    *WindowState
	ended      bool

	kafkaTopic     string
	publishMessage func(context.Context, kafka.Message) error
	stats          *EdgeStats
}

type ReadinessState struct {
	ready atomic.Bool
}

type SubscriptionCoordinator struct {
	generation atomic.Uint64
}

type SubscriptionRetryPolicy struct {
	Attempts int
	Timeout  time.Duration
	Backoff  time.Duration
}

const (
	telemetrySubscriptionTopic      = "sensors/+/telemetry"
	readinessAddress                = ":8080"
	mqttSubscriptionAttempts        = 3
	mqttSubscriptionTimeout         = 5 * time.Second
	mqttSubscriptionBackoff         = 250 * time.Millisecond
	defaultEdgeIngressQueueCapacity = 1000
)

var (
	errSubscriptionInactive = errors.New("tentativo di sottoscrizione MQTT non piu attivo")
	errEdgeWindowClosed     = errors.New("evento appartenente a finestra Edge gia chiusa")
	errEdgeReplayEnded      = errors.New("replay Edge gia terminato")
)

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

	ingressCapacity, err := loadEdgeIngressQueueCapacity(os.Getenv)
	if err != nil {
		panic(err)
	}

	ingress, err := newEdgeIngressQueue(ingressCapacity)
	if err != nil {
		panic(err)
	}
	stats := &EdgeStats{}

	kafkaWriter := newKafkaWriter(
		kafkaBroker,
		kafkaTopic,
	)

	aggregator := &WindowAggregator{
		edgeID:     edgeID,
		windowSize: windowSize,
		kafkaTopic: kafkaTopic,
		publishMessage: func(
			ctx context.Context,
			message kafka.Message,
		) error {
			return kafkaWriter.WriteMessages(
				ctx,
				message,
			)
		},
		stats: stats,
	}

	processor := &EdgeProcessor{
		edgeID:     edgeID,
		ingress:    ingress,
		aggregator: aggregator,
		stats:      stats,
		now:        time.Now,
	}
	processorDone := make(chan error, 1)
	go func() {
		processorDone <- processor.Run()
	}()

	readiness := &ReadinessState{}
	readinessServer, err := startReadinessServer(
		readiness,
		edgeID,
	)
	if err != nil {
		panic(err)
	}

	defer stopReadinessServer(
		readinessServer,
		edgeID,
	)

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
	fmt.Printf(
		"Edge ingress queue capacity: %d\n\n",
		ingressCapacity,
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

	options.SetOrderMatters(true)

	subscriptions := &SubscriptionCoordinator{}

	options.SetOnConnectHandler(
		func(client mqtt.Client) {
			readiness.MarkNotReady()
			generation := subscriptions.Begin()

			fmt.Printf(
				"%s connesso al broker MQTT\n",
				edgeID,
			)

			subscribeToEdgeTopics(
				client,
				edgeID,
				ingress,
				stats,
				readiness,
				subscriptions,
				generation,
			)
		},
	)

	options.SetConnectionLostHandler(
		func(
			client mqtt.Client,
			err error,
		) {
			subscriptions.Invalidate()
			readiness.MarkNotReady()

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

	processorErr := waitForShutdown(processorDone)

	fmt.Printf(
		"\nArresto %s...\n",
		edgeID,
	)

	subscriptions.Invalidate()
	readiness.MarkNotReady()
	client.Disconnect(250)
	ingress.Close()
	if processorErr == nil {
		processorErr = <-processorDone
	}

	if processorErr == nil {
		processorErr = aggregator.Flush()
	}
	if processorErr != nil {
		fmt.Printf(
			"%s: processing fallito: %v\n",
			edgeID,
			processorErr,
		)
	}

	err = kafkaWriter.Close()
	if err != nil {
		fmt.Printf(
			"%s: errore chiusura Kafka writer: %v\n",
			edgeID,
			err,
		)
	}

	printEdgeSummary(edgeID, stats.SnapshotWithQueue(ingress))
	if processorErr != nil {
		panic(processorErr)
	}
}

func (
	state *ReadinessState,
) MarkReady() {
	state.ready.Store(true)
}

func (
	state *ReadinessState,
) MarkNotReady() {
	state.ready.Store(false)
}

func (
	state *ReadinessState,
) IsReady() bool {
	return state.ready.Load()
}

func (
	coordinator *SubscriptionCoordinator,
) Begin() uint64 {
	return coordinator.generation.Add(1)
}

func (
	coordinator *SubscriptionCoordinator,
) Invalidate() {
	coordinator.generation.Add(1)
}

func (
	coordinator *SubscriptionCoordinator,
) IsCurrent(
	generation uint64,
) bool {
	return coordinator.generation.Load() == generation
}

func (
	state *ReadinessState,
) ServeHTTP(
	response http.ResponseWriter,
	request *http.Request,
) {
	response.Header().Set(
		"Content-Type",
		"text/plain; charset=utf-8",
	)

	if !state.IsReady() {
		response.WriteHeader(http.StatusServiceUnavailable)
		_, _ = response.Write([]byte("not ready\n"))

		return
	}

	response.WriteHeader(http.StatusOK)
	_, _ = response.Write([]byte("ready\n"))
}

func startReadinessServer(
	readiness *ReadinessState,
	edgeID string,
) (*http.Server, error) {
	listener, err := net.Listen(
		"tcp",
		readinessAddress,
	)
	if err != nil {
		return nil,
			fmt.Errorf(
				"%s: avvio readiness server su %s fallito: %w",
				edgeID,
				readinessAddress,
				err,
			)
	}

	mux := http.NewServeMux()
	mux.Handle(
		"GET /readyz",
		readiness,
	)

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      2 * time.Second,
		IdleTimeout:       5 * time.Second,
	}

	go func() {
		err := server.Serve(listener)
		if err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			fmt.Printf(
				"%s: readiness server terminato: %v\n",
				edgeID,
				err,
			)
		}
	}()

	return server, nil
}

func stopReadinessServer(
	server *http.Server,
	edgeID string,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf(
			"%s: arresto readiness server fallito: %v\n",
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

		MaxAttempts: 1,

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

func loadEdgeIngressQueueCapacity(
	getenv func(string) string,
) (int, error) {
	value := strings.TrimSpace(
		getenv("EDGE_INGRESS_QUEUE_CAPACITY"),
	)
	if value == "" {
		return defaultEdgeIngressQueueCapacity, nil
	}

	capacity, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf(
			"EDGE_INGRESS_QUEUE_CAPACITY non valida %q: %w",
			value,
			err,
		)
	}
	if capacity <= 0 {
		return 0, fmt.Errorf(
			"EDGE_INGRESS_QUEUE_CAPACITY deve essere maggiore di zero",
		)
	}

	return capacity, nil
}

func subscribeToEdgeTopics(
	client mqtt.Client,
	edgeID string,
	ingress *EdgeIngressQueue,
	stats *EdgeStats,
	readiness *ReadinessState,
	coordinator *SubscriptionCoordinator,
	generation uint64,
) {
	readiness.MarkNotReady()

	topics := edgeSubscriptionTopics(edgeID)
	handler := makeEdgeMessageHandler(
		edgeID,
		ingress,
		stats,
	)

	attempts, err := retrySubscription(
		SubscriptionRetryPolicy{
			Attempts: mqttSubscriptionAttempts,
			Timeout:  mqttSubscriptionTimeout,
			Backoff:  mqttSubscriptionBackoff,
		},
		func() bool {
			return coordinator.IsCurrent(generation) &&
				client.IsConnected()
		},
		func(timeout time.Duration) error {
			token := client.SubscribeMultiple(
				topics,
				handler,
			)

			if !token.WaitTimeout(timeout) {
				return fmt.Errorf("timeout sottoscrizione MQTT")
			}

			if token.Error() != nil {
				return fmt.Errorf(
					"errore sottoscrizione MQTT: %w",
					token.Error(),
				)
			}

			return nil
		},
		time.Sleep,
	)
	if err != nil {
		fmt.Printf(
			"%s: sottoscrizione MQTT non attiva dopo %d tentativi: %v\n",
			edgeID,
			attempts,
			err,
		)

		return
	}

	readiness.MarkReady()

	fmt.Printf(
		"%s sottoscritto a %s e %s\n\n",
		edgeID,
		telemetrySubscriptionTopic,
		replayEndTopic(edgeID),
	)
}

func edgeSubscriptionTopics(
	edgeID string,
) map[string]byte {
	return map[string]byte{
		telemetrySubscriptionTopic: 0,
		replayEndTopic(edgeID):     1,
	}
}

func replayEndTopic(
	edgeID string,
) string {
	return fmt.Sprintf(
		"replay/%s/end",
		edgeID,
	)
}

func retrySubscription(
	policy SubscriptionRetryPolicy,
	isActive func() bool,
	attempt func(time.Duration) error,
	wait func(time.Duration),
) (int, error) {
	if policy.Attempts <= 0 {
		return 0, fmt.Errorf("numero tentativi di sottoscrizione non valido")
	}

	var lastErr error

	for attemptNumber := 1; attemptNumber <= policy.Attempts; attemptNumber++ {
		if !isActive() {
			return attemptNumber - 1, errSubscriptionInactive
		}

		lastErr = attempt(policy.Timeout)
		if lastErr == nil {
			if !isActive() {
				return attemptNumber, errSubscriptionInactive
			}

			return attemptNumber, nil
		}

		if attemptNumber == policy.Attempts {
			break
		}

		if !isActive() {
			return attemptNumber, errSubscriptionInactive
		}

		wait(
			policy.Backoff * time.Duration(attemptNumber),
		)
	}

	return policy.Attempts,
		fmt.Errorf(
			"tentativi di sottoscrizione esauriti: %w",
			lastErr,
		)
}

func makeEdgeMessageHandler(
	edgeID string,
	ingress *EdgeIngressQueue,
	stats *EdgeStats,
) mqtt.MessageHandler {
	endTopic := replayEndTopic(edgeID)

	return func(
		_ mqtt.Client,
		message mqtt.Message,
	) {
		if message.Topic() == endTopic {
			ingress.RegisterEndOfReplay(message.Payload())
			return
		}

		stats.telemetryReceived.Add(1)
		switch ingress.TryEnqueueTelemetry(message.Payload()) {
		case TelemetryEnqueued:
			stats.ingressAccepted.Add(1)
		case TelemetryDroppedAfterEOS:
			stats.postEOSDropped.Add(1)
		case TelemetryDroppedQueueFull,
			TelemetryDroppedQueueClosed:
			stats.ingressQueueDropped.Add(1)
		}
	}
}

func handleEndOfReplayPayload(
	edgeID string,
	payload []byte,
	aggregator *WindowAggregator,
	emittedAt time.Time,
) error {
	var record model.EndOfReplay
	if err := json.Unmarshal(payload, &record); err != nil {
		return fmt.Errorf("EndOfReplay JSON non valido: %w", err)
	}

	if err := model.ValidateEndOfReplay(record); err != nil {
		return err
	}

	if record.EdgeID != edgeID {
		return fmt.Errorf(
			"EndOfReplay edge_id=%s ricevuto da %s",
			record.EdgeID,
			edgeID,
		)
	}

	return aggregator.EndReplay(record, emittedAt)
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

	if aggregator.ended {
		return fmt.Errorf(
			"%w: edge=%s event_id=%s",
			errEdgeReplayEnded,
			aggregator.edgeID,
			eventID,
		)
	}

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
			"%w: event_id=%s observed_at=%s current_window=%s",
			errEdgeWindowClosed,
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

	err = aggregator.publishMessage(
		ctx,
		kafka.Message{
			Key: []byte(
				aggregator.edgeID,
			),

			Value: payload,

			Headers: []kafka.Header{
				{
					Key:   model.RecordTypeHeader,
					Value: []byte(model.RecordTypeEdgeAggregate),
				},
			},

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
		"KAFKA_PUBLISHED edge=%s aggregate_id=%s events=%d topic=%s\n",
		aggregate.EdgeID,
		aggregate.AggregateID,
		aggregate.Events,
		aggregator.kafkaTopic,
	)
	if aggregator.stats != nil {
		aggregator.stats.aggregatesEmitted.Add(1)
	}

	return nil
}

func (
	aggregator *WindowAggregator,
) Flush() error {
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	if err := aggregator.emitCurrentWindow(); err != nil {
		return err
	}

	aggregator.current = nil

	return nil
}

func (
	aggregator *WindowAggregator,
) EndReplay(
	record model.EndOfReplay,
	emittedAt time.Time,
) error {
	if err := model.ValidateEndOfReplay(record); err != nil {
		return err
	}

	if record.EdgeID != aggregator.edgeID {
		return fmt.Errorf(
			"EndOfReplay edge_id=%s non coerente con Edge %s",
			record.EdgeID,
			aggregator.edgeID,
		)
	}

	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	if aggregator.ended {
		fmt.Printf(
			"%s: EndOfReplay duplicato ignorato\n",
			aggregator.edgeID,
		)
		return nil
	}

	if err := aggregator.emitCurrentWindow(); err != nil {
		return fmt.Errorf(
			"flush finestra finale Edge %s fallito: %w",
			aggregator.edgeID,
			err,
		)
	}
	aggregator.current = nil

	forwarded := record
	forwarded.EmittedAt = emittedAt.UTC()
	payload, err := json.Marshal(forwarded)
	if err != nil {
		return fmt.Errorf(
			"serializzazione EndOfReplay Edge %s fallita: %w",
			aggregator.edgeID,
			err,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := aggregator.publishMessage(
		ctx,
		kafka.Message{
			Key:   []byte(aggregator.edgeID),
			Value: payload,
			Headers: []kafka.Header{
				{
					Key:   model.RecordTypeHeader,
					Value: []byte(model.RecordTypeEndOfReplay),
				},
			},
			Time: forwarded.EmittedAt,
		},
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka EndOfReplay edge=%s fallita: %w",
			aggregator.edgeID,
			err,
		)
	}

	aggregator.ended = true

	return nil
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

		Events: window.Events,

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

func waitForShutdown(
	processorDone <-chan error,
) error {
	signals := make(
		chan os.Signal,
		1,
	)

	signal.Notify(
		signals,
		os.Interrupt,
		syscall.SIGTERM,
	)

	select {
	case <-signals:
		return nil
	case err := <-processorDone:
		return err
	}
}

func printEdgeSummary(
	edgeID string,
	stats EdgeStatsSnapshot,
) {
	fmt.Printf("\nEdge %s summary\n", edgeID)
	fmt.Printf("MQTT telemetry ricevuta: %d\n", stats.TelemetryReceived)
	fmt.Printf("Ingress queue capacity: %d\n", stats.IngressQueueCapacity)
	fmt.Printf("Max ingress queue depth: %d\n", stats.MaxIngressQueueDepthObserved)
	fmt.Printf("Max ingress queue utilization: %.1f%%\n", stats.MaxIngressQueueUtilization())
	fmt.Printf("Ingress accettata: %d\n", stats.IngressAccepted)
	fmt.Printf("Ingress queue drop: %d\n", stats.IngressQueueDropped)
	fmt.Printf("Telemetry invalida scartata: %d\n", stats.InvalidTelemetry)
	fmt.Printf("Finestre chiuse/out-of-order scartati: %d\n", stats.OutOfOrderDropped)
	fmt.Printf("Telemetry post-EOS scartata: %d\n", stats.PostEOSDropped)
	fmt.Printf("Telemetry processata: %d\n", stats.Processed)
	fmt.Printf("Aggregate Kafka emessi: %d\n", stats.AggregatesEmitted)
	fmt.Printf("EndOfReplay processati: %d\n", stats.EndOfReplayProcessed)
}
