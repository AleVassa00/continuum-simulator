package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"continuum/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SensorMeasurement struct {
	SensorID   string
	SensorType string
	LocationID string

	Latitude  float64
	Longitude float64
	Timestamp time.Time

	Pressure    string
	Temperature string
	Humidity    string
}

type SimulatorConfig struct {
	SiteID                 string
	MQTTEndpoint           string
	ReplayFile             string
	MaxEvents              int
	ReplayEpoch            time.Time
	ReplayStartAt          time.Time
	AccelerationFactor     float64
	TelemetryQueueCapacity int
}

type ReplayPacer struct {
	Epoch              time.Time
	StartAt            time.Time
	AccelerationFactor float64
}

type PublishToken interface {
	WaitTimeout(time.Duration) bool
	Done() <-chan struct{}
	Error() error
}

type PublishResult struct {
	Token       PublishToken
	PublishedAt time.Time
}

type MQTTPublish func(
	topic string,
	qos byte,
	retained bool,
	payload interface{},
) mqtt.Token

type EndOfReplayPublisher func(
	topic string,
	record model.EndOfReplay,
) (PublishResult, error)

type ReplayRuntime struct {
	Now                func() time.Time
	Sleep              func(time.Duration)
	PublishTelemetry   TelemetryPublisher
	PublishEndOfReplay EndOfReplayPublisher
	NewTelemetryEgress TelemetryEgressFactory
}

type ReplayStats struct {
	OfferedEvents           int
	TelemetryEnqueued       int
	TelemetryLocallyDropped int
	MQTTPublishAttempts     uint64
	MQTTPublishErrors       uint64
	SchedulingLagTotal      time.Duration
	SchedulingLagMax        time.Duration
	FirstOfferedAt          time.Time
	LastOfferedAt           time.Time
	CompletedAt             time.Time
	ReachedEOF              bool
	LastObservedAt          time.Time
	EOSSuccesses            int
	EOSFailures             int
}

const (
	defaultReplayEpoch            = "2025-01-01T00:00:00Z"
	defaultAccelerationFactor     = 1000.0
	defaultTelemetryQueueCapacity = 1000
	publishAckTimeout             = 5 * time.Second
	replayStartLateTolerance      = 1 * time.Second
)

func main() {
	config, err := loadSimulatorConfig(
		os.Getenv,
	)
	if err != nil {
		panic(err)
	}

	client, err := connectMQTTClient(
		config.SiteID,
		config.MQTTEndpoint,
	)
	if err != nil {
		panic(err)
	}

	defer func() {
		if client.IsConnected() {
			client.Disconnect(250)
		}
	}()

	fmt.Printf(
		"Replay file: %s\n",
		config.ReplayFile,
	)

	file, err := openReplayFile(
		config.ReplayFile,
	)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	stats, err := replaySite(
		reader,
		config,
		ReplayRuntime{
			Now:   time.Now,
			Sleep: time.Sleep,
			PublishTelemetry: func(
				topic string,
				event model.SensorEvent,
			) error {
				return publishSensorEvent(
					client.Publish,
					topic,
					event,
				)
			},
			PublishEndOfReplay: func(
				topic string,
				record model.EndOfReplay,
			) (PublishResult, error) {
				return publishEndOfReplay(
					client.Publish,
					topic,
					record,
					time.Now,
				)
			},
		},
	)

	printReplaySummary(
		config.SiteID,
		stats,
		err,
	)

	if err != nil {
		panic(
			fmt.Errorf(
				"replay %s fallito: %w",
				config.SiteID,
				err,
			),
		)
	}
}

func (
	pacer ReplayPacer,
) ScheduledTime(
	observedAt time.Time,
) (time.Time, error) {
	if pacer.Epoch.IsZero() {
		return time.Time{}, fmt.Errorf("REPLAY_EPOCH non impostata")
	}

	if pacer.StartAt.IsZero() {
		return time.Time{}, fmt.Errorf("REPLAY_START_AT non impostata")
	}

	if pacer.AccelerationFactor <= 0 ||
		math.IsNaN(pacer.AccelerationFactor) ||
		math.IsInf(pacer.AccelerationFactor, 0) {
		return time.Time{},
			fmt.Errorf(
				"ACCELERATION_FACTOR deve essere finito e maggiore di zero",
			)
	}

	if observedAt.Before(pacer.Epoch) {
		return time.Time{},
			fmt.Errorf(
				"observed_at %s precedente a REPLAY_EPOCH %s",
				observedAt.UTC().Format(time.RFC3339Nano),
				pacer.Epoch.UTC().Format(time.RFC3339Nano),
			)
	}

	eventOffset := observedAt.Sub(pacer.Epoch)
	acceleratedNanoseconds := float64(eventOffset) /
		pacer.AccelerationFactor

	if acceleratedNanoseconds > float64(math.MaxInt64) {
		return time.Time{},
			fmt.Errorf(
				"offset accelerato fuori dal range time.Duration",
			)
	}

	acceleratedOffset := time.Duration(acceleratedNanoseconds)

	return pacer.StartAt.Add(acceleratedOffset), nil
}

func localReplayStart(
	now time.Time,
	configuredStart time.Time,
) time.Time {
	// Add conserva il riferimento monotonic di now sull'istante UTC configurato.
	return now.Add(configuredStart.Sub(now))
}

func waitUntil(
	scheduledTime time.Time,
	now func() time.Time,
	sleep func(time.Duration),
) error {
	if now == nil {
		return fmt.Errorf("clock replay non configurato")
	}
	if sleep == nil {
		return fmt.Errorf("funzione di sleep non configurata")
	}

	wait := scheduledTime.Sub(now())
	if wait > 0 {
		sleep(wait)
	}

	return nil
}

func waitForPublishCompletion(
	result PublishResult,
	topic string,
	now func() time.Time,
) error {
	if result.Token == nil {
		return fmt.Errorf("token MQTT nil sul topic %s", topic)
	}
	if result.PublishedAt.IsZero() {
		return fmt.Errorf("istante publish MQTT mancante sul topic %s", topic)
	}

	select {
	case <-result.Token.Done():
		if err := result.Token.Error(); err != nil {
			return fmt.Errorf("publish MQTT topic=%s fallito: %w", topic, err)
		}
		return nil
	default:
	}

	timeRemaining := result.PublishedAt.
		Add(publishAckTimeout).
		Sub(now())
	if timeRemaining <= 0 ||
		!result.Token.WaitTimeout(timeRemaining) {
		return fmt.Errorf(
			"timeout PUBACK MQTT topic=%s dopo %s dal publish",
			topic,
			publishAckTimeout,
		)
	}

	if err := result.Token.Error(); err != nil {
		return fmt.Errorf("publish MQTT topic=%s fallito: %w", topic, err)
	}

	return nil
}

func (
	stats ReplayStats,
) AverageSchedulingLag() time.Duration {
	if stats.OfferedEvents == 0 {
		return 0
	}

	return stats.SchedulingLagTotal /
		time.Duration(stats.OfferedEvents)
}

func (
	stats ReplayStats,
) OfferDuration() time.Duration {
	if stats.OfferedEvents <= 1 ||
		stats.FirstOfferedAt.IsZero() ||
		stats.LastOfferedAt.IsZero() {
		return 0
	}

	duration := stats.LastOfferedAt.Sub(stats.FirstOfferedAt)
	if duration <= 0 {
		return 0
	}

	return duration
}

func (
	stats ReplayStats,
) DrainDuration() time.Duration {
	if stats.LastOfferedAt.IsZero() || stats.CompletedAt.IsZero() {
		return 0
	}

	duration := stats.CompletedAt.Sub(stats.LastOfferedAt)
	if duration <= 0 {
		return 0
	}

	return duration
}

func (
	stats ReplayStats,
) Throughput() float64 {
	duration := stats.OfferDuration()
	if stats.OfferedEvents <= 1 || duration <= 0 {
		return 0
	}

	// Prima e ultima offerta delimitano OfferedEvents-1 intervalli.
	return float64(stats.OfferedEvents-1) /
		duration.Seconds()
}

func (
	stats *ReplayStats,
) RecordOffer(
	offeredAt time.Time,
	schedulingLag time.Duration,
) {
	if schedulingLag < 0 {
		schedulingLag = 0
	}

	if stats.OfferedEvents == 0 {
		stats.FirstOfferedAt = offeredAt
	}

	stats.OfferedEvents++
	stats.LastOfferedAt = offeredAt
	stats.SchedulingLagTotal += schedulingLag
	stats.SchedulingLagMax = max(
		stats.SchedulingLagMax,
		schedulingLag,
	)
}

func replaySite(
	reader *csv.Reader,
	config SimulatorConfig,
	runtime ReplayRuntime,
) (
	stats ReplayStats,
	replayErr error,
) {
	anchorNow := runtime.Now()
	pacer := ReplayPacer{
		Epoch: config.ReplayEpoch,
		StartAt: localReplayStart(
			anchorNow,
			config.ReplayStartAt,
		),
		AccelerationFactor: config.AccelerationFactor,
	}

	egressFactory := runtime.NewTelemetryEgress
	if egressFactory == nil {
		egressFactory = func(
			capacity int,
			publish TelemetryPublisher,
			now func() time.Time,
		) (TelemetryQueue, error) {
			return newTelemetryEgress(capacity, publish, now)
		}
	}

	egress, err := egressFactory(
		config.TelemetryQueueCapacity,
		runtime.PublishTelemetry,
		runtime.Now,
	)
	if err != nil {
		return stats, err
	}
	egressClosed := false
	closeEgress := func() {
		if egressClosed {
			return
		}
		egressStats := egress.CloseAndWait()
		stats.MQTTPublishAttempts = egressStats.PublishAttempts
		stats.MQTTPublishErrors = egressStats.PublishErrors
		egressClosed = true
	}

	defer func() {
		closeEgress()
		stats.CompletedAt = runtime.Now()
	}()

	header, err := reader.Read()
	if err != nil {
		return stats, err
	}

	columns := buildColumnIndex(header)
	sequences := make(map[string]uint64)

	var previousObservedAt time.Time

	for {
		if config.MaxEvents > 0 &&
			stats.OfferedEvents >= config.MaxEvents {
			break
		}

		row, err := reader.Read()
		if err == io.EOF {
			stats.ReachedEOF = true
			break
		}
		if err != nil {
			return stats, err
		}

		measurement, err := parseMeasurement(
			row,
			columns,
		)
		if err != nil {
			return stats, err
		}

		// Ogni shard deve conservare l'ordine temporale del replay globale.
		if !previousObservedAt.IsZero() &&
			measurement.Timestamp.Before(previousObservedAt) {
			return stats,
				fmt.Errorf(
					"replay non ordinato temporalmente: %s arriva dopo %s",
					measurement.Timestamp.Format(time.RFC3339),
					previousObservedAt.Format(time.RFC3339),
				)
		}

		previousObservedAt = measurement.Timestamp
		scheduledTime, err := pacer.ScheduledTime(
			measurement.Timestamp,
		)
		if err != nil {
			return stats, err
		}

		if stats.OfferedEvents == 0 {
			actualTime := runtime.Now()
			lateness := actualTime.Sub(scheduledTime)
			if lateness > replayStartLateTolerance {
				return stats,
					fmt.Errorf(
						"replay %s avviato troppo tardi: primo evento scheduled_at=%s actual_at=%s lateness=%s tolleranza=%s",
						config.SiteID,
						scheduledTime.UTC().Format(time.RFC3339Nano),
						actualTime.UTC().Format(time.RFC3339Nano),
						lateness,
						replayStartLateTolerance,
					)
			}
		}

		if err := waitUntil(
			scheduledTime,
			runtime.Now,
			runtime.Sleep,
		); err != nil {
			return stats, err
		}

		sequence := sequences[measurement.SensorID] + 1
		offeredAt := runtime.Now()
		schedulingLag := offeredAt.Sub(scheduledTime)
		sequences[measurement.SensorID] = sequence
		stats.RecordOffer(
			offeredAt,
			schedulingLag,
		)
		stats.LastObservedAt = measurement.Timestamp

		if egress.TryEnqueue(measurement, sequence) {
			stats.TelemetryEnqueued++
		} else {
			stats.TelemetryLocallyDropped++
		}

		if stats.OfferedEvents%1000 == 0 {
			fmt.Printf(
				"%s: offered=%d enqueued=%d locally_dropped=%d lag_medio=%s lag_massimo=%s\n",
				config.SiteID,
				stats.OfferedEvents,
				stats.TelemetryEnqueued,
				stats.TelemetryLocallyDropped,
				stats.AverageSchedulingLag(),
				stats.SchedulingLagMax,
			)
		}
	}

	closeEgress()

	if !stats.ReachedEOF {
		return stats, nil
	}

	if stats.LastObservedAt.IsZero() {
		return stats,
			fmt.Errorf(
				"replay %s ha raggiunto EOF senza eventi offerti",
				config.SiteID,
			)
	}

	if runtime.PublishEndOfReplay == nil {
		stats.EOSFailures++
		return stats,
			fmt.Errorf(
				"publisher EndOfReplay non configurato per %s",
				config.SiteID,
			)
	}

	endRecord := model.EndOfReplay{
		SchemaVersion:  model.EndOfReplaySchemaVersion,
		EdgeID:         config.SiteID,
		LastObservedAt: stats.LastObservedAt,
		EmittedAt:      runtime.Now().UTC(),
	}
	endTopic := replayEndTopic(config.SiteID)
	endResult, err := runtime.PublishEndOfReplay(
		endTopic,
		endRecord,
	)
	if err != nil {
		stats.EOSFailures++
		return stats,
			fmt.Errorf(
				"pubblicazione EndOfReplay MQTT edge=%s fallita: %w",
				config.SiteID,
				err,
			)
	}

	if err := waitForPublishCompletion(
		endResult,
		endTopic,
		runtime.Now,
	); err != nil {
		stats.EOSFailures++
		return stats,
			fmt.Errorf(
				"PUBACK EndOfReplay MQTT edge=%s fallito: %w",
				config.SiteID,
				err,
			)
	}
	stats.EOSSuccesses++

	return stats, nil
}

func printReplaySummary(
	siteID string,
	stats ReplayStats,
	replayErr error,
) {
	status := "completato"
	if replayErr != nil {
		status = "fallito"
	}

	fmt.Printf(
		"\nReplay %s %s\n",
		siteID,
		status,
	)
	fmt.Printf("Eventi offered/generated: %d\n", stats.OfferedEvents)
	fmt.Printf("Telemetry accettata in coda: %d\n", stats.TelemetryEnqueued)
	fmt.Printf("Telemetry scartata localmente: %d\n", stats.TelemetryLocallyDropped)
	fmt.Printf("Tentativi publish MQTT QoS0: %d\n", stats.MQTTPublishAttempts)
	fmt.Printf("Errori publish MQTT QoS0: %d\n", stats.MQTTPublishErrors)
	fmt.Printf("Scheduling lag medio: %s\n", stats.AverageSchedulingLag())
	fmt.Printf("Scheduling lag massimo: %s\n", stats.SchedulingLagMax)
	fmt.Printf("Durata workload offerto: %s\n", stats.OfferDuration())
	fmt.Printf("Durata completamento dopo ultima offerta: %s\n", stats.DrainDuration())
	fmt.Printf("Throughput workload offerto: %.2f eventi/s\n", stats.Throughput())
	fmt.Printf("EOS successi: %d\n", stats.EOSSuccesses)
	fmt.Printf("EOS fallimenti: %d\n", stats.EOSFailures)
}

func buildColumnIndex(
	header []string,
) map[string]int {
	columns := make(
		map[string]int,
	)

	for index, name := range header {
		name = strings.TrimSpace(name)

		columns[name] = index
	}

	return columns
}

func requiredColumn(
	columns map[string]int,
	name string,
) (int, error) {
	index, found := columns[name]

	if !found {
		return 0,
			fmt.Errorf(
				"colonna %q non trovata nel CSV",
				name,
			)
	}

	return index, nil
}

func parseMeasurement(
	row []string,
	columns map[string]int,
) (SensorMeasurement, error) {
	sensorIDIndex, err := requiredColumn(
		columns,
		"sensor_id",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	sensorTypeIndex, err := requiredColumn(
		columns,
		"sensor_type",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	locationIndex, err := requiredColumn(
		columns,
		"location",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	latitudeIndex, err := requiredColumn(
		columns,
		"lat",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	longitudeIndex, err := requiredColumn(
		columns,
		"lon",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	timestampIndex, err := requiredColumn(
		columns,
		"timestamp",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	pressureIndex, err := requiredColumn(
		columns,
		"pressure",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	temperatureIndex, err := requiredColumn(
		columns,
		"temperature",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	humidityIndex, err := requiredColumn(
		columns,
		"humidity",
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	latitude, err := strconv.ParseFloat(
		strings.TrimSpace(
			row[latitudeIndex],
		),
		64,
	)
	if err != nil {
		return SensorMeasurement{},
			fmt.Errorf(
				"latitudine non valida %q: %w",
				row[latitudeIndex],
				err,
			)
	}

	longitude, err := strconv.ParseFloat(
		strings.TrimSpace(
			row[longitudeIndex],
		),
		64,
	)
	if err != nil {
		return SensorMeasurement{},
			fmt.Errorf(
				"longitudine non valida %q: %w",
				row[longitudeIndex],
				err,
			)
	}

	timestamp, err := parseTimestamp(
		strings.TrimSpace(
			row[timestampIndex],
		),
	)
	if err != nil {
		return SensorMeasurement{}, err
	}

	measurement := SensorMeasurement{
		SensorID: strings.TrimSpace(
			row[sensorIDIndex],
		),

		SensorType: strings.TrimSpace(
			row[sensorTypeIndex],
		),

		LocationID: strings.TrimSpace(
			row[locationIndex],
		),

		Latitude:  latitude,
		Longitude: longitude,
		Timestamp: timestamp,

		Pressure: strings.TrimSpace(
			row[pressureIndex],
		),

		Temperature: strings.TrimSpace(
			row[temperatureIndex],
		),

		Humidity: strings.TrimSpace(
			row[humidityIndex],
		),
	}

	return measurement, nil
}

func parseTimestamp(
	value string,
) (time.Time, error) {
	timestamp, err := time.Parse(
		time.RFC3339,
		value,
	)

	if err == nil {
		return timestamp, nil
	}

	timestamp, err = time.ParseInLocation(
		"2006-01-02T15:04:05",
		value,
		time.UTC,
	)

	if err != nil {
		return time.Time{},
			fmt.Errorf(
				"timestamp non valido %q: %w",
				value,
				err,
			)
	}

	return timestamp, nil
}

func buildSensorEvent(
	measurement SensorMeasurement,
	sequence uint64,
	emittedAt time.Time,
) model.SensorEvent {
	return model.SensorEvent{
		SchemaVersion: 1,

		EventID: fmt.Sprintf(
			"%s-%d",
			measurement.SensorID,
			sequence,
		),

		SensorID:   measurement.SensorID,
		SensorType: measurement.SensorType,
		LocationID: measurement.LocationID,
		Sequence:   sequence,

		ObservedAt: measurement.Timestamp,

		EmittedAt: emittedAt.UTC(),

		Measurements: map[string]string{
			"pressure":    measurement.Pressure,
			"temperature": measurement.Temperature,
			"humidity":    measurement.Humidity,
		},
	}
}

func openReplayFile(
	path string,
) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil,
			fmt.Errorf(
				"apertura REPLAY_FILE %q fallita: %w",
				path,
				err,
			)
	}

	return file, nil
}

func telemetryTopic(
	sensorID string,
) string {
	return fmt.Sprintf(
		"sensors/%s/telemetry",
		sensorID,
	)
}

func replayEndTopic(
	edgeID string,
) string {
	return fmt.Sprintf(
		"replay/%s/end",
		edgeID,
	)
}

func connectMQTTClient(
	siteID string,
	endpoint string,
) (mqtt.Client, error) {
	options := mqtt.NewClientOptions()

	options.AddBroker(
		endpoint,
	)

	options.SetClientID(
		"simulator-" + siteID,
	)

	options.SetAutoReconnect(
		true,
	)

	options.SetConnectTimeout(
		5 * time.Second,
	)

	client := mqtt.NewClient(
		options,
	)

	token := client.Connect()

	if !token.WaitTimeout(5 * time.Second) {
		return nil,
			fmt.Errorf(
				"timeout connessione MQTT a %s",
				endpoint,
			)
	}

	if token.Error() != nil {
		return nil,
			fmt.Errorf(
				"connessione MQTT a %s fallita: %w",
				endpoint,
				token.Error(),
			)
	}

	fmt.Printf(
		"Simulator %s connesso a %s\n",
		siteID,
		endpoint,
	)

	return client, nil
}

func publishSensorEvent(
	publish MQTTPublish,
	topic string,
	event model.SensorEvent,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf(
			"serializzazione SensorEvent fallita: %w",
			err,
		)
	}

	token := publish(
		topic,
		0,
		false,
		payload,
	)
	if token == nil {
		return fmt.Errorf(
			"client MQTT ha restituito un token nil sul topic %s",
			topic,
		)
	}

	// QoS0 e best effort: non attendiamo il completamento del token.
	return token.Error()
}

func publishEndOfReplay(
	publish MQTTPublish,
	topic string,
	record model.EndOfReplay,
	now func() time.Time,
) (PublishResult, error) {
	if err := model.ValidateEndOfReplay(record); err != nil {
		return PublishResult{},
			fmt.Errorf(
				"EndOfReplay non valido: %w",
				err,
			)
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return PublishResult{},
			fmt.Errorf(
				"serializzazione EndOfReplay fallita: %w",
				err,
			)
	}

	return publishMQTTPayload(
		publish,
		topic,
		payload,
		1,
		now,
	)
}

func publishMQTTPayload(
	publish MQTTPublish,
	topic string,
	payload []byte,
	qos byte,
	now func() time.Time,
) (PublishResult, error) {

	publishedAt := now()
	token := publish(
		topic,
		qos,
		false,
		payload,
	)

	if token == nil {
		return PublishResult{},
			fmt.Errorf(
				"client MQTT ha restituito un token nil sul topic %s",
				topic,
			)
	}

	return PublishResult{
		Token:       token,
		PublishedAt: publishedAt,
	}, nil
}

func loadSimulatorConfig(
	getenv func(string) string,
) (SimulatorConfig, error) {
	siteID := strings.TrimSpace(
		getenv("SITE_ID"),
	)
	if siteID == "" {
		return SimulatorConfig{},
			fmt.Errorf(
				"variabile SITE_ID non impostata",
			)
	}

	mqttEndpoint := strings.TrimSpace(
		getenv("MQTT_ENDPOINT"),
	)
	if mqttEndpoint == "" {
		return SimulatorConfig{},
			fmt.Errorf(
				"variabile MQTT_ENDPOINT non impostata",
			)
	}

	replayFile := strings.TrimSpace(
		getenv("REPLAY_FILE"),
	)
	if replayFile == "" {
		return SimulatorConfig{},
			fmt.Errorf(
				"variabile REPLAY_FILE non impostata",
			)
	}

	replayEpochValue := strings.TrimSpace(
		getenv("REPLAY_EPOCH"),
	)
	if replayEpochValue == "" {
		replayEpochValue = defaultReplayEpoch
	}

	replayEpoch, err := parseRFC3339UTC(
		"REPLAY_EPOCH",
		replayEpochValue,
	)
	if err != nil {
		return SimulatorConfig{}, err
	}

	replayStartAtValue := strings.TrimSpace(
		getenv("REPLAY_START_AT"),
	)
	if replayStartAtValue == "" {
		return SimulatorConfig{},
			fmt.Errorf(
				"variabile REPLAY_START_AT non impostata",
			)
	}

	replayStartAt, err := parseRFC3339UTC(
		"REPLAY_START_AT",
		replayStartAtValue,
	)
	if err != nil {
		return SimulatorConfig{}, err
	}

	accelerationFactor, err := parseAccelerationFactor(
		getenv("ACCELERATION_FACTOR"),
	)
	if err != nil {
		return SimulatorConfig{}, err
	}

	telemetryQueueCapacity, err := parseTelemetryQueueCapacity(
		getenv("TELEMETRY_QUEUE_CAPACITY"),
	)
	if err != nil {
		return SimulatorConfig{}, err
	}

	maxEvents, err := parseMaxEvents(
		getenv("MAX_EVENTS"),
	)
	if err != nil {
		return SimulatorConfig{}, err
	}

	return SimulatorConfig{
		SiteID:                 siteID,
		MQTTEndpoint:           mqttEndpoint,
		ReplayFile:             replayFile,
		MaxEvents:              maxEvents,
		ReplayEpoch:            replayEpoch,
		ReplayStartAt:          replayStartAt,
		AccelerationFactor:     accelerationFactor,
		TelemetryQueueCapacity: telemetryQueueCapacity,
	}, nil
}

func parseRFC3339UTC(
	name string,
	value string,
) (time.Time, error) {
	parsed, err := time.Parse(
		time.RFC3339,
		value,
	)
	if err != nil {
		return time.Time{},
			fmt.Errorf(
				"%s non valido %q: atteso RFC3339 UTC: %w",
				name,
				value,
				err,
			)
	}

	_, offset := parsed.Zone()
	if offset != 0 {
		return time.Time{},
			fmt.Errorf(
				"%s deve essere espresso in UTC: %q",
				name,
				value,
			)
	}

	return parsed.UTC(), nil
}

func parseAccelerationFactor(
	value string,
) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultAccelerationFactor, nil
	}

	factor, err := strconv.ParseFloat(
		value,
		64,
	)
	if err != nil {
		return 0,
			fmt.Errorf(
				"ACCELERATION_FACTOR non valido %q: %w",
				value,
				err,
			)
	}

	if factor <= 0 ||
		math.IsNaN(factor) ||
		math.IsInf(factor, 0) {
		return 0,
			fmt.Errorf(
				"ACCELERATION_FACTOR deve essere finito e maggiore di zero",
			)
	}

	return factor, nil
}

func parseTelemetryQueueCapacity(
	value string,
) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultTelemetryQueueCapacity, nil
	}

	capacity, err := strconv.Atoi(value)
	if err != nil {
		return 0,
			fmt.Errorf(
				"TELEMETRY_QUEUE_CAPACITY non valida %q: %w",
				value,
				err,
			)
	}

	if capacity <= 0 {
		return 0,
			fmt.Errorf(
				"TELEMETRY_QUEUE_CAPACITY deve essere maggiore di zero",
			)
	}

	return capacity, nil
}

func parseMaxEvents(
	value string,
) (int, error) {
	value = strings.TrimSpace(
		value,
	)

	if value == "" {
		return 0, nil
	}

	maxEvents, err := strconv.Atoi(
		value,
	)
	if err != nil {
		return 0,
			fmt.Errorf(
				"MAX_EVENTS non valido %q: %w",
				value,
				err,
			)
	}

	if maxEvents < 0 {
		return 0,
			fmt.Errorf(
				"MAX_EVENTS non può essere negativo",
			)
	}

	return maxEvents, nil
}
