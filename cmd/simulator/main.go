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
	SiteID             string
	MQTTEndpoint       string
	ReplayFile         string
	MaxEvents          int
	ReplayEpoch        time.Time
	ReplayStartAt      time.Time
	AccelerationFactor float64
	MQTTMaxInFlight    int
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

type PendingPublish struct {
	EventID     string
	Topic       string
	PublishedAt time.Time
	Token       PublishToken
}

type PendingPublishes struct {
	maxInFlight int
	ackTimeout  time.Duration
	now         func() time.Time
	pending     []PendingPublish
	head        int
	size        int
	peak        int
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

type EventPublisher func(
	topic string,
	event model.SensorEvent,
) (PublishResult, error)

type ReplayRuntime struct {
	Now     func() time.Time
	Sleep   func(time.Duration)
	Publish EventPublisher
}

type ReplayStats struct {
	Events             int
	SchedulingLagTotal time.Duration
	SchedulingLagMax   time.Duration
	PeakInFlight       int
	FirstPublishedAt   time.Time
	LastPublishedAt    time.Time
	CompletedAt        time.Time
}

const (
	defaultReplayEpoch        = "2025-01-01T00:00:00Z"
	defaultAccelerationFactor = 1000.0
	defaultMQTTMaxInFlight    = 1000
	publishAckTimeout         = 5 * time.Second
	replayStartLateTolerance  = 1 * time.Second
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
			Publish: func(
				topic string,
				event model.SensorEvent,
			) (PublishResult, error) {
				return publishSensorEvent(
					client.Publish,
					topic,
					event,
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

func newPendingPublishes(
	maxInFlight int,
	ackTimeout time.Duration,
	now func() time.Time,
) (*PendingPublishes, error) {
	if maxInFlight <= 0 {
		return nil, fmt.Errorf("MQTT_MAX_IN_FLIGHT deve essere maggiore di zero")
	}

	if ackTimeout <= 0 {
		return nil, fmt.Errorf("timeout PUBACK deve essere maggiore di zero")
	}

	if now == nil {
		return nil, fmt.Errorf("clock PUBACK non configurato")
	}

	return &PendingPublishes{
		maxInFlight: maxInFlight,
		ackTimeout:  ackTimeout,
		now:         now,
		pending: make(
			[]PendingPublish,
			maxInFlight,
		),
	}, nil
}

func (
	pending *PendingPublishes,
) Track(
	publish PendingPublish,
) error {
	if publish.Token == nil {
		return fmt.Errorf(
			"token MQTT nil per event_id=%s topic=%s",
			publish.EventID,
			publish.Topic,
		)
	}

	if publish.PublishedAt.IsZero() {
		return fmt.Errorf(
			"istante di pubblicazione MQTT mancante per event_id=%s topic=%s",
			publish.EventID,
			publish.Topic,
		)
	}

	if pending.Len() >= pending.maxInFlight {
		return fmt.Errorf(
			"coda MQTT in-flight già piena prima di tracciare event_id=%s topic=%s",
			publish.EventID,
			publish.Topic,
		)
	}

	pending.enqueue(publish)
	if pending.Len() > pending.peak {
		pending.peak = pending.Len()
	}

	if err := pending.reapCompletedPrefix(); err != nil {
		return err
	}

	if pending.Len() < pending.maxInFlight {
		return nil
	}

	return pending.waitOldest()
}

func (
	pending *PendingPublishes,
) reapCompletedPrefix() error {
	for pending.Len() > 0 {
		publish := pending.oldest()
		select {
		case <-publish.Token.Done():
			pending.popOldest()
			if err := pending.publishError(publish); err != nil {
				return err
			}
		default:
			return nil
		}
	}

	return nil
}

func (
	pending *PendingPublishes,
) waitOldest() error {
	publish := pending.oldest()
	pending.popOldest()
	select {
	case <-publish.Token.Done():
		return pending.publishError(publish)
	default:
	}

	timeRemaining := publish.PublishedAt.
		Add(pending.ackTimeout).
		Sub(pending.now())
	if timeRemaining <= 0 {
		return pending.timeoutError(publish)
	}

	if !publish.Token.WaitTimeout(timeRemaining) {
		return pending.timeoutError(publish)
	}

	return pending.publishError(publish)
}

func (
	pending *PendingPublishes,
) publishError(
	publish PendingPublish,
) error {
	if err := publish.Token.Error(); err != nil {
		return fmt.Errorf(
			"pubblicazione MQTT event_id=%s topic=%s fallita: %w",
			publish.EventID,
			publish.Topic,
			err,
		)
	}

	return nil
}

func (
	pending *PendingPublishes,
) timeoutError(
	publish PendingPublish,
) error {
	return fmt.Errorf(
		"timeout PUBACK MQTT event_id=%s topic=%s dopo %s dal publish",
		publish.EventID,
		publish.Topic,
		pending.ackTimeout,
	)
}

func (
	pending *PendingPublishes,
) WaitUntil(
	scheduledTime time.Time,
	sleep func(time.Duration),
) error {
	if sleep == nil {
		return fmt.Errorf("funzione di sleep non configurata")
	}

	for {
		if err := pending.reapCompletedPrefix(); err != nil {
			return err
		}

		now := pending.now()
		if pending.Len() > 0 {
			ackDeadline := pending.oldest().PublishedAt.
				Add(pending.ackTimeout)
			if !ackDeadline.After(now) {
				if err := pending.waitOldest(); err != nil {
					return err
				}

				continue
			}
		}

		wait := scheduledTime.Sub(now)
		if wait <= 0 {
			return nil
		}

		if pending.Len() == 0 {
			sleep(wait)
			return nil
		}

		ackDeadline := pending.oldest().PublishedAt.
			Add(pending.ackTimeout)
		if !ackDeadline.After(scheduledTime) {
			if err := pending.waitOldest(); err != nil {
				return err
			}

			continue
		}

		sleep(wait)
		return nil
	}
}

func (
	pending *PendingPublishes,
) Drain() error {
	var firstErr error

	for pending.Len() > 0 {
		if err := pending.waitOldest(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (
	pending *PendingPublishes,
) Len() int {
	return pending.size
}

func (
	pending *PendingPublishes,
) enqueue(
	publish PendingPublish,
) {
	tail := (pending.head + pending.size) % len(pending.pending)
	pending.pending[tail] = publish
	pending.size++
}

func (
	pending *PendingPublishes,
) oldest() PendingPublish {
	return pending.pending[pending.head]
}

func (
	pending *PendingPublishes,
) popOldest() {
	pending.pending[pending.head] = PendingPublish{}
	pending.head = (pending.head + 1) % len(pending.pending)
	pending.size--

	if pending.size == 0 {
		pending.head = 0
	}
}

func (
	pending *PendingPublishes,
) Peak() int {
	return pending.peak
}

func (
	stats ReplayStats,
) AverageSchedulingLag() time.Duration {
	if stats.Events == 0 {
		return 0
	}

	return stats.SchedulingLagTotal /
		time.Duration(stats.Events)
}

func (
	stats ReplayStats,
) PublishDuration() time.Duration {
	if stats.Events <= 1 ||
		stats.FirstPublishedAt.IsZero() ||
		stats.LastPublishedAt.IsZero() {
		return 0
	}

	duration := stats.LastPublishedAt.Sub(stats.FirstPublishedAt)
	if duration <= 0 {
		return 0
	}

	return duration
}

func (
	stats ReplayStats,
) DrainDuration() time.Duration {
	if stats.LastPublishedAt.IsZero() || stats.CompletedAt.IsZero() {
		return 0
	}

	duration := stats.CompletedAt.Sub(stats.LastPublishedAt)
	if duration <= 0 {
		return 0
	}

	return duration
}

func (
	stats ReplayStats,
) Throughput() float64 {
	duration := stats.PublishDuration()
	if stats.Events <= 1 || duration <= 0 {
		return 0
	}

	// Primo e ultimo publish delimitano Events-1 intervalli di emissione.
	return float64(stats.Events-1) /
		duration.Seconds()
}

func (
	stats *ReplayStats,
) RecordPublish(
	publishedAt time.Time,
	schedulingLag time.Duration,
) {
	if schedulingLag < 0 {
		schedulingLag = 0
	}

	if stats.Events == 0 {
		stats.FirstPublishedAt = publishedAt
	}

	stats.Events++
	stats.LastPublishedAt = publishedAt
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

	pending, err := newPendingPublishes(
		config.MQTTMaxInFlight,
		publishAckTimeout,
		runtime.Now,
	)
	if err != nil {
		return stats, err
	}

	defer func() {
		stats.CompletedAt = runtime.Now()
		stats.PeakInFlight = pending.Peak()
	}()

	header, err := reader.Read()
	if err != nil {
		return stats, err
	}

	columns := buildColumnIndex(header)
	sequences := make(map[string]uint64)

	var lastObservedAt time.Time

	for {
		if config.MaxEvents > 0 &&
			stats.Events >= config.MaxEvents {
			break
		}

		row, err := reader.Read()
		if err == io.EOF {
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
		if !lastObservedAt.IsZero() &&
			measurement.Timestamp.Before(lastObservedAt) {
			return stats,
				fmt.Errorf(
					"replay non ordinato temporalmente: %s arriva dopo %s",
					measurement.Timestamp.Format(time.RFC3339),
					lastObservedAt.Format(time.RFC3339),
				)
		}

		lastObservedAt = measurement.Timestamp
		scheduledTime, err := pacer.ScheduledTime(
			measurement.Timestamp,
		)
		if err != nil {
			return stats, err
		}

		if stats.Events == 0 {
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

		if err := pending.WaitUntil(
			scheduledTime,
			runtime.Sleep,
		); err != nil {
			return stats, err
		}

		sequence := sequences[measurement.SensorID] + 1
		emittedAt := runtime.Now().UTC()
		event := buildSensorEvent(
			measurement,
			sequence,
			emittedAt,
		)

		topic := telemetryTopic(
			measurement.SensorID,
		)

		result, err := runtime.Publish(
			topic,
			event,
		)
		if err != nil {
			return stats, err
		}

		schedulingLag := result.PublishedAt.Sub(scheduledTime)
		sequences[measurement.SensorID] = sequence
		stats.RecordPublish(
			result.PublishedAt,
			schedulingLag,
		)

		err = pending.Track(
			PendingPublish{
				EventID:     event.EventID,
				Topic:       topic,
				PublishedAt: result.PublishedAt,
				Token:       result.Token,
			},
		)
		if err != nil {
			return stats, err
		}

		if stats.Events%1000 == 0 {
			fmt.Printf(
				"%s: pubblicati=%d pending=%d peak_in_flight=%d lag_medio=%s lag_massimo=%s\n",
				config.SiteID,
				stats.Events,
				pending.Len(),
				pending.Peak(),
				stats.AverageSchedulingLag(),
				stats.SchedulingLagMax,
			)
		}
	}

	if err := pending.Drain(); err != nil {
		return stats, err
	}

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
	fmt.Printf("Eventi pubblicati: %d\n", stats.Events)
	fmt.Printf("Scheduling lag medio: %s\n", stats.AverageSchedulingLag())
	fmt.Printf("Scheduling lag massimo: %s\n", stats.SchedulingLagMax)
	fmt.Printf("Peak MQTT in-flight: %d\n", stats.PeakInFlight)
	fmt.Printf("Durata pubblicazione: %s\n", stats.PublishDuration())
	fmt.Printf("Durata drain finale: %s\n", stats.DrainDuration())
	fmt.Printf("Throughput medio: %.2f eventi/s\n", stats.Throughput())
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
	now func() time.Time,
) (PublishResult, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return PublishResult{},
			fmt.Errorf(
				"serializzazione SensorEvent fallita: %w",
				err,
			)
	}

	publishedAt := now()
	token := publish(
		topic,
		1,
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

	mqttMaxInFlight, err := parseMQTTMaxInFlight(
		getenv("MQTT_MAX_IN_FLIGHT"),
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
		SiteID:             siteID,
		MQTTEndpoint:       mqttEndpoint,
		ReplayFile:         replayFile,
		MaxEvents:          maxEvents,
		ReplayEpoch:        replayEpoch,
		ReplayStartAt:      replayStartAt,
		AccelerationFactor: accelerationFactor,
		MQTTMaxInFlight:    mqttMaxInFlight,
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

func parseMQTTMaxInFlight(
	value string,
) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultMQTTMaxInFlight, nil
	}

	maxInFlight, err := strconv.Atoi(value)
	if err != nil {
		return 0,
			fmt.Errorf(
				"MQTT_MAX_IN_FLIGHT non valido %q: %w",
				value,
				err,
			)
	}

	if maxInFlight <= 0 {
		return 0,
			fmt.Errorf(
				"MQTT_MAX_IN_FLIGHT deve essere maggiore di zero",
			)
	}

	return maxInFlight, nil
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
