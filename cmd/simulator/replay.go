package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"continuum/internal/model"
)

/* Riga del dataset */
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

type ReplayPacer struct {
	Epoch              time.Time
	StartAt            time.Time
	AccelerationFactor float64
}

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
	QueueCapacity           int
	MaxQueueDepthObserved   int
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

const replayStartLateTolerance = 1 * time.Second

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

func (stats ReplayStats) MaxQueueUtilization() float64 {
	if stats.QueueCapacity <= 0 {
		return 0
	}

	return float64(stats.MaxQueueDepthObserved) /
		float64(stats.QueueCapacity) * 100
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

func replaySite(reader *csv.Reader, config SimulatorConfig, runtime ReplayRuntime) (stats ReplayStats, replayErr error) {
	stats.QueueCapacity = config.TelemetryQueueCapacity

	anchorNow := runtime.Now()
	pacer := ReplayPacer{
		Epoch:              config.ReplayEpoch,
		StartAt:            localReplayStart(anchorNow, config.ReplayStartAt),
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

	egress, err := egressFactory(config.TelemetryQueueCapacity, runtime.PublishTelemetry, runtime.Now)
	if err != nil {
		return stats, err
	}
	egressClosed := false
	closeEgress := func() {
		if egressClosed {
			return
		}
		egressStats := egress.CloseAndWait()
		if egressStats.QueueCapacity > 0 {
			stats.QueueCapacity = egressStats.QueueCapacity
		}
		stats.MaxQueueDepthObserved = egressStats.MaxQueueDepthObserved
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
	fmt.Printf("Telemetry queue capacity: %d\n", stats.QueueCapacity)
	fmt.Printf("Max telemetry queue depth: %d\n", stats.MaxQueueDepthObserved)
	fmt.Printf("Max telemetry queue utilization: %.1f%%\n", stats.MaxQueueUtilization())
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
