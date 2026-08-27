package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
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
	SiteID       string
	MQTTEndpoint string
	ReplayFile   string
	MaxEvents    int
}

type EventPublisher func(
	topic string,
	event model.SensorEvent,
) error

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

	publishedEvents, err := replaySite(
		reader,
		config,
		func(
			topic string,
			event model.SensorEvent,
		) error {
			return publishSensorEvent(
				client,
				topic,
				event,
			)
		},
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"\nReplay %s terminato: %d eventi pubblicati\n",
		config.SiteID,
		publishedEvents,
	)
}

func replaySite(
	reader *csv.Reader,
	config SimulatorConfig,
	publish EventPublisher,
) (int, error) {
	header, err := reader.Read()
	if err != nil {
		return 0, err
	}

	columns := buildColumnIndex(header)
	sequences := make(map[string]uint64)
	publishedEvents := 0

	var lastObservedAt time.Time

	for {
		if config.MaxEvents > 0 &&
			publishedEvents >= config.MaxEvents {
			break
		}

		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return publishedEvents, err
		}

		measurement, err := parseMeasurement(
			row,
			columns,
		)
		if err != nil {
			return publishedEvents, err
		}

		// Ogni shard deve conservare l'ordine temporale del replay globale.
		if !lastObservedAt.IsZero() &&
			measurement.Timestamp.Before(lastObservedAt) {
			return publishedEvents,
				fmt.Errorf(
					"replay non ordinato temporalmente: %s arriva dopo %s",
					measurement.Timestamp.Format(time.RFC3339),
					lastObservedAt.Format(time.RFC3339),
				)
		}

		lastObservedAt = measurement.Timestamp

		sequence := sequences[measurement.SensorID] + 1
		event := buildSensorEvent(
			measurement,
			sequence,
		)

		topic := telemetryTopic(
			measurement.SensorID,
		)

		if err := publish(
			topic,
			event,
		); err != nil {
			return publishedEvents, err
		}

		sequences[measurement.SensorID] = sequence
		publishedEvents++

		if publishedEvents%1000 == 0 {
			fmt.Printf(
				"%s: pubblicati %d eventi\n",
				config.SiteID,
				publishedEvents,
			)
		}
	}

	return publishedEvents, nil
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

		EmittedAt: time.Now().UTC(),

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
	client mqtt.Client,
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

	token := client.Publish(
		topic,
		1,
		false,
		payload,
	)

	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf(
			"timeout pubblicazione MQTT sul topic %s",
			topic,
		)
	}

	if token.Error() != nil {
		return fmt.Errorf(
			"errore pubblicazione MQTT sul topic %s: %w",
			topic,
			token.Error(),
		)
	}

	return nil
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

	maxEvents, err := parseMaxEvents(
		getenv("MAX_EVENTS"),
	)
	if err != nil {
		return SimulatorConfig{}, err
	}

	return SimulatorConfig{
		SiteID:       siteID,
		MQTTEndpoint: mqttEndpoint,
		ReplayFile:   replayFile,
		MaxEvents:    maxEvents,
	}, nil
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
