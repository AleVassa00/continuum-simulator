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

const (
	datasetPath  = "dataset/derived/2025-01_bme280_europe_sensors-150_seed-42.csv"
	topologyPath = "dataset/output/kmeans_topology.csv"
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

type SensorAssignment struct {
	EdgeID      string
	MacroareaID string
}

func main() {
	topology, err := loadTopology(topologyPath)
	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"Topologia caricata: %d sensori\n\n",
		len(topology),
	)

	file, err := os.Open(datasetPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	header, err := reader.Read()
	if err != nil {
		panic(err)
	}

	columns := buildColumnIndex(header)

	sequences := make(
		map[string]uint64,
	)

	mqttClients := make(
		map[string]mqtt.Client,
	)

	defer func() {
		for _, client := range mqttClients {
			if client.IsConnected() {
				client.Disconnect(250)
			}
		}
	}()

	maxEvents, err := loadMaxEvents()
	if err != nil {
		panic(err)
	}

	publishedEvents := 0

	var lastObservedAt time.Time

	for {
		if maxEvents > 0 &&
			publishedEvents >= maxEvents {
			break
		}

		row, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		measurement, err := parseMeasurement(
			row,
			columns,
		)
		if err != nil {
			panic(err)
		}

		// Per ora assumiamo che il replay sia ordinato temporalmente.
		// Questo ci permette di usare sull'Edge una sola finestra attiva.
		if !lastObservedAt.IsZero() &&
			measurement.Timestamp.Before(lastObservedAt) {
			panic(
				fmt.Sprintf(
					"replay non ordinato temporalmente: %s arriva dopo %s",
					measurement.Timestamp.Format(time.RFC3339),
					lastObservedAt.Format(time.RFC3339),
				),
			)
		}

		lastObservedAt = measurement.Timestamp

		assignment, found := topology[measurement.SensorID]
		if !found {
			panic(
				fmt.Sprintf(
					"sensore %q non presente nella topologia",
					measurement.SensorID,
				),
			)
		}

		broker, err := brokerAddress(
			assignment.EdgeID,
		)
		if err != nil {
			panic(err)
		}

		client, err := getMQTTClient(
			mqttClients,
			assignment.EdgeID,
			broker,
		)
		if err != nil {
			panic(err)
		}

		sequences[measurement.SensorID]++

		sequence := sequences[measurement.SensorID]

		event := buildSensorEvent(
			measurement,
			sequence,
		)

		topic := fmt.Sprintf(
			"telemetry/%s",
			measurement.SensorID,
		)

		payload, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}

		token := client.Publish(
			topic,
			1,
			false,
			payload,
		)

		if !token.WaitTimeout(5 * time.Second) {
			panic(
				fmt.Sprintf(
					"timeout pubblicazione MQTT sul topic %s",
					topic,
				),
			)
		}

		if token.Error() != nil {
			panic(
				fmt.Errorf(
					"errore pubblicazione MQTT sul topic %s: %w",
					topic,
					token.Error(),
				),
			)
		}

		publishedEvents++

		if publishedEvents%1000 == 0 {
			fmt.Printf(
				"Pubblicati %d eventi\n",
				publishedEvents,
			)
		}
	}

	fmt.Printf(
		"\nReplay terminato: %d eventi pubblicati\n",
		publishedEvents,
	)
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

func loadTopology(
	path string,
) (map[string]SensorAssignment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','

	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := buildColumnIndex(header)

	sensorIDIndex, err := requiredColumn(
		columns,
		"sensor_id",
	)
	if err != nil {
		return nil, err
	}

	edgeIDIndex, err := requiredColumn(
		columns,
		"edge_id",
	)
	if err != nil {
		return nil, err
	}

	macroareaIDIndex, err := requiredColumn(
		columns,
		"macroarea_id",
	)
	if err != nil {
		return nil, err
	}

	topology := make(
		map[string]SensorAssignment,
	)

	for {
		row, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		sensorID := strings.TrimSpace(
			row[sensorIDIndex],
		)

		assignment := SensorAssignment{
			EdgeID: strings.TrimSpace(
				row[edgeIDIndex],
			),

			MacroareaID: strings.TrimSpace(
				row[macroareaIDIndex],
			),
		}

		topology[sensorID] = assignment
	}

	return topology, nil
}

func brokerAddress(
	edgeID string,
) (string, error) {
	const basePort = 18830

	var edgeNumber int

	n, err := fmt.Sscanf(
		edgeID,
		"edge-%d",
		&edgeNumber,
	)
	if err != nil || n != 1 {
		return "",
			fmt.Errorf(
				"edge_id non valido %q",
				edgeID,
			)
	}

	if edgeNumber < 0 {
		return "",
			fmt.Errorf(
				"numero Edge non valido in %q",
				edgeID,
			)
	}

	return fmt.Sprintf(
		"tcp://localhost:%d",
		basePort+edgeNumber,
	), nil
}

func getMQTTClient(
	clients map[string]mqtt.Client,
	edgeID string,
	broker string,
) (mqtt.Client, error) {
	if client, found := clients[edgeID]; found {
		return client, nil
	}

	options := mqtt.NewClientOptions()

	options.AddBroker(
		broker,
	)

	options.SetClientID(
		"simulator-" + edgeID,
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
				broker,
			)
	}

	if token.Error() != nil {
		return nil,
			fmt.Errorf(
				"connessione MQTT a %s fallita: %w",
				broker,
				token.Error(),
			)
	}

	clients[edgeID] = client

	fmt.Printf(
		"Connesso a %s (%s)\n",
		edgeID,
		broker,
	)

	return client, nil
}

func loadMaxEvents() (int, error) {
	value := strings.TrimSpace(
		os.Getenv("MAX_EVENTS"),
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
