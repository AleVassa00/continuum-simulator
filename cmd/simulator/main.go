package main

import (
	"encoding/csv"
	"fmt"

	"continuum/internal/model"
)

func main() {
	if err := runSimulator(); err != nil {
		panic(err)
	}
}

// runSimulator inizializza il Sensor Simulator, configura la connessione MQTT e avvia il replay del dataset
func runSimulator() error {
	config, err := loadSimulatorConfig()
	if err != nil {
		return err
	}

	client, err := connectMQTTClient(config.SiteID, config.MQTTEndpoint)
	if err != nil {
		return err
	}

	defer func() {
		if client.IsConnected() {
			client.Disconnect(250)
		}
	}()

	fmt.Printf("Replay file: %s\n", config.ReplayFile)

	file, err := openReplayFile(config.ReplayFile)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	publishTelemetry :=
		func(topic string, event model.SensorEvent) error {
			return publishSensorEvent(client.Publish, topic, event)
		}

	publishEndOfReplaySignal :=
		func(topic string) error {
			return publishEndOfReplay(client.Publish, topic)
		}
	// Fornisce al replay le funzioni necessarie per pubblicare telemetry ed EndOfReplay senza dipendere direttamente dal client MQTT
	replayRuntime := ReplayRuntime{
		PublishTelemetry:   publishTelemetry,
		PublishEndOfReplay: publishEndOfReplaySignal,
	}

	stats, replayErr := replaySite(reader, config, replayRuntime)

	printReplaySummary(config.SiteID, stats, replayErr)

	if replayErr != nil {
		return fmt.Errorf("replay %s fallito: %w", config.SiteID, replayErr)
	}

	return nil
}
