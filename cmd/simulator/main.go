package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"continuum/internal/model"
)

func main() {
	if err := runSimulator(); err != nil {
		panic(err)
	}
}

func runSimulator() error {
	// incapsuli
	config, err := loadSimulatorConfig(os.Getenv) //variabile di ambiente identificativa
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

	publishTelemetry := func(topic string, event model.SensorEvent) error {
		return publishSensorEvent(client.Publish, topic, event)
	}

	publishEndOfReplayRecord := func(topic string, record model.EndOfReplay) (PublishResult, error) {
		return publishEndOfReplay(client.Publish, topic, record, time.Now)
	}

	replayRuntime := ReplayRuntime{
		Now:                time.Now,
		Sleep:              time.Sleep,
		PublishTelemetry:   publishTelemetry,
		PublishEndOfReplay: publishEndOfReplayRecord,
	}

	stats, replayErr := replaySite(reader, config, replayRuntime)

	printReplaySummary(config.SiteID, stats, replayErr)

	if replayErr != nil {
		return fmt.Errorf("replay %s fallito: %w", config.SiteID, replayErr)
	}

	return nil
}
