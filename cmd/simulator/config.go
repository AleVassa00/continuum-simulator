package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type SimulatorConfig struct {
	SiteID                 string
	MQTTEndpoint           string
	ReplayFile             string
	ReplayEpoch            time.Time
	ReplayStartAt          time.Time
	AccelerationFactor     float64
	TelemetryQueueCapacity int
	StartLateTolerance     time.Duration
}

const (
	defaultReplayEpoch            = "2025-01-01T00:00:00Z"
	defaultAccelerationFactor     = 1000.0
	defaultTelemetryQueueCapacity = 1000
	defaultStartLateTolerance     = 10 * time.Second
)

func loadSimulatorConfig() (SimulatorConfig, error) {

	siteID := strings.TrimSpace(os.Getenv("SITE_ID"))
	if siteID == "" {
		return SimulatorConfig{}, fmt.Errorf("variabile SITE_ID non impostata")
	}

	mqttEndpoint := strings.TrimSpace(os.Getenv("MQTT_ENDPOINT"))
	if mqttEndpoint == "" {
		return SimulatorConfig{}, fmt.Errorf("variabile MQTT_ENDPOINT non impostata")
	}

	replayFile := strings.TrimSpace(os.Getenv("REPLAY_FILE"))
	if replayFile == "" {
		return SimulatorConfig{},
			fmt.Errorf("variabile REPLAY_FILE non impostata")
	}

	replayEpochValue := strings.TrimSpace(os.Getenv("REPLAY_EPOCH"))
	if replayEpochValue == "" {
		replayEpochValue = defaultReplayEpoch
	}

	replayEpoch, err := parseRFC3339UTC("REPLAY_EPOCH", replayEpochValue)
	if err != nil {
		return SimulatorConfig{}, err
	}

	replayStartAtValue := strings.TrimSpace(os.Getenv("REPLAY_START_AT"))
	if replayStartAtValue == "" {
		return SimulatorConfig{}, fmt.Errorf("variabile REPLAY_START_AT non impostata")
	}

	replayStartAt, err := parseRFC3339UTC("REPLAY_START_AT", replayStartAtValue)
	if err != nil {
		return SimulatorConfig{}, err
	}

	accelerationFactor, err := parseAccelerationFactor(os.Getenv("ACCELERATION_FACTOR"))
	if err != nil {
		return SimulatorConfig{}, err
	}

	telemetryQueueCapacity, err := parseTelemetryQueueCapacity(os.Getenv("TELEMETRY_QUEUE_CAPACITY"))
	if err != nil {
		return SimulatorConfig{}, err
	}

	startLateTolerance, err := parseStartLateTolerance(os.Getenv("START_LATE_TOLERANCE"))
	if err != nil {
		return SimulatorConfig{}, err
	}

	return SimulatorConfig{
		SiteID:                 siteID,
		MQTTEndpoint:           mqttEndpoint,
		ReplayFile:             replayFile,
		ReplayEpoch:            replayEpoch,
		ReplayStartAt:          replayStartAt,
		AccelerationFactor:     accelerationFactor,
		TelemetryQueueCapacity: telemetryQueueCapacity,
		StartLateTolerance:     startLateTolerance,
	}, nil
}

func parseRFC3339UTC(name string, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s non valido %q: atteso RFC3339 UTC: %w", name, value, err)
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

func parseAccelerationFactor(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultAccelerationFactor, nil
	}

	factor, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("ACCELERATION_FACTOR non valido %q: %w", value, err)
	}

	if factor <= 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return 0, fmt.Errorf("ACCELERATION_FACTOR deve essere finito e maggiore di zero")
	}
	return factor, nil
}

func parseTelemetryQueueCapacity(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultTelemetryQueueCapacity, nil
	}

	capacity, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("TELEMETRY_QUEUE_CAPACITY non valida %q: %w", value, err)
	}

	if capacity <= 0 {
		return 0, fmt.Errorf("TELEMETRY_QUEUE_CAPACITY deve essere maggiore di zero")
	}

	return capacity, nil
}

func parseStartLateTolerance(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultStartLateTolerance, nil
	}

	tolerance, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("START_LATE_TOLERANCE non valida %q: %w", value, err)
	}

	if tolerance <= 0 {
		return 0, fmt.Errorf("START_LATE_TOLERANCE deve essere maggiore di zero")
	}

	return tolerance, nil
}
