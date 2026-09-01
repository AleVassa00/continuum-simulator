package main

import (
	"encoding/json"
	"fmt"
	"time"

	"continuum/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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

const publishAckTimeout = 5 * time.Second

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
