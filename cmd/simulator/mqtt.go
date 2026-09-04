package main

import (
	"encoding/json"
	"fmt"
	"time"

	"continuum/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTPublish func(
	topic string,
	qos byte,
	retained bool,
	payload interface{},
) mqtt.Token

type EndOfReplayPublisher func(topic string) error

const publishAckTimeout = 5 * time.Second

// connectMQTTClient crea il client MQTT del simulatore e attende il completamento della connessione al broker
func connectMQTTClient(siteID string, endpoint string) (mqtt.Client, error) {

	options := mqtt.NewClientOptions()

	options.AddBroker(endpoint)
	options.SetClientID("simulator-" + siteID)
	options.SetAutoReconnect(true)
	options.SetConnectTimeout(5 * time.Second)

	client := mqtt.NewClient(options)

	token := client.Connect()

	if !token.WaitTimeout(5 * time.Second) {
		return nil, fmt.Errorf("timeout connessione MQTT a %s", endpoint)
	}

	if token.Error() != nil {
		return nil, fmt.Errorf("connessione MQTT a %s fallita: %w", endpoint, token.Error())
	}

	fmt.Printf("Simulator %s connesso a %s\n", siteID, endpoint)

	return client, nil
}

// publishSensorEvent serializza un SensorEvent e lo pubblica con QoS 0. La publish è best effort e non attende alcuna conferma dal broker
func publishSensorEvent(publish MQTTPublish, topic string, event model.SensorEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serializzazione SensorEvent fallita: %w", err)
	}
	token := publish(topic, 0, false, payload)
	if token == nil {
		return fmt.Errorf("client MQTT ha restituito un token nil sul topic %s", topic)
	}

	// non attendiamo il completamento del token
	return token.Error()
}

// publishEndOfReplay pubblica il segnale di fine replay con QoS 1 e attende il completamento della publish entro il timeout configurato
func publishEndOfReplay(publish MQTTPublish, topic string) error {

	// QoS 1: l'EndOfReplay deve essere confermato prima di terminare il replay
	token := publish(topic, 1, false, []byte{})

	if token == nil {
		return fmt.Errorf("client MQTT ha restituito un token nil sul topic %s", topic)
	}

	if !token.WaitTimeout(publishAckTimeout) {
		return fmt.Errorf("timeout PUBACK MQTT topic=%s dopo %s", topic, publishAckTimeout)
	}

	if err := token.Error(); err != nil {
		return fmt.Errorf("publish MQTT topic=%s fallito: %w", topic, err)
	}

	return nil
}
