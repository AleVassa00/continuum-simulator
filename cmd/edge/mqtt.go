package main

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"continuum/internal/mqtttopic"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SubscriptionCoordinator struct {
	generation atomic.Uint64
}

type SubscriptionRetryPolicy struct {
	Attempts int
	Timeout  time.Duration
	Backoff  time.Duration
}

const (
	mqttSubscriptionAttempts = 3
	mqttSubscriptionTimeout  = 5 * time.Second
	mqttSubscriptionBackoff  = 250 * time.Millisecond
)

var errSubscriptionInactive = errors.New("tentativo di sottoscrizione MQTT non piu attivo")

func (
	coordinator *SubscriptionCoordinator,
) Begin() uint64 {
	return coordinator.generation.Add(1)
}

func (
	coordinator *SubscriptionCoordinator,
) Invalidate() {
	coordinator.generation.Add(1)
}

func (
	coordinator *SubscriptionCoordinator,
) IsCurrent(
	generation uint64,
) bool {
	return coordinator.generation.Load() == generation
}

func connectEdgeMQTTClient(
	config EdgeConfig,
	ingress *EdgeIngressQueue,
	stats *EdgeStats,
	readiness *ReadinessState,
	subscriptions *SubscriptionCoordinator,
) (mqtt.Client, error) {
	options := mqtt.NewClientOptions()

	options.AddBroker(
		config.MQTTBroker,
	)

	options.SetClientID(
		"edge-consumer-" + config.EdgeID,
	)

	options.SetAutoReconnect(
		true,
	)

	options.SetConnectTimeout(
		5 * time.Second,
	)

	options.SetOrderMatters(true)

	options.SetOnConnectHandler(
		func(client mqtt.Client) {
			readiness.MarkNotReady()
			generation := subscriptions.Begin()

			fmt.Printf(
				"%s connesso al broker MQTT\n",
				config.EdgeID,
			)

			subscribeToEdgeTopics(
				client,
				config.EdgeID,
				ingress,
				stats,
				readiness,
				subscriptions,
				generation,
			)
		},
	)

	options.SetConnectionLostHandler(
		func(
			client mqtt.Client,
			err error,
		) {
			subscriptions.Invalidate()
			readiness.MarkNotReady()

			fmt.Printf(
				"%s ha perso la connessione MQTT: %v\n",
				config.EdgeID,
				err,
			)
		},
	)

	client := mqtt.NewClient(
		options,
	)

	token := client.Connect()

	if !token.WaitTimeout(
		5 * time.Second,
	) {
		return nil,
			fmt.Errorf(
				"timeout connessione MQTT per %s",
				config.EdgeID,
			)
	}

	if token.Error() != nil {
		return nil,
			fmt.Errorf(
				"connessione MQTT fallita per %s: %w",
				config.EdgeID,
				token.Error(),
			)
	}

	return client, nil
}

func subscribeToEdgeTopics(
	client mqtt.Client,
	edgeID string,
	ingress *EdgeIngressQueue,
	stats *EdgeStats,
	readiness *ReadinessState,
	coordinator *SubscriptionCoordinator,
	generation uint64,
) {
	readiness.MarkNotReady()

	topics := edgeSubscriptionTopics(edgeID)
	handler := makeEdgeMessageHandler(
		edgeID,
		ingress,
		stats,
	)

	attempts, err := retrySubscription(
		SubscriptionRetryPolicy{
			Attempts: mqttSubscriptionAttempts,
			Timeout:  mqttSubscriptionTimeout,
			Backoff:  mqttSubscriptionBackoff,
		},
		func() bool {
			return coordinator.IsCurrent(generation) &&
				client.IsConnected()
		},
		func(timeout time.Duration) error {
			token := client.SubscribeMultiple(
				topics,
				handler,
			)

			if !token.WaitTimeout(timeout) {
				return fmt.Errorf("timeout sottoscrizione MQTT")
			}

			if token.Error() != nil {
				return fmt.Errorf(
					"errore sottoscrizione MQTT: %w",
					token.Error(),
				)
			}

			return nil
		},
	)
	if err != nil {
		fmt.Printf(
			"%s: sottoscrizione MQTT non attiva dopo %d tentativi: %v\n",
			edgeID,
			attempts,
			err,
		)

		return
	}

	readiness.MarkReady()

	fmt.Printf(
		"%s sottoscritto a %s e %s\n\n",
		edgeID,
		mqtttopic.TelemetrySubscription,
		mqtttopic.ReplayEnd(edgeID),
	)
}

func edgeSubscriptionTopics(
	edgeID string,
) map[string]byte {
	return map[string]byte{
		mqtttopic.TelemetrySubscription: 0,
		mqtttopic.ReplayEnd(edgeID):     1,
	}
}

func retrySubscription(
	policy SubscriptionRetryPolicy,
	isActive func() bool,
	attempt func(time.Duration) error,
) (int, error) {
	if policy.Attempts <= 0 {
		return 0, fmt.Errorf("numero tentativi di sottoscrizione non valido")
	}

	var lastErr error

	for attemptNumber := 1; attemptNumber <= policy.Attempts; attemptNumber++ {
		if !isActive() {
			return attemptNumber - 1, errSubscriptionInactive
		}

		lastErr = attempt(policy.Timeout)
		if lastErr == nil {
			if !isActive() {
				return attemptNumber, errSubscriptionInactive
			}

			return attemptNumber, nil
		}

		if attemptNumber == policy.Attempts {
			break
		}

		if !isActive() {
			return attemptNumber, errSubscriptionInactive
		}

		time.Sleep(
			policy.Backoff * time.Duration(attemptNumber),
		)
	}

	return policy.Attempts,
		fmt.Errorf(
			"tentativi di sottoscrizione esauriti: %w",
			lastErr,
		)
}

func makeEdgeMessageHandler(
	edgeID string,
	ingress *EdgeIngressQueue,
	stats *EdgeStats,
) mqtt.MessageHandler {
	endTopic := mqtttopic.ReplayEnd(edgeID)

	return func(
		_ mqtt.Client,
		message mqtt.Message,
	) {
		if message.Topic() == endTopic {
			ingress.RegisterEndOfReplay()
			return
		}

		stats.telemetryReceived.Add(1)
		switch ingress.TryEnqueueTelemetry(message.Payload()) {
		case TelemetryEnqueued:
			stats.ingressAccepted.Add(1)
		case TelemetryDroppedAfterEOS:
			stats.postEOSDropped.Add(1)
		case TelemetryDroppedQueueFull,
			TelemetryDroppedQueueClosed:
			stats.ingressQueueDropped.Add(1)
		}
	}
}
