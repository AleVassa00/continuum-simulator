package mqtttransport

import (
	"context"
	"fmt"

	"continuum/internal/config"
	"continuum/internal/ports"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type Subscriber struct {
	connection *autopaho.ConnectionManager
}

func NewSubscriber(
	ctx context.Context,
	cfg config.MQTTConfig,
	clientID string,
	edgeID string,
	handler ports.SensorEventHandler,
) (*Subscriber, error) {
	if handler == nil {
		return nil, fmt.Errorf("sensor event handler is required")
	}
	subscription, err := edgeSubscription(cfg.TopicTemplate, edgeID)
	if err != nil {
		return nil, err
	}

	subscriptionResult := make(chan error, 1)
	onConnectionUp := func(connection *autopaho.ConnectionManager, _ *paho.Connack) {
		_, subscribeErr := connection.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{
				Topic: subscription,
				QoS:   cfg.QoS,
			}},
		})
		select {
		case subscriptionResult <- subscribeErr:
		default:
		}
	}

	onPublishReceived := func(received paho.PublishReceived) (bool, error) {
		event, err := decodeSensorEvent(received.Packet.Payload)
		if err != nil {
			return true, fmt.Errorf("decode MQTT payload on %q: %w", received.Packet.Topic, err)
		}
		expectedTopic, err := eventTopic(cfg.TopicTemplate, edgeID, event.SensorID)
		if err != nil {
			return true, err
		}
		if received.Packet.Topic != expectedTopic {
			return true, fmt.Errorf(
				"sensor %q arrived on topic %q, expected %q",
				event.SensorID,
				received.Packet.Topic,
				expectedTopic,
			)
		}
		event.EdgeID = edgeID
		if err := handler.HandleSensorEvent(ctx, event); err != nil {
			return true, err
		}
		return true, nil
	}

	connection, err := connect(ctx, cfg, clientID, onConnectionUp, onPublishReceived)
	if err != nil {
		return nil, err
	}
	subscribeContext, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout.Duration)
	defer cancel()
	select {
	case err := <-subscriptionResult:
		if err != nil {
			_ = disconnect(connection)
			return nil, fmt.Errorf("subscribe MQTT client %q to %q: %w", clientID, subscription, err)
		}
		return &Subscriber{connection: connection}, nil
	case <-subscribeContext.Done():
		_ = disconnect(connection)
		return nil, fmt.Errorf(
			"subscribe MQTT client %q to %q: %w",
			clientID,
			subscription,
			subscribeContext.Err(),
		)
	}
}

func (s *Subscriber) Close() error {
	return disconnect(s.connection)
}
