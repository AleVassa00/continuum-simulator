package mqtttransport

import (
	"context"
	"fmt"

	"continuum/internal/config"
	"continuum/internal/domain"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type Publisher struct {
	connection    *autopaho.ConnectionManager
	topicTemplate string
	qos           byte
	retain        bool
}

func NewPublisher(
	ctx context.Context,
	cfg config.MQTTConfig,
	brokerURL string,
	clientID string,
) (*Publisher, error) {

	connection, err := connect(
		ctx,
		cfg,
		brokerURL,
		clientID,
		nil,
		nil,
	)

	if err != nil {
		return nil, err
	}

	return &Publisher{
		connection:    connection,
		topicTemplate: cfg.TopicTemplate,
		qos:           cfg.QoS,
		retain:        cfg.Retain,
	}, nil
}

func (p *Publisher) PublishSensorEvent(
	ctx context.Context,
	event domain.SensorEvent,
) error {

	topic, err := eventTopic(
		p.topicTemplate,
		event.EdgeID,
		event.SensorID,
	)

	if err != nil {
		return err
	}

	payload, err := encodeSensorEvent(event)
	if err != nil {
		return err
	}

	_, err = p.connection.Publish(
		ctx,
		&paho.Publish{
			QoS:     p.qos,
			Retain:  p.retain,
			Topic:   topic,
			Payload: payload,
		},
	)

	if err != nil {
		return fmt.Errorf(
			"publish sensor event %q to %q: %w",
			event.EventID,
			topic,
			err,
		)
	}

	return nil
}

func (p *Publisher) Close() error {
	return disconnect(p.connection)
}
