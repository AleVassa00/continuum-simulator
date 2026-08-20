package mqtttransport

import (
	"context"
	"fmt"
	"sync"

	"continuum/internal/config"
	"continuum/internal/domain"
)

type devicePublisher struct {
	edgeID    string
	publisher *Publisher
}

type DevicePublisherPool struct {
	mqttConfig config.MQTTConfig
	deployment config.DeploymentConfig

	mu         sync.Mutex
	publishers map[string]devicePublisher
}

func NewDevicePublisherPool(
	mqttConfig config.MQTTConfig,
	deployment config.DeploymentConfig,
) *DevicePublisherPool {

	return &DevicePublisherPool{
		mqttConfig: mqttConfig,
		deployment: deployment,
		publishers: make(map[string]devicePublisher),
	}
}

func (p *DevicePublisherPool) PublishSensorEvent(
	ctx context.Context,
	event domain.SensorEvent,
) error {

	if event.SensorID == "" {
		return fmt.Errorf("sensor event has empty sensor ID")
	}

	if event.EdgeID == "" {
		return fmt.Errorf(
			"sensor %q has empty edge ID",
			event.SensorID,
		)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	existing, found := p.publishers[event.SensorID]

	if found {
		if existing.edgeID != event.EdgeID {
			return fmt.Errorf(
				"sensor %q changed edge from %q to %q during replay",
				event.SensorID,
				existing.edgeID,
				event.EdgeID,
			)
		}

		return existing.publisher.PublishSensorEvent(
			ctx,
			event,
		)
	}

	endpoint, found := p.deployment.Edges[event.EdgeID]

	if !found {
		return fmt.Errorf(
			"edge %q has no deployment endpoint",
			event.EdgeID,
		)
	}

	clientID := "sensor-" + event.SensorID

	publisher, err := NewPublisher(
		ctx,
		p.mqttConfig,
		endpoint.MQTTBrokerURL,
		clientID,
	)

	if err != nil {
		return fmt.Errorf(
			"create MQTT publisher for sensor %q on edge %q: %w",
			event.SensorID,
			event.EdgeID,
			err,
		)
	}

	p.publishers[event.SensorID] = devicePublisher{
		edgeID:    event.EdgeID,
		publisher: publisher,
	}

	return publisher.PublishSensorEvent(
		ctx,
		event,
	)
}

func (p *DevicePublisherPool) Close() error {

	p.mu.Lock()

	publishers := make(
		[]devicePublisher,
		0,
		len(p.publishers),
	)

	for _, publisher := range p.publishers {
		publishers = append(
			publishers,
			publisher,
		)
	}

	p.publishers = make(map[string]devicePublisher)

	p.mu.Unlock()

	var firstErr error

	for _, device := range publishers {
		if err := device.publisher.Close(); err != nil &&
			firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
