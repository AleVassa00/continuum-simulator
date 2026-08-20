package mqtttransport

import (
	"fmt"
	"strings"
)

const (
	edgePlaceholder   = "{edge_id}"
	sensorPlaceholder = "{sensor_id}"
)

func eventTopic(template, edgeID, sensorID string) (string, error) {
	if strings.TrimSpace(edgeID) == "" {
		return "", fmt.Errorf("edge ID is required")
	}
	if strings.TrimSpace(sensorID) == "" {
		return "", fmt.Errorf("sensor ID is required")
	}
	topic := strings.ReplaceAll(template, edgePlaceholder, edgeID)
	topic = strings.ReplaceAll(topic, sensorPlaceholder, sensorID)
	return topic, nil
}

func edgeSubscription(template, edgeID string) (string, error) {
	if strings.TrimSpace(edgeID) == "" {
		return "", fmt.Errorf("edge ID is required")
	}
	topic := strings.ReplaceAll(template, edgePlaceholder, edgeID)
	topic = strings.ReplaceAll(topic, sensorPlaceholder, "+")
	return topic, nil
}
