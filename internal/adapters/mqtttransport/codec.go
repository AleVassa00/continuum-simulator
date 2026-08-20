package mqtttransport

import (
	"encoding/json"
	"fmt"

	"continuum/internal/domain"
)

func encodeSensorEvent(event domain.SensorEvent) ([]byte, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode sensor event %q: %w", event.EventID, err)
	}
	return payload, nil
}

func decodeSensorEvent(payload []byte) (domain.SensorEvent, error) {
	var event domain.SensorEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return domain.SensorEvent{}, fmt.Errorf("decode sensor event: %w", err)
	}
	return event, nil
}
