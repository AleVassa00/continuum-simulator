package model

import "time"

type SensorEvent struct {
	SchemaVersion int               `json:"schema_version"`
	EventID       string            `json:"event_id"`
	SensorID      string            `json:"sensor_id"`
	SensorType    string            `json:"sensor_type"`
	LocationID    string            `json:"location_id"`
	Sequence      uint64            `json:"sequence"`
	EventTime     time.Time         `json:"event_time"`
	EmittedAt     time.Time         `json:"emitted_at"`
	Measurements  map[string]string `json:"measurements"`
}
