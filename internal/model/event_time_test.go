package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEventTimeJSONFieldNames(t *testing.T) {
	eventTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)

	for _, test := range []struct {
		name     string
		value    any
		expected string
	}{
		{"sensor event", SensorEvent{EventTime: eventTime}, `"event_time"`},
		{"end of replay", EndOfReplay{
			EdgeID:        "edge-0",
			LastEventTime: eventTime,
			EmittedAt:     eventTime,
		}, `"last_event_time"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}

			encoded := string(payload)
			if !strings.Contains(encoded, test.expected) {
				t.Fatalf("campo JSON %s assente: %s", test.expected, encoded)
			}
			if strings.Contains(encoded, "observed_at") ||
				strings.Contains(encoded, "last_observed_at") {
				t.Fatalf("campo JSON legacy presente: %s", encoded)
			}
		})
	}
}
