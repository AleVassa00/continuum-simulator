package model

import (
	"strings"
	"testing"
	"time"
)

func TestValidateEndOfReplay(t *testing.T) {
	valid := EndOfReplay{
		EdgeID:        "edge-3",
		LastEventTime: time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC),
		EmittedAt:     time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC),
	}

	if err := ValidateEndOfReplay(valid); err != nil {
		t.Fatalf("EndOfReplay valido rifiutato: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*EndOfReplay)
		field  string
	}{
		{"edge", func(record *EndOfReplay) { record.EdgeID = " " }, "edge_id"},
		{"last event time", func(record *EndOfReplay) { record.LastEventTime = time.Time{} }, "last_event_time"},
		{"emitted", func(record *EndOfReplay) { record.EmittedAt = time.Time{} }, "emitted_at"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid
			test.mutate(&record)

			err := ValidateEndOfReplay(record)
			if err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}
}
