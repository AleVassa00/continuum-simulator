package model

import (
	"fmt"
	"strings"
	"time"
)

type EndOfReplay struct {
	EdgeID        string    `json:"edge_id"`
	LastEventTime time.Time `json:"last_event_time"`
	EmittedAt     time.Time `json:"emitted_at"`
}

func ValidateEndOfReplay(record EndOfReplay) error {
	if strings.TrimSpace(record.EdgeID) == "" {
		return fmt.Errorf("edge_id EndOfReplay mancante")
	}

	if record.LastEventTime.IsZero() {
		return fmt.Errorf("last_event_time EndOfReplay mancante")
	}

	if record.EmittedAt.IsZero() {
		return fmt.Errorf("emitted_at EndOfReplay mancante")
	}

	return nil
}
