package model

import (
	"fmt"
	"strings"
	"time"
)

const EndOfReplaySchemaVersion = 1

type EndOfReplay struct {
	SchemaVersion  int       `json:"schema_version"`
	EdgeID         string    `json:"edge_id"`
	LastObservedAt time.Time `json:"last_observed_at"`
	EmittedAt      time.Time `json:"emitted_at"`
}

func ValidateEndOfReplay(record EndOfReplay) error {
	if record.SchemaVersion != EndOfReplaySchemaVersion {
		return fmt.Errorf(
			"schema_version EndOfReplay non supportata: %d",
			record.SchemaVersion,
		)
	}

	if strings.TrimSpace(record.EdgeID) == "" {
		return fmt.Errorf("edge_id EndOfReplay mancante")
	}

	if record.LastObservedAt.IsZero() {
		return fmt.Errorf("last_observed_at EndOfReplay mancante")
	}

	if record.EmittedAt.IsZero() {
		return fmt.Errorf("emitted_at EndOfReplay mancante")
	}

	return nil
}
