package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAggregateSchemaVersionsAfterDeduplicationRemoval(t *testing.T) {
	if EdgeAggregateSchemaVersion != 3 {
		t.Fatalf("EdgeAggregateSchemaVersion=%d, attesa 3", EdgeAggregateSchemaVersion)
	}
	if CloudEdgeAggregateSchemaVersion != 2 {
		t.Fatalf("CloudEdgeAggregateSchemaVersion=%d, attesa 2", CloudEdgeAggregateSchemaVersion)
	}
}

func TestAggregateWireSchemasKeepIDsAndOmitRemovedField(t *testing.T) {
	for name, aggregate := range map[string]interface{}{
		"edge":  EdgeAggregate{AggregateID: "edge-0:window"},
		"cloud": CloudEdgeAggregate{AggregateID: "cloud:edge-0:window"},
	} {
		t.Run(name, func(t *testing.T) {
			payload, err := json.Marshal(aggregate)
			if err != nil {
				t.Fatal(err)
			}

			encoded := string(payload)
			if !strings.Contains(encoded, `"aggregate_id"`) {
				t.Fatalf("aggregate_id assente dal payload: %s", encoded)
			}
			if strings.Contains(encoded, `"duplicate_events"`) {
				t.Fatalf("duplicate_events ancora presente nel payload: %s", encoded)
			}
		})
	}
}
