package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Control-plane only, once after successful Kafka drain; retries are idempotent.
// These deadlines never decide event-time completeness or close Cloud windows.
func notifyEdgeCompletion(ctx context.Context, endpoint, edgeID string) error {
	payload, err := json.Marshal(struct {
		EdgeID string `json:"edge_id"`
	}{edgeID})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("completion request: %w", err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				return nil
			}
			if response.StatusCode < 500 {
				return fmt.Errorf("Edge %s completion rejected: HTTP %d", edgeID, response.StatusCode)
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return fmt.Errorf("Edge %s completion notification failed after 3 attempts; run incomplete", edgeID)
}
