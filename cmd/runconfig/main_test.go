package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDescribeReportsExperimentRunParameters(t *testing.T) {
	experimentPath := writeExperimentConfig(t)
	var output bytes.Buffer

	if err := run([]string{
		"--experiment", experimentPath,
		"--describe",
	}, &output); err != nil {
		t.Fatal(err)
	}

	var description experimentDescription
	if err := json.Unmarshal(output.Bytes(), &description); err != nil {
		t.Fatal(err)
	}
	if description.ExperimentName != "aws-test" ||
		description.StartLeadTime != "1m30s" ||
		description.Workers != 3 ||
		len(description.ConfigSHA256) != 64 {
		t.Fatalf("description inattesa: %#v", description)
	}
}

func TestMaterializeUsesRemoteBaseTimeAndWritesEffectiveConfig(t *testing.T) {
	experimentPath := writeExperimentConfig(t)
	effectivePath := filepath.Join(t.TempDir(), "run", "effective-config.yaml")
	var output bytes.Buffer

	if err := run([]string{
		"--experiment", experimentPath,
		"--base-time", "2026-09-05T10:20:30.123456789Z",
		"--output", effectivePath,
	}, &output); err != nil {
		t.Fatal(err)
	}

	var materialized materializedRun
	if err := json.Unmarshal(output.Bytes(), &materialized); err != nil {
		t.Fatal(err)
	}
	const expectedReplayStart = "2026-09-05T10:22:00.123456789Z"
	if materialized.ReplayStartAt != expectedReplayStart ||
		materialized.Workers != 3 ||
		len(materialized.ConfigSHA256) != 64 {
		t.Fatalf("run materializzata inattesa: %#v", materialized)
	}

	payload, err := os.ReadFile(effectivePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"name: aws-test",
		"replay_start_at: \"" + expectedReplayStart + "\"",
		"workers: 3",
		"start_late_tolerance: 10s",
		"watermark_delay: 15m0s",
		"edge_idle_timeout: 5s",
	} {
		if !strings.Contains(string(payload), expected) {
			t.Errorf("effective config senza %q:\n%s", expected, payload)
		}
	}
}

func TestMaterializeRequiresBaseTimeAndOutput(t *testing.T) {
	experimentPath := writeExperimentConfig(t)

	if err := run([]string{"--experiment", experimentPath}, &bytes.Buffer{}); err == nil ||
		!strings.Contains(err.Error(), "--base-time") {
		t.Fatalf("errore base time inatteso: %v", err)
	}
	if err := run([]string{
		"--experiment", experimentPath,
		"--base-time", "2026-09-05T10:20:30Z",
	}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "--output") {
		t.Fatalf("errore output inatteso: %v", err)
	}
}

func writeExperimentConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "experiment.yaml")
	content := `experiment:
  name: aws-test
workload:
  acceleration_factor: 1000
  max_events: 0
  start_lead_time: 90s
simulator:
  telemetry_queue_capacity: 1000
edge:
  window_size: 5m
  ingress_queue_capacity: 1000
cloud:
  workers: 3
  window_size: 15m
global: {}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
