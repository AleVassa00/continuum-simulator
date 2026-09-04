package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"continuum/internal/experiment"
)

const defaultExperimentPath = "experiments/baseline.yaml"

type experimentDescription struct {
	ExperimentName string `json:"experiment_name"`
	StartLeadTime  string `json:"start_lead_time"`
	Workers        int    `json:"workers"`
	ConfigSHA256   string `json:"config_sha256"`
}

type materializedRun struct {
	BaseTime      string `json:"base_time"`
	ReplayStartAt string `json:"replay_start_at"`
	Workers       int    `json:"workers"`
	ConfigSHA256  string `json:"config_sha256"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("runconfig", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	experimentPath := flags.String(
		"experiment",
		defaultExperimentPath,
		"experiment configuration path",
	)
	describe := flags.Bool(
		"describe",
		false,
		"print the run parameters required before orchestration",
	)
	baseTimeValue := flags.String(
		"base-time",
		"",
		"synchronized RFC3339 timestamp read from the Simulator host",
	)
	effectiveOutputPath := flags.String(
		"output",
		"",
		"effective configuration output path",
	)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unsupported positional arguments: %s", strings.Join(flags.Args(), " "))
	}

	config, err := experiment.Load(*experimentPath)
	if err != nil {
		return err
	}
	configSHA256, err := experiment.Fingerprint(config)
	if err != nil {
		return err
	}

	if *describe {
		if strings.TrimSpace(*baseTimeValue) != "" || strings.TrimSpace(*effectiveOutputPath) != "" {
			return fmt.Errorf("--describe cannot be combined with --base-time or --output")
		}
		return writeJSON(output, experimentDescription{
			ExperimentName: config.Experiment.Name,
			StartLeadTime:  config.Workload.StartLeadTime.String(),
			Workers:        config.Cloud.Workers,
			ConfigSHA256:   configSHA256,
		})
	}

	if strings.TrimSpace(*baseTimeValue) == "" {
		return fmt.Errorf("--base-time is required when materializing a run")
	}
	if strings.TrimSpace(*effectiveOutputPath) == "" {
		return fmt.Errorf("--output is required when materializing a run")
	}

	baseTime, err := time.Parse(time.RFC3339Nano, *baseTimeValue)
	if err != nil {
		return fmt.Errorf("invalid --base-time %q: %w", *baseTimeValue, err)
	}
	replayStartAt := baseTime.UTC().Add(config.Workload.StartLeadTime.Duration())
	effective := experiment.BuildEffective(config, replayStartAt)
	if err := experiment.WriteEffective(*effectiveOutputPath, effective); err != nil {
		return err
	}

	return writeJSON(output, materializedRun{
		BaseTime:      baseTime.UTC().Format(time.RFC3339Nano),
		ReplayStartAt: effective.Workload.ReplayStartAt,
		Workers:       effective.Cloud.Workers,
		ConfigSHA256:  configSHA256,
	})
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("JSON output failed: %w", err)
	}
	return nil
}
