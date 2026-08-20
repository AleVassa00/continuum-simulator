package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

func Load(path string) (Config, error) {

	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf(
			"read configuration %q: %w",
			path,
			err,
		)
	}

	var cfg Config

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)

	err = decoder.Decode(&cfg)
	if err != nil {
		return Config{}, fmt.Errorf(
			"decode configuration %q: %w",
			path,
			err,
		)
	}

	applyEnvironmentOverrides(&cfg)

	err = cfg.Validate()
	if err != nil {
		return Config{}, fmt.Errorf(
			"validate configuration %q: %w",
			path,
			err,
		)
	}

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return Config{}, fmt.Errorf(
			"resolve configuration path %q: %w",
			path,
			err,
		)
	}

	baseDirectory := filepath.Dir(absolutePath)

	cfg.Dataset.ReferenceFile = resolvePath(
		baseDirectory,
		cfg.Dataset.ReferenceFile,
	)

	cfg.Dataset.ReplayFile = resolvePath(
		baseDirectory,
		cfg.Dataset.ReplayFile,
	)

	cfg.Dataset.TopologyFile = resolvePath(
		baseDirectory,
		cfg.Dataset.TopologyFile,
	)

	return cfg, nil
}

func applyEnvironmentOverrides(cfg *Config) {
	if brokerURL, found := os.LookupEnv("CONTINUUM_MQTT_BROKER_URL"); found {
		cfg.Transport.SimulatorToEdge.BrokerURL = brokerURL
	}
}

func resolvePath(baseDirectory string, path string) string {

	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}

	fullPath := filepath.Join(baseDirectory, path)

	return filepath.Clean(fullPath)
}
