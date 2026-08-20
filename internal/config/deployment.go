package config

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type DeploymentConfig struct {
	Edges map[string]EdgeEndpoint `yaml:"edges"`
}

type EdgeEndpoint struct {
	MQTTBrokerURL string `yaml:"mqtt_broker_url"`
}

func LoadDeployment(path string) (DeploymentConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return DeploymentConfig{}, fmt.Errorf(
			"read deployment configuration %q: %w",
			path,
			err,
		)
	}

	var cfg DeploymentConfig

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)

	if err := decoder.Decode(&cfg); err != nil {
		return DeploymentConfig{}, fmt.Errorf(
			"decode deployment configuration %q: %w",
			path,
			err,
		)
	}

	if err := cfg.Validate(); err != nil {
		return DeploymentConfig{}, fmt.Errorf(
			"validate deployment configuration %q: %w",
			path,
			err,
		)
	}

	return cfg, nil
}

func (c DeploymentConfig) Validate() error {
	if len(c.Edges) == 0 {
		return fmt.Errorf("deployment must contain at least one edge")
	}

	for edgeID, endpoint := range c.Edges {
		if strings.TrimSpace(edgeID) == "" {
			return fmt.Errorf("edge ID cannot be empty")
		}

		brokerURL, err := url.Parse(endpoint.MQTTBrokerURL)
		if err != nil ||
			brokerURL.Host == "" ||
			(brokerURL.Scheme != "mqtt" && brokerURL.Scheme != "mqtts") {
			return fmt.Errorf(
				"edge %q has invalid mqtt_broker_url %q",
				edgeID,
				endpoint.MQTTBrokerURL,
			)
		}
	}

	return nil
}
