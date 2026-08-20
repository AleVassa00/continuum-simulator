package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"time"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {

	duration, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}

	d.Duration = duration

	return nil
}
