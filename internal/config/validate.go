package config

import (
	"fmt"
	"strings"
	"time"
)

func (c Config) Validate() error {

	err := c.validateVersion()
	if err != nil {
		return err
	}

	err = c.validateDataset()
	if err != nil {
		return err
	}

	err = c.validateExperiment()
	if err != nil {
		return err
	}

	err = c.validateProcessing()
	if err != nil {
		return err
	}

	err = c.validateTransport()
	if err != nil {
		return err
	}

	err = c.validateCloud()
	if err != nil {
		return err
	}

	return nil
}

func (c Config) validateVersion() error {

	if c.Version != CurrentVersion {
		return fmt.Errorf(
			"unsupported version %d, expected %d",
			c.Version,
			CurrentVersion,
		)
	}

	return nil
}

func (c Config) validateDataset() error {

	if strings.TrimSpace(c.Dataset.Name) == "" {
		return fmt.Errorf("dataset.name is required")
	}

	if strings.TrimSpace(c.Dataset.ReferenceFile) == "" {
		return fmt.Errorf("dataset.reference_file is required")
	}

	if strings.TrimSpace(c.Dataset.ReplayFile) == "" {
		return fmt.Errorf("dataset.replay_file is required")
	}

	if strings.TrimSpace(c.Dataset.TopologyFile) == "" {
		return fmt.Errorf("dataset.topology_file is required")
	}

	if len([]rune(c.Dataset.Delimiter)) != 1 {
		return fmt.Errorf(
			"dataset.delimiter must contain exactly one character",
		)
	}

	if strings.TrimSpace(c.Dataset.TimestampLayout) == "" {
		return fmt.Errorf(
			"dataset.timestamp_layout is required",
		)
	}

	if strings.TrimSpace(c.Dataset.TimestampTimezone) == "" {
		return fmt.Errorf(
			"dataset.timestamp_timezone is required",
		)
	}

	_, err := time.LoadLocation(c.Dataset.TimestampTimezone)
	if err != nil {
		return fmt.Errorf(
			"dataset.timestamp_timezone %q is invalid: %w",
			c.Dataset.TimestampTimezone,
			err,
		)
	}

	err = validateColumns(c.Dataset.Columns)
	if err != nil {
		return err
	}

	err = validateMeasurements(c.Dataset.Measurements)
	if err != nil {
		return err
	}

	return nil
}

func (c Config) validateExperiment() error {

	if c.Experiment.Warmup.Duration < 0 {
		return fmt.Errorf(
			"experiment.warmup cannot be negative",
		)
	}

	if c.Experiment.Measurement.Duration <= 0 {
		return fmt.Errorf(
			"experiment.measurement must be positive",
		)
	}

	if c.Experiment.DrainTimeout.Duration < 0 {
		return fmt.Errorf(
			"experiment.drain_timeout cannot be negative",
		)
	}

	if c.Experiment.TargetEventsPerSecond <= 0 {
		return fmt.Errorf(
			"experiment.target_events_per_second must be positive",
		)
	}

	if c.Experiment.ReplaySpeedup <= 0 {
		return fmt.Errorf(
			"experiment.replay_speedup must be positive",
		)
	}

	return nil
}

func (c Config) validateProcessing() error {

	edgeWindow := c.Processing.Edge.WindowSize.Duration
	allowedLateness := c.Processing.Edge.AllowedLateness.Duration
	cloudWindow := c.Processing.Cloud.WindowSize.Duration

	if edgeWindow <= 0 {
		return fmt.Errorf(
			"processing.edge.window_size must be positive",
		)
	}

	if allowedLateness < 0 {
		return fmt.Errorf(
			"processing.edge.allowed_lateness cannot be negative",
		)
	}

	if cloudWindow < edgeWindow {
		return fmt.Errorf(
			"processing.cloud.window_size cannot be smaller than the edge window",
		)
	}

	if cloudWindow%edgeWindow != 0 {
		return fmt.Errorf(
			"processing.cloud.window_size must be a multiple of the edge window",
		)
	}

	return nil
}

func (c Config) validateTransport() error {

	err := validateMQTT(c.Transport.SimulatorToEdge)
	if err != nil {
		return err
	}

	err = validateKafka(c.Transport.EdgeToCloud)
	if err != nil {
		return err
	}

	return nil
}

func validateMQTT(mqtt MQTTConfig) error {

	if mqtt.Protocol != "mqtt" {
		return fmt.Errorf(
			"transport.simulator_to_edge.protocol must be mqtt",
		)
	}

	if !strings.Contains(mqtt.TopicTemplate, "{edge_id}") ||
		!strings.Contains(mqtt.TopicTemplate, "{sensor_id}") {
		return fmt.Errorf(
			"transport.simulator_to_edge.topic_template must contain {edge_id} and {sensor_id}",
		)
	}

	if mqtt.QoS > 2 {
		return fmt.Errorf(
			"transport.simulator_to_edge.qos must be between 0 and 2",
		)
	}

	if mqtt.KeepAlive.Duration <= 0 ||
		mqtt.KeepAlive.Duration%time.Second != 0 ||
		mqtt.KeepAlive.Duration/time.Second > 65_535 {
		return fmt.Errorf(
			"transport.simulator_to_edge.keep_alive must be a positive whole number of seconds up to 65535s",
		)
	}

	if mqtt.ConnectTimeout.Duration <= 0 {
		return fmt.Errorf(
			"transport.simulator_to_edge.connect_timeout must be positive",
		)
	}

	return nil
}

func (c Config) validateCloud() error {

	if strings.TrimSpace(c.Cloud.ConsumerGroup) == "" {
		return fmt.Errorf(
			"cloud.consumer_group is required",
		)
	}

	if c.Cloud.Storage.Driver != "postgres" {
		return fmt.Errorf(
			"cloud.storage.driver must be postgres",
		)
	}

	if strings.TrimSpace(c.Cloud.Storage.DSNEnv) == "" {
		return fmt.Errorf(
			"cloud.storage.dsn_env is required",
		)
	}

	return nil
}

func validateColumns(columns ColumnConfig) error {

	if strings.TrimSpace(columns.SensorID) == "" {
		return fmt.Errorf(
			"dataset.columns.sensor_id is required",
		)
	}

	if strings.TrimSpace(columns.LocationID) == "" {
		return fmt.Errorf(
			"dataset.columns.location_id is required",
		)
	}

	if strings.TrimSpace(columns.Timestamp) == "" {
		return fmt.Errorf(
			"dataset.columns.timestamp is required",
		)
	}

	if strings.TrimSpace(columns.Latitude) == "" {
		return fmt.Errorf(
			"dataset.columns.latitude is required",
		)
	}

	if strings.TrimSpace(columns.Longitude) == "" {
		return fmt.Errorf(
			"dataset.columns.longitude is required",
		)
	}

	usedColumns := make(map[string]string)

	err := addColumn(
		usedColumns,
		"sensor_id",
		columns.SensorID,
	)
	if err != nil {
		return err
	}

	err = addColumn(
		usedColumns,
		"location_id",
		columns.LocationID,
	)
	if err != nil {
		return err
	}

	err = addColumn(
		usedColumns,
		"timestamp",
		columns.Timestamp,
	)
	if err != nil {
		return err
	}

	err = addColumn(
		usedColumns,
		"latitude",
		columns.Latitude,
	)
	if err != nil {
		return err
	}

	err = addColumn(
		usedColumns,
		"longitude",
		columns.Longitude,
	)
	if err != nil {
		return err
	}

	return nil
}

func addColumn(
	usedColumns map[string]string,
	logicalName string,
	physicalName string,
) error {

	physicalName = strings.TrimSpace(physicalName)

	previousName, exists := usedColumns[physicalName]

	if exists {
		return fmt.Errorf(
			"dataset columns %s and %s both map to %q",
			previousName,
			logicalName,
			physicalName,
		)
	}

	usedColumns[physicalName] = logicalName

	return nil
}

func validateMeasurements(measurements []MeasurementConfig) error {

	if len(measurements) == 0 {
		return fmt.Errorf(
			"dataset.measurements must not be empty",
		)
	}

	usedNames := make(map[string]bool)
	usedColumns := make(map[string]bool)

	for index, measurement := range measurements {

		prefix := fmt.Sprintf(
			"dataset.measurements[%d]",
			index,
		)

		if strings.TrimSpace(measurement.Name) == "" {
			return fmt.Errorf(
				"%s.name is required",
				prefix,
			)
		}

		if strings.TrimSpace(measurement.Column) == "" {
			return fmt.Errorf(
				"%s.column is required",
				prefix,
			)
		}

		if strings.TrimSpace(measurement.Unit) == "" {
			return fmt.Errorf(
				"%s.unit is required",
				prefix,
			)
		}

		if usedNames[measurement.Name] {
			return fmt.Errorf(
				"duplicate measurement name %q",
				measurement.Name,
			)
		}

		if usedColumns[measurement.Column] {
			return fmt.Errorf(
				"duplicate measurement column %q",
				measurement.Column,
			)
		}

		usedNames[measurement.Name] = true
		usedColumns[measurement.Column] = true

		if measurement.ValidRange.Min >= measurement.ValidRange.Max {
			return fmt.Errorf(
				"%s.valid_range requires min < max",
				prefix,
			)
		}

		if measurement.Normalizer != "identity" &&
			measurement.Normalizer != "pressure_pa_or_hpa" {

			return fmt.Errorf(
				"%s.normalizer must be identity or pressure_pa_or_hpa",
				prefix,
			)
		}
	}

	return nil
}

func validateKafka(kafka KafkaConfig) error {

	if kafka.Protocol != "kafka" {
		return fmt.Errorf(
			"transport.edge_to_cloud.protocol must be kafka",
		)
	}

	if len(kafka.Brokers) == 0 {
		return fmt.Errorf(
			"transport.edge_to_cloud.brokers must not be empty",
		)
	}

	for _, broker := range kafka.Brokers {

		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf(
				"transport.edge_to_cloud.brokers cannot contain an empty broker",
			)
		}
	}

	if strings.TrimSpace(kafka.Topic) == "" {
		return fmt.Errorf(
			"transport.edge_to_cloud.topic is required",
		)
	}

	if kafka.Partitions <= 0 {
		return fmt.Errorf(
			"transport.edge_to_cloud.partitions must be positive",
		)
	}

	if kafka.PartitionKey != "macroarea_id" {
		return fmt.Errorf(
			"transport.edge_to_cloud.partition_key must be macroarea_id",
		)
	}

	if kafka.Delivery != "at_least_once" {
		return fmt.Errorf(
			"transport.edge_to_cloud.delivery must be at_least_once",
		)
	}

	return nil
}
