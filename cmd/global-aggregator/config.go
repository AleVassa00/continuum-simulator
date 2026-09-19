package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"continuum/internal/envutil"
	"continuum/internal/globalaggregator"

	"github.com/jackc/pgx/v5/pgxpool"
)

type GlobalAggregatorConfig struct {
	SourcePartitionCount      int
	MaxPartitionWatermarkSkew time.Duration
	KafkaBroker               string
	InputTopic                string
	GroupID                   string
	SinkType                  string
	Postgres                  *pgxpool.Config
}

func loadGlobalAggregatorConfig() (GlobalAggregatorConfig, error) {
	kafkaBroker := envutil.Required("KAFKA_BROKER")
	inputTopic := envutil.OrDefault(
		"KAFKA_INPUT_TOPIC",
		"cloud-partition-aggregates",
	)
	groupID := envutil.OrDefault(
		"KAFKA_GROUP_ID",
		"global-aggregator",
	)

	count, err := envutil.SourcePartitionCount()
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}
	maxPartitionWatermarkSkew, err := time.ParseDuration(envutil.OrDefault(
		"GLOBAL_MAX_PARTITION_WATERMARK_SKEW",
		globalaggregator.DefaultMaxPartitionWatermarkSkew.String(),
	))
	if err != nil || maxPartitionWatermarkSkew <= 0 {
		return GlobalAggregatorConfig{}, fmt.Errorf("GLOBAL_MAX_PARTITION_WATERMARK_SKEW deve essere una durata maggiore di zero")
	}
	sinkType := envutil.OrDefault("GLOBAL_SINK_TYPE", "log")
	var postgres *pgxpool.Config
	switch sinkType {
	case "log":
	case "postgres":
		postgres, err = loadPostgresConfig()
		if err != nil {
			return GlobalAggregatorConfig{}, err
		}
	default:
		return GlobalAggregatorConfig{}, fmt.Errorf("GLOBAL_SINK_TYPE non valido %q: usare log o postgres", sinkType)
	}

	return GlobalAggregatorConfig{
		SourcePartitionCount:      count,
		MaxPartitionWatermarkSkew: maxPartitionWatermarkSkew,
		KafkaBroker:               kafkaBroker,
		InputTopic:                inputTopic,
		GroupID:                   groupID,
		SinkType:                  sinkType,
		Postgres:                  postgres,
	}, nil
}

func loadPostgresConfig() (*pgxpool.Config, error) {
	host := envutil.OrDefault("GLOBAL_POSTGRES_HOST", "")
	database := envutil.OrDefault("GLOBAL_POSTGRES_DATABASE", "")
	user := envutil.OrDefault("GLOBAL_POSTGRES_USER", "")
	// La password non viene normalizzata.
	password := os.Getenv("GLOBAL_POSTGRES_PASSWORD")
	for _, field := range []struct{ name, value string }{
		{"GLOBAL_POSTGRES_HOST", host},
		{"GLOBAL_POSTGRES_DATABASE", database},
		{"GLOBAL_POSTGRES_USER", user},
		{"GLOBAL_POSTGRES_PASSWORD", password},
	} {
		if field.value == "" {
			return nil, fmt.Errorf("variabile %s obbligatoria con GLOBAL_SINK_TYPE=postgres", field.name)
		}
		if strings.ContainsRune(field.value, '\x00') {
			return nil, fmt.Errorf("variabile %s contiene un carattere NUL non valido", field.name)
		}
	}
	port, err := strconv.Atoi(envutil.OrDefault("GLOBAL_POSTGRES_PORT", "5432"))
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("GLOBAL_POSTGRES_PORT deve essere un intero tra 1 e 65535")
	}
	sslMode := envutil.OrDefault("GLOBAL_POSTGRES_SSLMODE", "verify-full")
	switch sslMode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
	default:
		return nil, fmt.Errorf("GLOBAL_POSTGRES_SSLMODE non valido: usare disable, allow, prefer, require, verify-ca o verify-full")
	}
	connection := url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   "/" + database,
		User:   url.User(user),
	}
	query := url.Values{"sslmode": {sslMode}, "connect_timeout": {"5"}}
	connection.RawQuery = query.Encode()
	config, err := pgxpool.ParseConfig(connection.String())
	if err != nil {
		// Non esporre la connection string nell'errore.
		return nil, fmt.Errorf("configurazione PostgreSQL non valida: controllare host e impostazioni SSL del driver")
	}
	// La password viene assegnata dopo il parsing.
	config.ConnConfig.Password = password
	return config, nil
}
