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

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultGlobalEdgeIdleTimeout = 5 * time.Second

type GlobalAggregatorConfig struct {
	KafkaBroker     string
	InputTopic      string
	GroupID         string
	WindowSize      time.Duration
	WatermarkDelay  time.Duration
	EdgeIdleTimeout time.Duration
	ExpectedEdgeIDs []string
	SinkType        string
	Postgres        *pgxpool.Config
}

func loadGlobalAggregatorConfig() (GlobalAggregatorConfig, error) {
	kafkaBroker := envutil.Required("KAFKA_BROKER")
	inputTopic := envutil.OrDefault(
		"KAFKA_INPUT_TOPIC",
		"cloud-edge-aggregates",
	)
	groupID := envutil.OrDefault(
		"KAFKA_GROUP_ID",
		"global-aggregator",
	)
	windowSize, err := loadGlobalWindowSize()
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}
	watermarkDelay, err := loadGlobalWatermarkDelay(windowSize)
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}
	edgeIdleTimeout, err := loadGlobalEdgeIdleTimeout()
	if err != nil {
		return GlobalAggregatorConfig{}, err
	}
	expectedEdgeIDs, err := loadExpectedEdgeIDs()
	if err != nil {
		return GlobalAggregatorConfig{}, err
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
		KafkaBroker:     kafkaBroker,
		InputTopic:      inputTopic,
		GroupID:         groupID,
		WindowSize:      windowSize,
		WatermarkDelay:  watermarkDelay,
		EdgeIdleTimeout: edgeIdleTimeout,
		ExpectedEdgeIDs: expectedEdgeIDs,
		SinkType:        sinkType,
		Postgres:        postgres,
	}, nil
}

// Il pool riceve una configurazione già interpretata e validata, senza rileggere l'environment.
func loadPostgresConfig() (*pgxpool.Config, error) {
	host := envutil.OrDefault("GLOBAL_POSTGRES_HOST", "")
	database := envutil.OrDefault("GLOBAL_POSTGRES_DATABASE", "")
	user := envutil.OrDefault("GLOBAL_POSTGRES_USER", "")
	// La password è obbligatoria ma non viene modificata: anche gli spazi possono farne parte.
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
		// Gli errori del parser possono contenere la connection string: non la esponiamo.
		return nil, fmt.Errorf("configurazione PostgreSQL non valida: controllare host e impostazioni SSL del driver")
	}
	// La password non entra nella connection string, nemmeno in quella conservata dal driver.
	config.ConnConfig.Password = password
	return config, nil
}

func loadGlobalWindowSize() (time.Duration, error) {
	value := envutil.OrDefault("GLOBAL_WINDOW_SIZE", "15m")
	windowSize, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"GLOBAL_WINDOW_SIZE non valida %q: %w",
			value,
			err,
		)
	}
	if windowSize <= 0 {
		return 0, fmt.Errorf(
			"GLOBAL_WINDOW_SIZE deve essere maggiore di zero",
		)
	}
	return windowSize, nil
}

func loadGlobalWatermarkDelay(
	defaultDelay time.Duration,
) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("GLOBAL_WATERMARK_DELAY"))
	if value == "" {
		return defaultDelay, nil
	}
	delay, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("GLOBAL_WATERMARK_DELAY non valida %q: %w", value, err)
	}
	if delay <= 0 {
		return 0, fmt.Errorf("GLOBAL_WATERMARK_DELAY deve essere maggiore di zero")
	}
	return delay, nil
}

func loadGlobalEdgeIdleTimeout() (time.Duration, error) {
	value := envutil.OrDefault(
		"GLOBAL_EDGE_IDLE_TIMEOUT",
		defaultGlobalEdgeIdleTimeout.String(),
	)
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"GLOBAL_EDGE_IDLE_TIMEOUT non valido %q: %w",
			value,
			err,
		)
	}
	if timeout <= 0 {
		return 0, fmt.Errorf(
			"GLOBAL_EDGE_IDLE_TIMEOUT deve essere maggiore di zero",
		)
	}
	return timeout, nil
}

func loadExpectedEdgeIDs() ([]string, error) {
	value := strings.TrimSpace(os.Getenv("EXPECTED_EDGE_IDS"))
	if value == "" {
		return nil, fmt.Errorf("variabile EXPECTED_EDGE_IDS non impostata")
	}
	parts := strings.Split(value, ",")
	edgeIDs := make([]string, len(parts))
	for index, part := range parts {
		edgeIDs[index] = strings.TrimSpace(part)
	}
	return edgeIDs, nil
}
