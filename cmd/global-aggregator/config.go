package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"continuum/internal/envutil"
	"continuum/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type GlobalAggregatorConfig struct {
	KafkaBroker string
	InputTopic  string
	GroupID     string
	SinkType    string
	Postgres    *pgxpool.Config
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

	count, err := strconv.Atoi(envutil.OrDefault("SOURCE_PARTITION_COUNT", "6"))
	if err != nil || count != model.SourcePartitionCount {
		return GlobalAggregatorConfig{}, fmt.Errorf("SOURCE_PARTITION_COUNT must be 6")
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
		KafkaBroker: kafkaBroker,
		InputTopic:  inputTopic,
		GroupID:     groupID,
		SinkType:    sinkType,
		Postgres:    postgres,
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
