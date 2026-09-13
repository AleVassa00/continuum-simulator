package envutil

import (
	"continuum/internal/model"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// OrDefault restituisce il valore trimmed della variabile d'ambiente,
// oppure defaultValue se la variabile è vuota o non impostata.
func OrDefault(
	name string,
	defaultValue string,
) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue
	}

	return value
}

// Required restituisce il valore trimmed della variabile d'ambiente,
// oppure termina con panic se la variabile è vuota o non impostata.
func Required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		panic(
			fmt.Sprintf(
				"variabile %s non impostata",
				name,
			),
		)
	}

	return value
}

// SourcePartitionCount reads the deploygen-provided, immutable run topology.
func SourcePartitionCount() (int, error) {
	count, err := strconv.Atoi(OrDefault("SOURCE_PARTITION_COUNT", strconv.Itoa(model.DefaultSourcePartitionCount)))
	if err != nil || count <= 0 {
		return 0, fmt.Errorf("SOURCE_PARTITION_COUNT must be a positive integer")
	}
	return count, nil
}
