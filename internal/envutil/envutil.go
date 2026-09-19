package envutil

import (
	"continuum/internal/model"
	"fmt"
	"os"
	"strconv"
	"strings"
)

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

// Legge il numero di partizioni fissato per la run.
func SourcePartitionCount() (int, error) {
	count, err := strconv.Atoi(OrDefault("SOURCE_PARTITION_COUNT", strconv.Itoa(model.DefaultSourcePartitionCount)))
	if err != nil || count <= 0 {
		return 0, fmt.Errorf("SOURCE_PARTITION_COUNT must be a positive integer")
	}
	return count, nil
}
