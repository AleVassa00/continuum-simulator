package envutil

import (
	"fmt"
	"os"
	"strings"
)

// OrDefault restituisce il valore trimmed della variabile d'ambiente,
// oppure defaultValue se la variabile è vuota o non impostata.
func OrDefault(
	getenv func(string) string,
	name string,
	defaultValue string,
) string {
	value := strings.TrimSpace(getenv(name))
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
