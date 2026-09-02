package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
)

type NullableFloat64 struct {
	Value float64
	Valid bool
}

func (value NullableFloat64) MarshalJSON() ([]byte, error) {
	if !value.Valid {
		return []byte("null"), nil
	}
	if math.IsNaN(value.Value) || math.IsInf(value.Value, 0) {
		return nil, fmt.Errorf("NullableFloat64 contiene un valore non finito")
	}

	return json.Marshal(value.Value)
}

func (value *NullableFloat64) UnmarshalJSON(payload []byte) error {
	if value == nil {
		return fmt.Errorf("destinazione NullableFloat64 nil")
	}

	payload = bytes.TrimSpace(payload)
	if bytes.Equal(payload, []byte("null")) {
		*value = NullableFloat64{}
		return nil
	}

	var decoded float64
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return fmt.Errorf("valore NullableFloat64 non valido: %w", err)
	}
	if math.IsNaN(decoded) || math.IsInf(decoded, 0) {
		return fmt.Errorf("NullableFloat64 contiene un valore non finito")
	}

	*value = NullableFloat64{
		Value: decoded,
		Valid: true,
	}
	return nil
}
