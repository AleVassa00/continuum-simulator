package model

import (
	"encoding/json"
	"math"
	"testing"
)

func TestNullableFloat64MarshalJSON(t *testing.T) {
	for _, test := range []struct {
		name  string
		value NullableFloat64
		want  string
	}{
		{
			name:  "number",
			value: NullableFloat64{Value: 20.5, Valid: true},
			want:  "20.5",
		},
		{
			name:  "null",
			value: NullableFloat64{},
			want:  "null",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(payload) != test.want {
				t.Fatalf("JSON=%s, atteso %s", payload, test.want)
			}
		})
	}
}

func TestNullableFloat64UnmarshalJSON(t *testing.T) {
	for _, test := range []struct {
		name      string
		payload   string
		wantValue float64
		wantValid bool
	}{
		{name: "number", payload: "20.5", wantValue: 20.5, wantValid: true},
		{name: "null", payload: "null", wantValid: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := NullableFloat64{Value: 99, Valid: true}
			if err := json.Unmarshal([]byte(test.payload), &value); err != nil {
				t.Fatal(err)
			}
			if value.Valid != test.wantValid || value.Value != test.wantValue {
				t.Fatalf("valore=%#v", value)
			}
		})
	}
}

func TestNullableFloat64RejectsNonFiniteValue(t *testing.T) {
	_, err := json.Marshal(NullableFloat64{Value: math.NaN(), Valid: true})
	if err == nil {
		t.Fatal("NaN serializzato senza errore")
	}
}
