package domain

import (
	"math"
	"time"
)

const SchemaVersion = 1

// SensorEvent is produced by the dataset replayer and routed to exactly one Edge.
// Measurement values remain raw so parsing, normalization and quality accounting
// stay an Edge responsibility.
type SensorEvent struct {
	SchemaVersion int               `json:"schema_version"`
	EventID       string            `json:"event_id"`
	SensorID      string            `json:"sensor_id"`
	LocationID    string            `json:"location_id"`
	EdgeID        string            `json:"-"`
	MacroareaID   string            `json:"-"`
	Sequence      uint64            `json:"sequence"`
	EventTime     time.Time         `json:"observed_at"`
	EmittedAt     time.Time         `json:"emitted_at"`
	Measurements  map[string]string `json:"measurements"`
}

// MetricStats is an exactly mergeable summary. Count, sum and sum of squares
// let Cloud workers combine Edge windows without receiving the original frames.
type MetricStats struct {
	Count      uint64  `json:"count"`
	Min        float64 `json:"min"`
	Max        float64 `json:"max"`
	Sum        float64 `json:"sum"`
	SumSquares float64 `json:"sum_squares"`
}

func (s *MetricStats) Add(value float64) {
	if s.Count == 0 {
		s.Min = value
		s.Max = value
	} else {
		if value < s.Min {
			s.Min = value
		}
		if value > s.Max {
			s.Max = value
		}
	}
	s.Count++
	s.Sum += value
	s.SumSquares += value * value
}

func (s *MetricStats) Merge(other MetricStats) {
	if other.Count == 0 {
		return
	}
	if s.Count == 0 {
		*s = other
		return
	}
	if other.Min < s.Min {
		s.Min = other.Min
	}
	if other.Max > s.Max {
		s.Max = other.Max
	}
	s.Count += other.Count
	s.Sum += other.Sum
	s.SumSquares += other.SumSquares
}

func (s MetricStats) Mean() float64 {
	if s.Count == 0 {
		return math.NaN()
	}
	return s.Sum / float64(s.Count)
}

func (s MetricStats) PopulationStdDev() float64 {
	if s.Count == 0 {
		return math.NaN()
	}
	mean := s.Mean()
	variance := s.SumSquares/float64(s.Count) - mean*mean
	return math.Sqrt(math.Max(variance, 0))
}

type EdgeWindow struct {
	SchemaVersion int                    `json:"schema_version"`
	WindowID      string                 `json:"window_id"`
	EdgeID        string                 `json:"edge_id"`
	MacroareaID   string                 `json:"macroarea_id"`
	WindowStart   time.Time              `json:"window_start"`
	WindowEnd     time.Time              `json:"window_end"`
	InputEvents   uint64                 `json:"input_events"`
	SensorCount   uint64                 `json:"sensor_count"`
	Metrics       map[string]MetricStats `json:"metrics"`
	Quality       QualityCounters        `json:"quality"`
}

type QualityCounters struct {
	MissingValues uint64 `json:"missing_values"`
	InvalidValues uint64 `json:"invalid_values"`
}

type MacroareaWindow struct {
	SchemaVersion int                    `json:"schema_version"`
	WindowID      string                 `json:"window_id"`
	MacroareaID   string                 `json:"macroarea_id"`
	WindowStart   time.Time              `json:"window_start"`
	WindowEnd     time.Time              `json:"window_end"`
	EdgeCount     uint64                 `json:"edge_count"`
	InputEvents   uint64                 `json:"input_events"`
	Metrics       map[string]MetricStats `json:"metrics"`
}
