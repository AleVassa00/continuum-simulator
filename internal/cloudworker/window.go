package cloudworker

import (
	"continuum/internal/model"
	"time"
)

type cloudWindowState struct {
	start, end                      time.Time
	inputs                          map[string]model.EdgeAggregate
	events                          uint64
	temperature, humidity, pressure metricState
}
type metricState struct {
	valid, invalid uint64
	sum, min, max  float64
}

func (s *cloudWindowState) add(a model.EdgeAggregate) {
	s.inputs[a.AggregateID] = a
	s.events += a.Events
	s.temperature.add(a.Temperature)
	s.humidity.add(a.Humidity)
	s.pressure.add(a.Pressure)
}
func (s *cloudWindowState) buildAggregate(partition int) model.CloudPartitionAggregate {
	return model.CloudPartitionAggregate{
		AggregateID: model.PartitionAggregateID(partition, s.start, s.end), SourcePartition: partition,
		WindowStart: s.start, WindowEnd: s.end, InputAggregates: uint64(len(s.inputs)), Events: s.events,
		Temperature: s.temperature.buildAggregate(), Humidity: s.humidity.buildAggregate(), Pressure: s.pressure.buildAggregate(), EmittedAt: time.Now().UTC(),
	}
}
func (s *metricState) add(m model.MetricAggregate) {
	if m.Valid > 0 {
		if s.valid == 0 {
			s.min = *m.Min
			s.max = *m.Max
		} else {
			s.min = min(s.min, *m.Min)
			s.max = max(s.max, *m.Max)
		}
	}
	s.valid += m.Valid
	s.invalid += m.Invalid
	s.sum += m.Sum
}
func (s metricState) buildAggregate() model.MetricAggregate {
	m := model.MetricAggregate{Valid: s.valid, Invalid: s.invalid, Sum: s.sum}
	if s.valid > 0 {
		avg := s.sum / float64(s.valid)
		m.Average = &avg
		m.Min = &s.min
		m.Max = &s.max
	}
	return m
}
