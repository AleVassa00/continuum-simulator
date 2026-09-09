package globalaggregator

import (
	"fmt"
	"sort"
	"time"

	"continuum/internal/model"
)

type windowKey struct {
	start int64
	end   int64
}

type windowState struct {
	start time.Time
	end   time.Time

	contributors map[int]model.CloudPartitionAggregate // source partition -> immutable partial
	events       uint64

	temperature metricState
	humidity    metricState
	pressure    metricState
}

type metricState struct {
	valid   uint64
	invalid uint64
	sum     float64
	min     float64
	max     float64
}

func (aggregator *Aggregator) sortedOpenWindowKeys() []windowKey {
	keys := make([]windowKey, 0, len(aggregator.windows))
	for key := range aggregator.windows {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i int, j int) bool {
		if keys[i].start == keys[j].start {
			return keys[i].end < keys[j].end
		}
		return keys[i].start < keys[j].start
	})
	return keys
}

func makeWindowKey(start time.Time, end time.Time) windowKey {
	return windowKey{
		start: start.UTC().UnixNano(),
		end:   end.UTC().UnixNano(),
	}
}

func (state *windowState) add(input model.CloudPartitionAggregate) {
	state.contributors[input.SourcePartition] = input
}

func (state *metricState) add(input model.MetricAggregate) {
	if input.Valid > 0 {
		if state.valid == 0 {
			state.min = *input.Min
			state.max = *input.Max
		} else {
			state.min = min(state.min, *input.Min)
			state.max = max(state.max, *input.Max)
		}
	}
	state.valid += input.Valid
	state.invalid += input.Invalid
	state.sum += input.Sum
}

func (state *windowState) buildAggregate(
	expectedPartitions uint64,
	emittedAt time.Time,
) model.GlobalAggregate {
	// Stable partition order gives the same reduction across worker counts.
	state.events = 0
	state.temperature = metricState{}
	state.humidity = metricState{}
	state.pressure = metricState{}
	for p := 0; p < int(expectedPartitions); p++ {
		input, ok := state.contributors[p]
		if !ok {
			continue
		}
		state.events += input.Events
		state.temperature.add(input.Temperature)
		state.humidity.add(input.Humidity)
		state.pressure.add(input.Pressure)
	}
	return model.GlobalAggregate{
		AggregateID:            buildGlobalAggregateID(state.start, state.end),
		WindowStart:            state.start,
		WindowEnd:              state.end,
		ExpectedPartitions:     expectedPartitions,
		ContributingPartitions: expectedPartitions, // Includes certified zero contributions.
		Events:                 state.events,
		Temperature:            state.temperature.buildAggregate(),
		Humidity:               state.humidity.buildAggregate(),
		Pressure:               state.pressure.buildAggregate(),
		EmittedAt:              emittedAt,
	}
}

func (state metricState) buildAggregate() model.MetricAggregate {
	if state.valid == 0 {
		return model.MetricAggregate{
			Valid:   0,
			Invalid: state.invalid,
			Sum:     0,
		}
	}

	average := state.sum / float64(state.valid)
	minimum := state.min
	maximum := state.max
	return model.MetricAggregate{
		Valid:   state.valid,
		Invalid: state.invalid,
		Sum:     state.sum,
		Average: &average,
		Min:     &minimum,
		Max:     &maximum,
	}
}

func buildGlobalAggregateID(start time.Time, end time.Time) string {
	return fmt.Sprintf(
		"global:%s:%s",
		start.UTC().Format(time.RFC3339Nano),
		end.UTC().Format(time.RFC3339Nano),
	)
}
