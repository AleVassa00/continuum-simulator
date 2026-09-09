package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"continuum/internal/globalaggregator"
	"continuum/internal/model"
)

func newGlobalAggregateSink(ctx context.Context, config GlobalAggregatorConfig) (globalaggregator.GlobalAggregateSink, func(), error) {
	if config.SinkType == "postgres" {
		pool, err := newPostgresPool(ctx, config.Postgres)
		if err != nil {
			return nil, nil, err
		}
		return newPostgresSink(pool), pool.Close, nil
	}
	return newJSONLogSink(os.Stdout), func() {}, nil
}

type metricAggregateLog struct {
	Valid   uint64   `json:"valid"`
	Invalid uint64   `json:"invalid"`
	Sum     float64  `json:"sum"`
	Average *float64 `json:"average"`
	Min     *float64 `json:"min"`
	Max     *float64 `json:"max"`
}

type globalAggregateLog struct {
	AggregateID            string             `json:"aggregate_id"`
	WindowStart            time.Time          `json:"window_start"`
	WindowEnd              time.Time          `json:"window_end"`
	ExpectedPartitions     uint64             `json:"expected_partitions"`
	ContributingPartitions uint64             `json:"contributing_partitions"`
	Events                 uint64             `json:"events"`
	Temperature            metricAggregateLog `json:"temperature"`
	Humidity               metricAggregateLog `json:"humidity"`
	Pressure               metricAggregateLog `json:"pressure"`
	EmittedAt              time.Time          `json:"emitted_at"`
}

func metricLog(metric model.MetricAggregate) metricAggregateLog {
	return metricAggregateLog{
		Valid:   metric.Valid,
		Invalid: metric.Invalid,
		Sum:     metric.Sum,
		Average: metric.Average,
		Min:     metric.Min,
		Max:     metric.Max,
	}
}

func newJSONLogSink(writer io.Writer) globalaggregator.GlobalAggregateSink {
	return func(
		_ context.Context,
		aggregate model.GlobalAggregate,
	) error {
		// emit ha già validato il dominio; il sink è responsabile soltanto del formato e dell'I/O.
		payload, err := json.Marshal(globalAggregateLog{
			AggregateID:            aggregate.AggregateID,
			WindowStart:            aggregate.WindowStart,
			WindowEnd:              aggregate.WindowEnd,
			ExpectedPartitions:     aggregate.ExpectedPartitions,
			ContributingPartitions: aggregate.ContributingPartitions,
			Events:                 aggregate.Events,
			Temperature:            metricLog(aggregate.Temperature),
			Humidity:               metricLog(aggregate.Humidity),
			Pressure:               metricLog(aggregate.Pressure),
			EmittedAt:              aggregate.EmittedAt,
		})
		if err != nil {
			return fmt.Errorf(
				"serializzazione GlobalAggregate %q fallita: %w",
				aggregate.AggregateID,
				err,
			)
		}
		if _, err := fmt.Fprintf(writer, "GLOBAL_AGGREGATE %s\n", payload); err != nil {
			return fmt.Errorf("scrittura log GlobalAggregate fallita: %w", err)
		}
		return nil
	}
}
