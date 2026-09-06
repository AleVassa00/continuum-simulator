package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"continuum/internal/globalaggregator"
	"continuum/internal/model"
)

func newJSONLogSink(writer io.Writer) globalaggregator.GlobalAggregateSink {
	return func(
		_ context.Context,
		aggregate model.GlobalAggregate,
	) error {
		if err := globalaggregator.ValidateGlobalAggregate(aggregate); err != nil {
			return err
		}
		payload, err := json.Marshal(aggregate)
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
