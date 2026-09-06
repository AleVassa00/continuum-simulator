package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"continuum/internal/globalaggregator"
	"continuum/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const postgresOperationTimeout = 5 * time.Second

const insertGlobalAggregate = `INSERT INTO global_aggregates (
	aggregate_id, window_start, window_end, expected_edges, contributing_edges, events,
	temperature_valid, temperature_invalid, temperature_sum, temperature_average, temperature_min, temperature_max,
	humidity_valid, humidity_invalid, humidity_sum, humidity_average, humidity_min, humidity_max,
	pressure_valid, pressure_invalid, pressure_sum, pressure_average, pressure_min, pressure_max,
	emitted_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10, $11, $12,
	$13, $14, $15, $16, $17, $18,
	$19, $20, $21, $22, $23, $24,
	$25
) ON CONFLICT (aggregate_id) DO NOTHING`

func newPostgresPool(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresOperationTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, postgresError("creazione pool", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, postgresError("Ping iniziale", err)
	}
	return pool, nil
}

func newPostgresSink(pool *pgxpool.Pool) globalaggregator.GlobalAggregateSink {
	return func(ctx context.Context, aggregate model.GlobalAggregate) error {
		arguments, err := postgresAggregateArguments(aggregate)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, postgresOperationTimeout)
		defer cancel()
		if _, err := pool.Exec(ctx, insertGlobalAggregate, arguments...); err != nil {
			return postgresError("INSERT GlobalAggregate", err)
		}
		return nil
	}
}

func postgresAggregateArguments(aggregate model.GlobalAggregate) ([]any, error) {
	arguments := []any{
		aggregate.AggregateID, aggregate.WindowStart, aggregate.WindowEnd,
		aggregate.ExpectedEdges, aggregate.ContributingEdges, aggregate.Events,
		aggregate.Temperature.Valid, aggregate.Temperature.Invalid, aggregate.Temperature.Sum,
		aggregate.Temperature.Average, aggregate.Temperature.Min, aggregate.Temperature.Max,
		aggregate.Humidity.Valid, aggregate.Humidity.Invalid, aggregate.Humidity.Sum,
		aggregate.Humidity.Average, aggregate.Humidity.Min, aggregate.Humidity.Max,
		aggregate.Pressure.Valid, aggregate.Pressure.Invalid, aggregate.Pressure.Sum,
		aggregate.Pressure.Average, aggregate.Pressure.Min, aggregate.Pressure.Max,
		aggregate.EmittedAt,
	}
	// I parametri conservano l'ordine delle colonne nell'INSERT; pgx mappa i puntatori nil a NULL.
	for index, argument := range arguments {
		if counter, ok := argument.(uint64); ok {
			if counter > math.MaxInt64 {
				return nil, fmt.Errorf("GlobalAggregate %q: contatore SQL $%d supera BIGINT", aggregate.AggregateID, index+1)
			}
			arguments[index] = int64(counter)
		}
	}
	return arguments, nil
}

// I dettagli del driver/server possono contenere credenziali o dati: esponiamo
// soltanto operazione, cancellazione/timeout e codice SQLSTATE.
func postgresError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("PostgreSQL %s: %w", operation, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("PostgreSQL %s: %w", operation, context.DeadlineExceeded)
	}
	var serverError *pgconn.PgError
	if errors.As(err, &serverError) {
		return fmt.Errorf("PostgreSQL %s fallita: SQLSTATE %s", operation, serverError.Code)
	}
	return fmt.Errorf("PostgreSQL %s fallita: verificare disponibilità, rete, TLS e credenziali", operation)
}
