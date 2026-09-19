package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"time"
)

// Converte l'event time nel tempo reale della simulazione
type ReplayPacer struct {
	Epoch              time.Time
	StartAt            time.Time
	AccelerationFactor float64
}

type ReplayRuntime struct {
	PublishTelemetry   TelemetryPublisher
	PublishEndOfReplay EndOfReplayPublisher
}

func (pacer ReplayPacer) ScheduledTime(eventTime time.Time) time.Time {
	eventOffset := eventTime.Sub(pacer.Epoch)
	acceleratedNanoseconds := float64(eventOffset) / pacer.AccelerationFactor
	acceleratedOffset := time.Duration(acceleratedNanoseconds)

	return pacer.StartAt.Add(acceleratedOffset)
}

func localReplayStart(now time.Time, configuredStart time.Time) time.Time {
	return now.Add(configuredStart.Sub(now))
}

func waitUntil(scheduledTime time.Time) {
	wait := time.Until(scheduledTime)
	if wait > 0 {
		time.Sleep(wait)
	}
}

// Legge uno shard e accoda gli eventi rispettando il ritmo configurato
func replaySite(reader *csv.Reader, config SimulatorConfig, runtime ReplayRuntime) (stats ReplayStats, replayErr error) {
	stats.QueueCapacity = config.TelemetryQueueCapacity

	anchorNow := time.Now()

	pacer := ReplayPacer{
		Epoch:              config.ReplayEpoch,
		StartAt:            localReplayStart(anchorNow, config.ReplayStartAt),
		AccelerationFactor: config.AccelerationFactor,
	}

	egress := newReplayEgress(config.SiteID, config.TelemetryQueueCapacity, runtime.PublishTelemetry, runtime.PublishEndOfReplay)

	egressClosed := false

	closeEgress := func() error {
		if egressClosed {
			return nil
		}
		egressClosed = true
		egressStats, err := egress.CloseAndWait()
		recordReplayEgressStats(&stats, egressStats)
		if !egressStats.TelemetryDrainedAt.IsZero() {
			stats.CompletedAt = egressStats.TelemetryDrainedAt
		}
		return err
	}

	defer func() {
		if err := closeEgress(); err != nil && replayErr == nil {
			replayErr = err
		}
	}()

	if err := runReplayLoop(reader, config, pacer, egress, &stats); err != nil {
		return stats, err
	}

	// L'EOS segue sempre l'ultima riga del replay.
	egress.EnqueueEndOfReplay()

	if err := closeEgress(); err != nil {
		return stats, err
	}

	return stats, nil
}

// Il loop produce solo telemetria; l'EOS viene aggiunto da replaySite
func runReplayLoop(reader *csv.Reader, config SimulatorConfig, pacer ReplayPacer, egress *ReplayEgress, stats *ReplayStats) error {
	header, err := reader.Read()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateReplayCSVHeader(header); err != nil {
		return err
	}

	sequences := make(map[string]uint64)

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		measurement, err := parseMeasurement(row)
		if err != nil {
			return err
		}
		sequence := sequences[measurement.SensorID] + 1
		event, err := buildSensorEvent(measurement, sequence)
		if err != nil {
			return fmt.Errorf(
				"costruzione SensorEvent sensor_id=%s fallita: %w",
				measurement.SensorID,
				err,
			)
		}

		scheduledTime := pacer.ScheduledTime(measurement.EventTime)

		// Verifica la sincronizzazione all'avvio.
		if stats.OfferedEvents == 0 {
			actualTime := time.Now()
			lateness := actualTime.Sub(scheduledTime)

			if lateness > config.StartLateTolerance {
				return fmt.Errorf(
					"replay %s avviato troppo tardi: primo evento scheduled_at=%s actual_at=%s lateness=%s tolleranza=%s",
					config.SiteID,
					scheduledTime.UTC().Format(time.RFC3339Nano),
					actualTime.UTC().Format(time.RFC3339Nano),
					lateness,
					config.StartLateTolerance,
				)
			}
		}

		waitUntil(scheduledTime)
		offeredAt := time.Now()
		schedulingLag := offeredAt.Sub(scheduledTime)

		sequences[measurement.SensorID] = sequence

		stats.RecordOffer(offeredAt, schedulingLag)

		if egress.TryEnqueueTelemetry(event) {
			stats.TelemetryEnqueued++
		} else {
			stats.TelemetryLocallyDropped++
		}

		if stats.OfferedEvents%10000 == 0 {
			fmt.Printf("%s: offered=%d enqueued=%d locally_dropped=%d lag_medio=%s lag_massimo=%s\n",
				config.SiteID,
				stats.OfferedEvents,
				stats.TelemetryEnqueued,
				stats.TelemetryLocallyDropped,
				stats.AverageSchedulingLag(),
				stats.SchedulingLagMax,
			)
		}
	}

	return nil
}
