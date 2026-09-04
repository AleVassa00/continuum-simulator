package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"continuum/internal/mqtttopic"
)

type ReplayPacer struct {
	Epoch              time.Time
	StartAt            time.Time
	AccelerationFactor float64
}

type ReplayRuntime struct {
	PublishTelemetry   TelemetryPublisher
	PublishEndOfReplay EndOfReplayPublisher
}

// ScheduledTime converte l'EventTime originale nella corrispondente deadline della timeline accelerata del replay
func (pacer ReplayPacer) ScheduledTime(eventTime time.Time) time.Time {
	eventOffset := eventTime.Sub(pacer.Epoch)
	acceleratedNanoseconds := float64(eventOffset) / pacer.AccelerationFactor
	acceleratedOffset := time.Duration(acceleratedNanoseconds)

	return pacer.StartAt.Add(acceleratedOffset)
}

// localReplayStart associa l'istante configurato di avvio al riferimento monotono di now, mantenendo invariato il corrispondente wall-clock time
func localReplayStart(now time.Time, configuredStart time.Time) time.Time {
	return now.Add(configuredStart.Sub(now))
}

// waitUntil sospende la goroutine fino alla deadline prevista dell'evento. Se la deadline è già trascorsa, ritorna immediatamente
func waitUntil(scheduledTime time.Time) {
	wait := time.Until(scheduledTime)
	if wait > 0 {
		time.Sleep(wait)
	}
}

// replaySite coordina l'intero replay di un sito: pacing degli eventi, telemetry egress, drain finale e pubblicazione dell'EndOfReplay
func replaySite(reader *csv.Reader, config SimulatorConfig, runtime ReplayRuntime) (stats ReplayStats, replayErr error) {
	stats.QueueCapacity = config.TelemetryQueueCapacity

	anchorNow := time.Now()

	//costruzione del ReplayPacer
	pacer := ReplayPacer{
		Epoch:              config.ReplayEpoch,
		StartAt:            localReplayStart(anchorNow, config.ReplayStartAt),
		AccelerationFactor: config.AccelerationFactor,
	}

	egress, err := newTelemetryEgress(config.TelemetryQueueCapacity, runtime.PublishTelemetry)
	if err != nil {
		return stats, err
	}
	egressClosed := false

	closeEgress :=
		func() {
			if egressClosed {
				return
			}
			recordTelemetryEgressStats(&stats, egress.CloseAndWait())
			egressClosed = true
		}

	defer func() {
		closeEgress()
		stats.CompletedAt = time.Now()
	}()

	if err := runReplayLoop(reader, config, pacer, egress, &stats); err != nil {
		return stats, err
	}

	// Prima dell'EndOfReplay devono essere processati tutti gli eventi telemetry già accettati nella coda
	closeEgress()

	if !stats.ReachedEOF {
		return stats, nil
	}

	if err := publishReplayEnd(config, runtime.PublishEndOfReplay, &stats); err != nil {
		return stats, err
	}

	return stats, nil
}

// runReplayLoop legge gli eventi dal CSV, ne calcola la deadline accelerata e li offre alla telemetry egress rispettando il pacing del replay
func runReplayLoop(reader *csv.Reader, config SimulatorConfig, pacer ReplayPacer, egress *TelemetryEgress, stats *ReplayStats) error {
	header, err := reader.Read()
	if err != nil {
		return err
	}
	//costruzione di mappa associata al nome presente nell'header
	columns := buildColumnIndex(header)

	// mappa contatori per ogni sensore
	sequences := make(map[string]uint64)

	for {
		//teniamo il conto
		if config.MaxEvents > 0 && stats.OfferedEvents >= config.MaxEvents {
			break
		}

		row, err := reader.Read()
		if err == io.EOF {
			stats.ReachedEOF = true
			break
		}
		if err != nil {
			return err
		}

		measurement, err := parseMeasurement(row, columns)
		if err != nil {
			return err
		}
		// Sequence progressivo indipendente per ogni sensore
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

		// Il primo evento verifica che il processo sia partito entro la tolleranza prevista rispetto all'istante configurato
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
		// Il scheduling lag misura quanto l'offerta reale è avvenuta dopo la deadline prevista
		offeredAt := time.Now()
		schedulingLag := offeredAt.Sub(scheduledTime)

		sequences[measurement.SensorID] = sequence

		stats.RecordOffer(offeredAt, schedulingLag)
		stats.LastEventTime = measurement.EventTime

		if egress.TryEnqueue(event) {
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

// publishReplayEnd pubblica l'EndOfReplay solo dopo il raggiungimento dell'EOF e aggiorna le relative statistiche di successo o fallimento
func publishReplayEnd(config SimulatorConfig, publishEndOfReplay EndOfReplayPublisher, stats *ReplayStats) error {
	if stats.LastEventTime.IsZero() {
		return fmt.Errorf("replay %s ha raggiunto EOF senza eventi offerti", config.SiteID)
	}

	if publishEndOfReplay == nil {
		stats.EOSFailures++
		return fmt.Errorf("publisher EndOfReplay non configurato per %s", config.SiteID)
	}

	endTopic := mqtttopic.ReplayEnd(config.SiteID)

	if err := publishEndOfReplay(endTopic); err != nil {
		stats.EOSFailures++
		return fmt.Errorf("pubblicazione EndOfReplay MQTT edge=%s fallita: %w", config.SiteID, err)
	}

	stats.EOSSuccesses++
	return nil
}
