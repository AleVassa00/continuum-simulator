package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
	"time"

	"continuum/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestReplayPacerScheduledTime(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 10,
	}

	scheduled, err := pacer.ScheduledTime(
		mustTime("2025-01-01T00:10:00Z"),
	)
	if err != nil {
		t.Fatal(err)
	}

	expected := mustTime("2026-08-28T20:01:00Z")
	if !scheduled.Equal(expected) {
		t.Fatalf("scheduled=%s, atteso %s", scheduled, expected)
	}
}

func TestReplayPacerFactorOnePreservesOffset(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 1,
	}

	scheduled, err := pacer.ScheduledTime(
		mustTime("2025-01-01T00:10:00Z"),
	)
	if err != nil {
		t.Fatal(err)
	}

	expected := pacer.StartAt.Add(10 * time.Minute)
	if !scheduled.Equal(expected) {
		t.Fatalf("scheduled=%s, atteso %s", scheduled, expected)
	}
}

func TestReplayPacerSupportsFractionalFactor(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 2.5,
	}

	scheduled, err := pacer.ScheduledTime(
		pacer.Epoch.Add(10 * time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	expected := pacer.StartAt.Add(4 * time.Second)
	if !scheduled.Equal(expected) {
		t.Fatalf("scheduled=%s, atteso %s", scheduled, expected)
	}
}

func TestReplayPacerRejectsObservedAtBeforeEpoch(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 10,
	}

	_, err := pacer.ScheduledTime(
		pacer.Epoch.Add(-time.Nanosecond),
	)
	if err == nil || !strings.Contains(err.Error(), "precedente") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestReplayPacerIsIndependentFromPreviousEvents(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 100,
	}
	observedAt := pacer.Epoch.Add(10 * time.Minute)

	first, err := pacer.ScheduledTime(observedAt)
	if err != nil {
		t.Fatal(err)
	}

	_, err = pacer.ScheduledTime(
		pacer.Epoch.Add(2 * time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}

	second, err := pacer.ScheduledTime(observedAt)
	if err != nil {
		t.Fatal(err)
	}

	if !first.Equal(second) {
		t.Fatalf("deadline dipendente da eventi precedenti: %s != %s", first, second)
	}
}

func TestLocalReplayStartTranslatesConfiguredStart(t *testing.T) {
	now := mustTime("2026-08-28T19:59:50Z")
	configuredStart := now.Add(10 * time.Second)

	anchor := localReplayStart(now, configuredStart)
	if !anchor.Equal(configuredStart) {
		t.Fatalf("anchor=%s, atteso %s", anchor, configuredStart)
	}

	if delay := anchor.Sub(now); delay != 10*time.Second {
		t.Fatalf("ritardo anchor=%s, atteso 10s", delay)
	}
}

func TestReplayDoesNotUseFirstShardEventAsEpoch(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	token := newAwaitableToken(nil)

	stats, err := replaySite(
		replayReader(
			"101;BME280;1;45.0;9.0;2025-01-01T00:10:00Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       token,
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Events != 1 {
		t.Fatalf("eventi=%d, atteso 1", stats.Events)
	}

	if len(clock.sleeps) != 1 || clock.sleeps[0] != time.Minute {
		t.Fatalf("sleep=%v, atteso [1m0s]", clock.sleeps)
	}
}

func TestReplayPublishesEveryShardRowWithEventSemanticsUnchanged(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	tokens := []*fakeToken{
		newAwaitableToken(nil),
		newAwaitableToken(nil),
		newAwaitableToken(nil),
	}

	var topics []string
	var events []model.SensorEvent

	stats, err := replaySite(
		replayReader(
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"102;BME280;2;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:02Z;100002;22;52",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(topic string, event model.SensorEvent) (PublishResult, error) {
				topics = append(topics, topic)
				events = append(events, event)

				return PublishResult{
					Token:       tokens[len(events)-1],
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Events != 3 || len(events) != 3 {
		t.Fatalf("statistiche=%d, eventi catturati=%d", stats.Events, len(events))
	}

	expected := []struct {
		sensorID   string
		sequence   uint64
		eventID    string
		topic      string
		observedAt time.Time
	}{
		{"101", 1, "101-1", "sensors/101/telemetry", mustTime("2025-01-01T00:00:00Z")},
		{"102", 1, "102-1", "sensors/102/telemetry", mustTime("2025-01-01T00:00:01Z")},
		{"101", 2, "101-2", "sensors/101/telemetry", mustTime("2025-01-01T00:00:02Z")},
	}

	for index, want := range expected {
		actual := events[index]

		if actual.SensorID != want.sensorID ||
			actual.Sequence != want.sequence ||
			actual.EventID != want.eventID ||
			!actual.ObservedAt.Equal(want.observedAt) ||
			topics[index] != want.topic {
			t.Fatalf("evento %d inatteso: topic=%q event=%#v", index, topics[index], actual)
		}

		if actual.EmittedAt.IsZero() || actual.EmittedAt.Before(config.ReplayStartAt) {
			t.Fatalf("EmittedAt prematuro per evento %d: %s", index, actual.EmittedAt)
		}
	}

	if events[0].Measurements["temperature"] != "20" ||
		events[0].Measurements["humidity"] != "50" ||
		events[0].Measurements["pressure"] != "100000" {
		t.Fatalf("misure modificate: %#v", events[0].Measurements)
	}
}

func TestMaxEventsLimitsOnlyThisSimulatorInstance(t *testing.T) {
	config := validSimulatorConfig()
	config.MaxEvents = 2
	clock := newFakeClock(config.ReplayStartAt)
	publishCalls := 0
	endPublishCalls := 0

	stats, err := replaySite(
		replayReader(
			"301;BME280;3;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"302;BME280;3;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
			"303;BME280;3;45.0;9.0;2025-01-01T00:00:02Z;100002;22;52",
		),
		config,
		ReplayRuntime{
			Now:   clock.Now,
			Sleep: clock.Sleep,
			PublishEndOfReplay: func(_ string, _ model.EndOfReplay) (PublishResult, error) {
				endPublishCalls++
				return PublishResult{}, nil
			},
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				publishCalls++

				return PublishResult{
					Token:       newAwaitableToken(nil),
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Events != 2 || publishCalls != 2 || endPublishCalls != 0 || stats.ReachedEOF {
		t.Fatalf(
			"eventi=%d publish=%d end=%d reached_eof=%t",
			stats.Events,
			publishCalls,
			endPublishCalls,
			stats.ReachedEOF,
		)
	}
}

func TestReplayRejectsDecreasingTimestamp(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)

	_, err := replaySite(
		replayReader(
			"301;BME280;3;45.0;9.0;2025-01-01T00:00:02Z;100000;20;50",
			"302;BME280;3;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       newAwaitableToken(nil),
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "non ordinato") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestReplayMeasuresSchedulingLag(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	publishCalls := 0

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"402;BME280;4;45.0;9.0;2025-01-01T00:00:10Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				publishCalls++
				lag := time.Duration(publishCalls*200-100) * time.Millisecond
				clock.Advance(lag)

				return PublishResult{
					Token:       newCompletedToken(nil),
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.AverageSchedulingLag() != 200*time.Millisecond ||
		stats.SchedulingLagMax != 300*time.Millisecond {
		t.Fatalf(
			"lag medio=%s massimo=%s",
			stats.AverageSchedulingLag(),
			stats.SchedulingLagMax,
		)
	}
}

func TestReplayRejectsStartBeyondLateTolerance(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(
		config.ReplayStartAt.Add(5 * time.Second),
	)
	publishCalls := 0

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				publishCalls++

				return PublishResult{}, nil
			},
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "avviato troppo tardi") ||
		!strings.Contains(err.Error(), "edge-3") ||
		!strings.Contains(err.Error(), "scheduled_at=") ||
		!strings.Contains(err.Error(), "actual_at=") ||
		!strings.Contains(err.Error(), "lateness=5s") {
		t.Fatalf("errore inatteso: %v", err)
	}

	if publishCalls != 0 || stats.Events != 0 {
		t.Fatalf("publish=%d eventi=%d", publishCalls, stats.Events)
	}
}

func TestReplayAcceptsSmallInitialLateness(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(
		config.ReplayStartAt.Add(100 * time.Millisecond),
	)

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       newCompletedToken(nil),
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Events != 1 ||
		stats.AverageSchedulingLag() != 100*time.Millisecond {
		t.Fatalf(
			"eventi=%d lag=%s",
			stats.Events,
			stats.AverageSchedulingLag(),
		)
	}
}

func TestReplayDoesNotAbortForLatenessAfterFirstPublish(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	publishCalls := 0

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"402;BME280;4;45.0;9.0;2025-01-01T00:00:10Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				publishedAt := clock.Now()
				publishCalls++
				if publishCalls == 1 {
					clock.Advance(3 * time.Second)
				}

				return PublishResult{
					Token:       newCompletedToken(nil),
					PublishedAt: publishedAt,
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Events != 2 || stats.SchedulingLagMax != 2*time.Second {
		t.Fatalf(
			"eventi=%d lag massimo=%s",
			stats.Events,
			stats.SchedulingLagMax,
		)
	}
}

func TestReplayThroughputUsesOnlyPublishWindow(t *testing.T) {
	config := validSimulatorConfig()
	enteredAt := config.ReplayStartAt.Add(-10 * time.Second)
	clock := newFakeClock(enteredAt)

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"402;BME280;4;45.0;9.0;2025-01-01T00:00:20Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       newCompletedToken(nil),
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !stats.FirstPublishedAt.Equal(config.ReplayStartAt) ||
		!stats.LastPublishedAt.Equal(config.ReplayStartAt.Add(2*time.Second)) ||
		stats.PublishDuration() != 2*time.Second ||
		stats.Throughput() != 0.5 {
		t.Fatalf("statistiche publish inattese: %#v", stats)
	}

	if len(clock.sleeps) != 2 ||
		clock.sleeps[0] != 10*time.Second ||
		clock.sleeps[1] != 2*time.Second {
		t.Fatalf("sleep inattesi: %v", clock.sleeps)
	}
}

func TestReplayCompletedAtIsRecordedAfterDrain(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	token := newAwaitableToken(nil)
	token.onWait = func() {
		clock.Advance(2 * time.Second)
	}

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       token,
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if token.waitCalls != 1 ||
		!stats.FirstPublishedAt.Equal(config.ReplayStartAt) ||
		!stats.LastPublishedAt.Equal(config.ReplayStartAt) ||
		!stats.CompletedAt.Equal(config.ReplayStartAt.Add(2*time.Second)) ||
		stats.DrainDuration() != 2*time.Second ||
		stats.Throughput() != 0 {
		t.Fatalf("statistiche drain inattese: %#v", stats)
	}

	if stats.FirstPublishedAt.After(stats.LastPublishedAt) ||
		stats.LastPublishedAt.After(stats.CompletedAt) {
		t.Fatalf("ordine timestamp non valido: %#v", stats)
	}
}

func TestReplayStatsClampNegativeSchedulingLagToZero(t *testing.T) {
	stats := ReplayStats{}
	stats.RecordPublish(testPublishTime, -time.Second)

	if stats.Events != 1 ||
		stats.SchedulingLagTotal != 0 ||
		stats.SchedulingLagMax != 0 ||
		!stats.FirstPublishedAt.Equal(testPublishTime) ||
		!stats.LastPublishedAt.Equal(testPublishTime) ||
		stats.PublishDuration() != 0 ||
		stats.Throughput() != 0 {
		t.Fatalf("statistiche lag negative inattese: %#v", stats)
	}
}

func TestPendingPublishesBelowLimitDoesNotWait(t *testing.T) {
	pending := mustPendingPublishes(t, 3)
	token := newAwaitableToken(nil)

	if err := pending.Track(testPending("event-1", token)); err != nil {
		t.Fatal(err)
	}

	if token.waitCalls != 0 || pending.Len() != 1 {
		t.Fatalf("waitCalls=%d pending=%d", token.waitCalls, pending.Len())
	}
}

func TestPendingPublishesAppliesFIFOBackpressureAtLimit(t *testing.T) {
	pending := mustPendingPublishes(t, 3)
	tokens := []*fakeToken{
		newAwaitableToken(nil),
		newAwaitableToken(nil),
		newAwaitableToken(nil),
	}

	for index, publishToken := range tokens {
		if err := pending.Track(testPending(eventID(index), publishToken)); err != nil {
			t.Fatal(err)
		}
	}

	if tokens[0].waitCalls != 1 ||
		tokens[1].waitCalls != 0 ||
		tokens[2].waitCalls != 0 {
		t.Fatalf(
			"attese FIFO inattese: %d %d %d",
			tokens[0].waitCalls,
			tokens[1].waitCalls,
			tokens[2].waitCalls,
		)
	}

	if pending.Len() != 2 || pending.Peak() != 3 {
		t.Fatalf("pending=%d peak=%d", pending.Len(), pending.Peak())
	}
}

func TestPendingPublishesReapsCompletedToken(t *testing.T) {
	pending := mustPendingPublishes(t, 3)
	token := newCompletedToken(nil)

	if err := pending.Track(testPending("event-1", token)); err != nil {
		t.Fatal(err)
	}

	if pending.Len() != 0 || pending.Peak() != 1 || token.waitCalls != 0 {
		t.Fatalf(
			"pending=%d peak=%d waitCalls=%d",
			pending.Len(),
			pending.Peak(),
			token.waitCalls,
		)
	}
}

func TestPendingPublishesReapsOnlyCompletedPrefix(t *testing.T) {
	pending := mustPendingPublishes(t, 10)
	tokens := []*fakeToken{
		newAwaitableToken(nil),
		newAwaitableToken(nil),
		newAwaitableToken(nil),
		newAwaitableToken(nil),
	}

	for index, publishToken := range tokens {
		if err := pending.Track(testPending(eventID(index), publishToken)); err != nil {
			t.Fatal(err)
		}
	}

	tokens[0].complete()
	tokens[1].complete()
	tokens[3].complete()
	for _, publishToken := range tokens {
		publishToken.doneCalls = 0
	}

	if err := pending.reapCompletedPrefix(); err != nil {
		t.Fatal(err)
	}

	if pending.Len() != 2 || pending.oldest().EventID != "event-2" {
		t.Fatalf(
			"pending=%d oldest=%s",
			pending.Len(),
			pending.oldest().EventID,
		)
	}

	if tokens[0].doneCalls != 1 ||
		tokens[1].doneCalls != 1 ||
		tokens[2].doneCalls != 1 ||
		tokens[3].doneCalls != 0 {
		t.Fatalf(
			"consultazioni Done inattese: %d %d %d %d",
			tokens[0].doneCalls,
			tokens[1].doneCalls,
			tokens[2].doneCalls,
			tokens[3].doneCalls,
		)
	}

	if err := pending.waitOldest(); err != nil {
		t.Fatal(err)
	}
	if tokens[2].waitCalls != 1 || tokens[3].waitCalls != 0 {
		t.Fatalf(
			"ordine FIFO inatteso: C=%d D=%d",
			tokens[2].waitCalls,
			tokens[3].waitCalls,
		)
	}

	if err := pending.reapCompletedPrefix(); err != nil {
		t.Fatal(err)
	}
	if pending.Len() != 0 {
		t.Fatalf("pending finali=%d", pending.Len())
	}
}

func TestPendingPublishesPrefixReapStopsAtFirstPendingToken(t *testing.T) {
	pending := mustPendingPublishes(t, 4)
	first := newAwaitableToken(nil)
	second := newCompletedToken(nil)

	if err := pending.Track(testPending("event-1", first)); err != nil {
		t.Fatal(err)
	}
	if err := pending.Track(testPending("event-2", second)); err != nil {
		t.Fatal(err)
	}
	first.doneCalls = 0
	second.doneCalls = 0

	if err := pending.reapCompletedPrefix(); err != nil {
		t.Fatal(err)
	}

	if first.doneCalls != 1 || second.doneCalls != 0 || pending.Len() != 2 {
		t.Fatalf(
			"Done first=%d second=%d pending=%d",
			first.doneCalls,
			second.doneCalls,
			pending.Len(),
		)
	}
}

func TestPendingPublishesPropagatesCompletedTokenError(t *testing.T) {
	pending := mustPendingPublishes(t, 3)
	token := newCompletedToken(errors.New("puback fallito"))

	err := pending.Track(testPending("event-1", token))
	if err == nil || !strings.Contains(err.Error(), "puback fallito") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestPendingPublishesPropagatesTimeout(t *testing.T) {
	pending := mustPendingPublishes(t, 1)
	token := newTimeoutToken()

	err := pending.Track(testPending("event-1", token))
	if err == nil || !strings.Contains(err.Error(), "timeout PUBACK") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestPendingPublishesDoesNotSleepPastPublishAckDeadline(t *testing.T) {
	clock := newFakeClock(testPublishTime)
	pending, err := newPendingPublishes(
		3,
		time.Second,
		clock.Now,
	)
	if err != nil {
		t.Fatal(err)
	}

	token := newTimeoutToken()
	if err := pending.Track(testPending("event-1", token)); err != nil {
		t.Fatal(err)
	}

	err = pending.WaitUntil(
		testPublishTime.Add(time.Minute),
		clock.Sleep,
	)
	if err == nil || !strings.Contains(err.Error(), "timeout PUBACK") {
		t.Fatalf("errore inatteso: %v", err)
	}

	if token.waitCalls != 1 || len(clock.sleeps) != 0 {
		t.Fatalf(
			"waitCalls=%d sleep=%v",
			token.waitCalls,
			clock.sleeps,
		)
	}
}

func TestPendingPublishesDrainWaitsAllTokens(t *testing.T) {
	pending := mustPendingPublishes(t, 10)
	tokens := []*fakeToken{
		newAwaitableToken(errors.New("primo fallito")),
		newAwaitableToken(nil),
		newAwaitableToken(nil),
	}

	for index, publishToken := range tokens {
		if err := pending.Track(testPending(eventID(index), publishToken)); err != nil {
			t.Fatal(err)
		}
	}

	err := pending.Drain()
	if err == nil || !strings.Contains(err.Error(), "primo fallito") {
		t.Fatalf("errore inatteso: %v", err)
	}

	for index, publishToken := range tokens {
		if publishToken.waitCalls != 1 {
			t.Fatalf("token %d atteso %d volte", index, publishToken.waitCalls)
		}
	}

	if pending.Len() != 0 {
		t.Fatalf("pending dopo drain=%d", pending.Len())
	}
}

func TestReplayPublishesAsynchronouslyUntilInFlightLimit(t *testing.T) {
	config := validSimulatorConfig()
	config.MQTTMaxInFlight = 3
	clock := newFakeClock(config.ReplayStartAt)
	publishCalls := 0
	tokens := []*fakeToken{
		newAwaitableToken(nil),
		newAwaitableToken(nil),
		newAwaitableToken(nil),
		newAwaitableToken(nil),
	}
	tokens[0].onWait = func() {
		if publishCalls != 3 {
			t.Fatalf(
				"primo PUBACK atteso dopo %d publish, attesi 3",
				publishCalls,
			)
		}
	}

	stats, err := replaySite(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"502;BME280;5;45.0;9.0;2025-01-01T00:00:01Z;100000;20;50",
			"503;BME280;5;45.0;9.0;2025-01-01T00:00:02Z;100000;20;50",
			"504;BME280;5;45.0;9.0;2025-01-01T00:00:03Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:                clock.Now,
			Sleep:              clock.Sleep,
			PublishEndOfReplay: completedEndPublisher(clock),
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				result := PublishResult{
					Token:       tokens[publishCalls],
					PublishedAt: clock.Now(),
				}
				publishCalls++

				return result, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Events != 4 || stats.PeakInFlight != 3 {
		t.Fatalf("eventi=%d peak=%d", stats.Events, stats.PeakInFlight)
	}

	for index, publishToken := range tokens {
		if publishToken.waitCalls != 1 {
			t.Fatalf("token %d non drenato: waitCalls=%d", index, publishToken.waitCalls)
		}
	}
}

func TestReplayPublishesEndOfReplayOnlyAfterTelemetryDrain(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	telemetryTokens := []*fakeToken{
		newAwaitableToken(nil),
		newAwaitableToken(nil),
	}
	endToken := newAwaitableToken(nil)
	order := make([]string, 0, 5)
	telemetryTokens[0].onWait = func() { order = append(order, "ack-a") }
	telemetryTokens[1].onWait = func() { order = append(order, "ack-b") }
	endToken.onWait = func() { order = append(order, "ack-end") }
	var endTopic string
	var endRecord model.EndOfReplay
	publishCalls := 0

	stats, err := replaySite(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"502;BME280;5;45.0;9.0;2025-01-01T00:00:01Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:   clock.Now,
			Sleep: clock.Sleep,
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				order = append(order, "telemetry-"+strconv.Itoa(publishCalls))
				result := PublishResult{
					Token:       telemetryTokens[publishCalls],
					PublishedAt: clock.Now(),
				}
				publishCalls++

				return result, nil
			},
			PublishEndOfReplay: func(
				topic string,
				record model.EndOfReplay,
			) (PublishResult, error) {
				if telemetryTokens[0].waitCalls != 1 ||
					telemetryTokens[1].waitCalls != 1 {
					t.Fatal("EndOfReplay pubblicato prima del drain telemetry")
				}

				order = append(order, "publish-end")
				endTopic = topic
				endRecord = record

				return PublishResult{
					Token:       endToken,
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedOrder := []string{
		"telemetry-0",
		"telemetry-1",
		"ack-a",
		"ack-b",
		"publish-end",
		"ack-end",
	}
	if len(order) != len(expectedOrder) {
		t.Fatalf("ordine=%v, atteso %v", order, expectedOrder)
	}
	for index := range expectedOrder {
		if order[index] != expectedOrder[index] {
			t.Fatalf("ordine=%v, atteso %v", order, expectedOrder)
		}
	}

	lastObservedAt := mustTime("2025-01-01T00:00:01Z")
	if !stats.ReachedEOF ||
		!stats.LastObservedAt.Equal(lastObservedAt) ||
		endTopic != "replay/edge-3/end" ||
		endRecord.SchemaVersion != model.EndOfReplaySchemaVersion ||
		endRecord.EdgeID != "edge-3" ||
		!endRecord.LastObservedAt.Equal(lastObservedAt) ||
		endRecord.EmittedAt.IsZero() ||
		endToken.waitCalls != 1 {
		t.Fatalf(
			"stats=%#v topic=%q record=%#v end_wait=%d",
			stats,
			endTopic,
			endRecord,
			endToken.waitCalls,
		)
	}
}

func TestReplayDoesNotPublishEndOfReplayWhenTelemetryDrainFails(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	endPublishCalls := 0

	_, err := replaySite(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:   clock.Now,
			Sleep: clock.Sleep,
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       newAwaitableToken(errors.New("telemetry PUBACK fallito")),
					PublishedAt: clock.Now(),
				}, nil
			},
			PublishEndOfReplay: func(_ string, _ model.EndOfReplay) (PublishResult, error) {
				endPublishCalls++
				return PublishResult{}, nil
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "telemetry PUBACK fallito") {
		t.Fatalf("errore inatteso: %v", err)
	}

	if endPublishCalls != 0 {
		t.Fatalf("EndOfReplay pubblicato %d volte", endPublishCalls)
	}
}

func TestReplayFailsWhenEndOfReplayPublishFails(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)

	_, err := replaySite(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:   clock.Now,
			Sleep: clock.Sleep,
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       newCompletedToken(nil),
					PublishedAt: clock.Now(),
				}, nil
			},
			PublishEndOfReplay: func(_ string, _ model.EndOfReplay) (PublishResult, error) {
				return PublishResult{}, errors.New("publish end fallito")
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "publish end fallito") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestReplayFailsWhenEndOfReplayAckTimesOut(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	endToken := newTimeoutToken()

	_, err := replaySite(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		ReplayRuntime{
			Now:   clock.Now,
			Sleep: clock.Sleep,
			Publish: func(_ string, _ model.SensorEvent) (PublishResult, error) {
				return PublishResult{
					Token:       newCompletedToken(nil),
					PublishedAt: clock.Now(),
				}, nil
			},
			PublishEndOfReplay: func(_ string, _ model.EndOfReplay) (PublishResult, error) {
				return PublishResult{
					Token:       endToken,
					PublishedAt: clock.Now(),
				}, nil
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "timeout PUBACK") {
		t.Fatalf("errore inatteso: %v", err)
	}

	if endToken.waitCalls != 1 {
		t.Fatalf("token EndOfReplay atteso %d volte", endToken.waitCalls)
	}
}

func TestReplayEndTopic(t *testing.T) {
	if topic := replayEndTopic("edge-3"); topic != "replay/edge-3/end" {
		t.Fatalf("topic=%q", topic)
	}
}

func TestPublishSensorEventUsesQoSOneWithoutWaiting(t *testing.T) {
	fixedNow := mustTime("2026-08-28T20:00:01Z")
	token := newCompletedToken(nil)
	var capturedTopic string
	var capturedQoS byte
	var capturedRetained bool
	var capturedPayload []byte

	event := model.SensorEvent{
		SchemaVersion: 1,
		EventID:       "101-1",
		SensorID:      "101",
		ObservedAt:    mustTime("2025-01-01T00:00:00Z"),
		EmittedAt:     fixedNow,
	}

	result, err := publishSensorEvent(
		func(
			topic string,
			qos byte,
			retained bool,
			payload interface{},
		) mqtt.Token {
			capturedTopic = topic
			capturedQoS = qos
			capturedRetained = retained
			capturedPayload = append([]byte(nil), payload.([]byte)...)

			return token
		},
		"sensors/101/telemetry",
		event,
		func() time.Time { return fixedNow },
	)
	if err != nil {
		t.Fatal(err)
	}

	if capturedTopic != "sensors/101/telemetry" ||
		capturedQoS != 1 ||
		capturedRetained {
		t.Fatalf(
			"publish MQTT inatteso: topic=%q qos=%d retained=%t",
			capturedTopic,
			capturedQoS,
			capturedRetained,
		)
	}

	if token.waitCalls != 0 {
		t.Fatalf("publishSensorEvent ha atteso il token %d volte", token.waitCalls)
	}

	if result.Token != token || !result.PublishedAt.Equal(fixedNow) {
		t.Fatalf("risultato publish inatteso: %#v", result)
	}

	var decoded model.SensorEvent
	if err := json.Unmarshal(capturedPayload, &decoded); err != nil {
		t.Fatalf("payload non valido: %v", err)
	}

	if decoded.EventID != event.EventID ||
		!decoded.ObservedAt.Equal(event.ObservedAt) ||
		!decoded.EmittedAt.Equal(event.EmittedAt) {
		t.Fatalf("evento serializzato modificato: %#v", decoded)
	}
}

func TestPublishEndOfReplayUsesQoSOneWithoutRetain(t *testing.T) {
	fixedNow := mustTime("2026-08-28T20:00:01Z")
	token := newCompletedToken(nil)
	var capturedTopic string
	var capturedQoS byte
	var capturedRetained bool
	var capturedPayload []byte
	record := model.EndOfReplay{
		SchemaVersion:  model.EndOfReplaySchemaVersion,
		EdgeID:         "edge-3",
		LastObservedAt: mustTime("2025-01-01T00:00:00Z"),
		EmittedAt:      fixedNow,
	}

	result, err := publishEndOfReplay(
		func(
			topic string,
			qos byte,
			retained bool,
			payload interface{},
		) mqtt.Token {
			capturedTopic = topic
			capturedQoS = qos
			capturedRetained = retained
			capturedPayload = append([]byte(nil), payload.([]byte)...)

			return token
		},
		"replay/edge-3/end",
		record,
		func() time.Time { return fixedNow },
	)
	if err != nil {
		t.Fatal(err)
	}

	if capturedTopic != "replay/edge-3/end" ||
		capturedQoS != 1 ||
		capturedRetained ||
		result.Token != token ||
		!result.PublishedAt.Equal(fixedNow) ||
		token.waitCalls != 0 {
		t.Fatalf(
			"topic=%q qos=%d retained=%t result=%#v waits=%d",
			capturedTopic,
			capturedQoS,
			capturedRetained,
			result,
			token.waitCalls,
		)
	}

	var decoded model.EndOfReplay
	if err := json.Unmarshal(capturedPayload, &decoded); err != nil {
		t.Fatalf("payload EndOfReplay non valido: %v", err)
	}
	if decoded != record {
		t.Fatalf("record=%#v, atteso %#v", decoded, record)
	}
}

func TestLoadSimulatorConfig(t *testing.T) {
	config, err := loadSimulatorConfig(
		envFrom(validSimulatorEnv()),
	)
	if err != nil {
		t.Fatal(err)
	}

	if config.SiteID != "edge-3" ||
		config.MQTTEndpoint != "tcp://mqtt-edge-3:1883" ||
		config.ReplayFile != "/app/dataset/derived/replay_by_edge/edge-3.csv" ||
		config.MaxEvents != 25 ||
		!config.ReplayEpoch.Equal(mustTime("2025-01-01T00:00:00Z")) ||
		!config.ReplayStartAt.Equal(mustTime("2026-08-28T20:00:10Z")) ||
		config.AccelerationFactor != 2.5 ||
		config.MQTTMaxInFlight != 123 {
		t.Fatalf("configurazione inattesa: %#v", config)
	}
}

func TestLoadSimulatorConfigRequiresRuntimeValues(t *testing.T) {
	for _, missing := range []string{
		"SITE_ID",
		"MQTT_ENDPOINT",
		"REPLAY_FILE",
		"REPLAY_START_AT",
	} {
		t.Run(
			missing,
			func(t *testing.T) {
				values := validSimulatorEnv()
				delete(values, missing)

				_, err := loadSimulatorConfig(envFrom(values))
				if err == nil || !strings.Contains(err.Error(), missing) {
					t.Fatalf("errore inatteso: %v", err)
				}
			},
		)
	}
}

func TestLoadSimulatorConfigRejectsInvalidAccelerationFactor(t *testing.T) {
	for _, value := range []string{"0", "-1", "invalid", "NaN", "+Inf"} {
		t.Run(
			value,
			func(t *testing.T) {
				values := validSimulatorEnv()
				values["ACCELERATION_FACTOR"] = value

				_, err := loadSimulatorConfig(envFrom(values))
				if err == nil || !strings.Contains(err.Error(), "ACCELERATION_FACTOR") {
					t.Fatalf("errore inatteso: %v", err)
				}
			},
		)
	}
}

func TestLoadSimulatorConfigRejectsInvalidReplayTimes(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"REPLAY_EPOCH", "not-a-time"},
		{"REPLAY_EPOCH", "2025-01-01T01:00:00+01:00"},
		{"REPLAY_START_AT", "not-a-time"},
		{"REPLAY_START_AT", "2026-08-28T22:00:10+02:00"},
	}

	for _, test := range tests {
		t.Run(
			test.name+"_"+test.value,
			func(t *testing.T) {
				values := validSimulatorEnv()
				values[test.name] = test.value

				_, err := loadSimulatorConfig(envFrom(values))
				if err == nil || !strings.Contains(err.Error(), test.name) {
					t.Fatalf("errore inatteso: %v", err)
				}
			},
		)
	}
}

func TestLoadSimulatorConfigRejectsInvalidMaxInFlight(t *testing.T) {
	for _, value := range []string{"0", "-1", "invalid"} {
		t.Run(
			value,
			func(t *testing.T) {
				values := validSimulatorEnv()
				values["MQTT_MAX_IN_FLIGHT"] = value

				_, err := loadSimulatorConfig(envFrom(values))
				if err == nil || !strings.Contains(err.Error(), "MQTT_MAX_IN_FLIGHT") {
					t.Fatalf("errore inatteso: %v", err)
				}
			},
		)
	}
}

func TestLoadSimulatorConfigUsesRuntimeDefaults(t *testing.T) {
	values := validSimulatorEnv()
	delete(values, "REPLAY_EPOCH")
	delete(values, "ACCELERATION_FACTOR")
	delete(values, "MQTT_MAX_IN_FLIGHT")

	config, err := loadSimulatorConfig(envFrom(values))
	if err != nil {
		t.Fatal(err)
	}

	if !config.ReplayEpoch.Equal(mustTime(defaultReplayEpoch)) ||
		config.AccelerationFactor != defaultAccelerationFactor ||
		config.MQTTMaxInFlight != defaultMQTTMaxInFlight {
		t.Fatalf("default inattesi: %#v", config)
	}
}

func TestLoadSimulatorConfigPreservesMaxEventsValidation(t *testing.T) {
	values := validSimulatorEnv()
	values["MAX_EVENTS"] = "-1"

	_, err := loadSimulatorConfig(envFrom(values))
	if err == nil || !strings.Contains(err.Error(), "MAX_EVENTS") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestTelemetryTopic(t *testing.T) {
	if got := telemetryTopic("87575"); got != "sensors/87575/telemetry" {
		t.Fatalf("topic=%q", got)
	}
}

func TestOpenReplayFileReportsMissingFile(t *testing.T) {
	missingPath := t.TempDir() + "/missing.csv"

	_, err := openReplayFile(missingPath)
	if err == nil || !strings.Contains(err.Error(), "REPLAY_FILE") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestSimulatorHasNoRuntimeTopologyDependency(t *testing.T) {
	file := parseSimulatorSource(t)
	forbiddenIdentifiers := map[string]struct{}{
		"SensorAssignment": {},
		"belongsToSite":    {},
		"brokerAddress":    {},
		"getMQTTClient":    {},
	}
	forbiddenStrings := []string{
		"kmeans_topology.csv",
		"macroarea_id",
	}

	ast.Inspect(
		file,
		func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.Ident:
				if _, forbidden := forbiddenIdentifiers[value.Name]; forbidden {
					t.Fatalf("dipendenza topologica runtime presente: %s", value.Name)
				}

			case *ast.BasicLit:
				if value.Kind != token.STRING {
					return true
				}

				literal, err := strconv.Unquote(value.Value)
				if err != nil {
					t.Fatalf("stringa Go non valida %s: %v", value.Value, err)
				}

				for _, forbidden := range forbiddenStrings {
					if strings.Contains(literal, forbidden) {
						t.Fatalf("riferimento runtime vietato: %s", forbidden)
					}
				}
			}

			return true
		},
	)
}

func TestSimulatorCreatesExactlyOneMQTTClient(t *testing.T) {
	file := parseSimulatorSource(t)
	newClientCalls := 0

	ast.Inspect(
		file,
		func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}

			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "NewClient" {
				newClientCalls++
			}

			return true
		},
	)

	if newClientCalls != 1 {
		t.Fatalf("chiamate mqtt.NewClient=%d, attesa 1", newClientCalls)
	}
}

type fakeClock struct {
	now    time.Time
	sleeps []time.Duration
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (
	clock *fakeClock,
) Now() time.Time {
	return clock.now
}

func (
	clock *fakeClock,
) Sleep(duration time.Duration) {
	clock.sleeps = append(clock.sleeps, duration)
	clock.Advance(duration)
}

func (
	clock *fakeClock,
) Advance(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}

type fakeToken struct {
	done       chan struct{}
	completed  bool
	waitResult bool
	err        error
	doneCalls  int
	waitCalls  int
	onWait     func()
}

func newAwaitableToken(err error) *fakeToken {
	return &fakeToken{
		done:       make(chan struct{}),
		waitResult: true,
		err:        err,
	}
}

func newTimeoutToken() *fakeToken {
	return &fakeToken{
		done:       make(chan struct{}),
		waitResult: false,
	}
}

func newCompletedToken(err error) *fakeToken {
	token := newAwaitableToken(err)
	token.complete()

	return token
}

func (
	token *fakeToken,
) complete() {
	if token.completed {
		return
	}

	token.completed = true
	close(token.done)
}

func (
	token *fakeToken,
) Wait() bool {
	<-token.done

	return true
}

func (
	token *fakeToken,
) WaitTimeout(_ time.Duration) bool {
	token.waitCalls++
	if token.onWait != nil {
		token.onWait()
	}

	if !token.waitResult {
		return false
	}

	token.complete()

	return true
}

func (
	token *fakeToken,
) Done() <-chan struct{} {
	token.doneCalls++
	return token.done
}

func (
	token *fakeToken,
) Error() error {
	return token.err
}

func mustPendingPublishes(
	t *testing.T,
	limit int,
) *PendingPublishes {
	t.Helper()

	pending, err := newPendingPublishes(
		limit,
		time.Second,
		func() time.Time { return testPublishTime },
	)
	if err != nil {
		t.Fatal(err)
	}

	return pending
}

func testPending(
	eventID string,
	token PublishToken,
) PendingPublish {
	return PendingPublish{
		EventID:     eventID,
		Topic:       "sensors/101/telemetry",
		PublishedAt: testPublishTime,
		Token:       token,
	}
}

func completedEndPublisher(
	clock *fakeClock,
) EndOfReplayPublisher {
	return func(
		_ string,
		_ model.EndOfReplay,
	) (PublishResult, error) {
		return PublishResult{
			Token:       newCompletedToken(nil),
			PublishedAt: clock.Now(),
		}, nil
	}
}

var testPublishTime = mustTime("2026-08-28T20:00:00Z")

func validSimulatorConfig() SimulatorConfig {
	return SimulatorConfig{
		SiteID:             "edge-3",
		ReplayEpoch:        mustTime("2025-01-01T00:00:00Z"),
		ReplayStartAt:      mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 10,
		MQTTMaxInFlight:    10,
	}
}

func validSimulatorEnv() map[string]string {
	return map[string]string{
		"SITE_ID":             "edge-3",
		"MQTT_ENDPOINT":       "tcp://mqtt-edge-3:1883",
		"REPLAY_FILE":         "/app/dataset/derived/replay_by_edge/edge-3.csv",
		"MAX_EVENTS":          "25",
		"REPLAY_EPOCH":        "2025-01-01T00:00:00Z",
		"REPLAY_START_AT":     "2026-08-28T20:00:10Z",
		"ACCELERATION_FACTOR": "2.5",
		"MQTT_MAX_IN_FLIGHT":  "123",
	}
}

func replayReader(rows ...string) *csv.Reader {
	const header = "sensor_id;sensor_type;location;lat;lon;timestamp;pressure;temperature;humidity"

	reader := csv.NewReader(
		strings.NewReader(
			header + "\n" + strings.Join(rows, "\n") + "\n",
		),
	)
	reader.Comma = ';'

	return reader
}

func envFrom(values map[string]string) func(string) string {
	return func(name string) string {
		return values[name]
	}
}

func mustTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}

	return parsed.UTC()
}

func eventID(index int) string {
	return "event-" + strconv.Itoa(index)
}

func parseSimulatorSource(t *testing.T) *ast.File {
	t.Helper()

	file, err := parser.ParseFile(
		token.NewFileSet(),
		"main.go",
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("lettura main.go fallita: %v", err)
	}

	return file
}
