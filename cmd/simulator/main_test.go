package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"continuum/internal/model"
	"continuum/internal/mqtttopic"

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
	if want := mustTime("2026-08-28T20:01:00Z"); !scheduled.Equal(want) {
		t.Fatalf("scheduled=%s, atteso %s", scheduled, want)
	}
}

func TestReplayPacerPreservesFactorOneAndSupportsFractionalFactor(t *testing.T) {
	epoch := mustTime("2025-01-01T00:00:00Z")
	start := mustTime("2026-08-28T20:00:00Z")

	for _, test := range []struct {
		name   string
		factor float64
		offset time.Duration
		want   time.Duration
	}{
		{"factor_one", 1, 10 * time.Second, 10 * time.Second},
		{"fractional", 2.5, 10 * time.Second, 4 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			pacer := ReplayPacer{
				Epoch:              epoch,
				StartAt:            start,
				AccelerationFactor: test.factor,
			}
			got, err := pacer.ScheduledTime(epoch.Add(test.offset))
			if err != nil {
				t.Fatal(err)
			}
			if want := start.Add(test.want); !got.Equal(want) {
				t.Fatalf("scheduled=%s, atteso %s", got, want)
			}
		})
	}
}

func TestReplayPacerRejectsEventTimeBeforeEpoch(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 10,
	}

	_, err := pacer.ScheduledTime(pacer.Epoch.Add(-time.Nanosecond))
	if err == nil || !strings.Contains(err.Error(), "precedente") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestReplayPacerDoesNotUseFirstShardEventAsEpoch(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 100,
	}
	eventTime := pacer.Epoch.Add(10 * time.Minute)

	first, err := pacer.ScheduledTime(eventTime)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = pacer.ScheduledTime(pacer.Epoch.Add(2 * time.Hour))
	second, err := pacer.ScheduledTime(eventTime)
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

	if !anchor.Equal(configuredStart) || anchor.Sub(now) != 10*time.Second {
		t.Fatalf("anchor=%s, atteso %s", anchor, configuredStart)
	}
}

func TestParseNullableMeasurement(t *testing.T) {
	for _, test := range []struct {
		name      string
		input     string
		wantValue float64
		wantValid bool
		wantError bool
	}{
		{name: "number", input: "20.5", wantValue: 20.5, wantValid: true},
		{name: "empty", input: "", wantValid: false},
		{name: "spaces", input: "   ", wantValid: false},
		{name: "null", input: "null", wantValid: false},
		{name: "uppercase_null", input: "NULL", wantValid: false},
		{name: "malformed", input: "abc", wantError: true},
		{name: "nan", input: "NaN", wantError: true},
		{name: "inf", input: "Inf", wantError: true},
		{name: "positive_inf", input: "+Inf", wantError: true},
		{name: "negative_inf", input: "-Inf", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseNullableMeasurement(test.input)
			if test.wantError {
				if err == nil {
					t.Fatalf("input %q accettato: %#v", test.input, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Valid != test.wantValid || got.Value != test.wantValue {
				t.Fatalf("input=%q valore=%#v", test.input, got)
			}
		})
	}
}

func TestBuildSensorEventSerializesNullableNumericMeasurements(t *testing.T) {
	measurement := testMeasurement("101", 0)
	measurement.Pressure = "101200.0"
	measurement.Temperature = ""
	measurement.Humidity = "65.0"

	event, err := buildSensorEvent(measurement, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	var decoded struct {
		Measurements map[string]any `json:"measurements"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Measurements) != 3 ||
		decoded.Measurements["pressure"] != float64(101200) ||
		decoded.Measurements["temperature"] != nil ||
		decoded.Measurements["humidity"] != float64(65) {
		t.Fatalf("measurements JSON inattese: %s", payload)
	}
}

func TestReplayPreservesEventSemanticsAndOrder(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	var topics []string
	var events []model.SensorEvent

	stats, err := replaySite(
		replayReader(
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"102;BME280;2;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:02Z;100002;22;52",
		),
		config,
		testReplayRuntime(clock, func(topic string, event model.SensorEvent) error {
			topics = append(topics, topic)
			events = append(events, event)
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.OfferedEvents != 3 ||
		stats.TelemetryEnqueued != 3 ||
		stats.TelemetryLocallyDropped != 0 ||
		stats.MQTTPublishAttempts != 3 ||
		len(events) != 3 {
		t.Fatalf("statistiche=%#v eventi=%d", stats, len(events))
	}

	wants := []struct {
		sensorID string
		sequence uint64
		eventID  string
		topic    string
	}{
		{"101", 1, "101-1", "sensors/101/telemetry"},
		{"102", 1, "102-1", "sensors/102/telemetry"},
		{"101", 2, "101-2", "sensors/101/telemetry"},
	}
	for index, want := range wants {
		got := events[index]
		if got.SensorID != want.sensorID ||
			got.Sequence != want.sequence ||
			got.EventID != want.eventID ||
			topics[index] != want.topic ||
			got.EmittedAt.IsZero() {
			t.Fatalf("evento %d inatteso: topic=%q event=%#v", index, topics[index], got)
		}
	}
	if temperature := events[0].Measurements["temperature"]; !temperature.Valid || temperature.Value != 20 {
		t.Fatalf("temperatura modificata: %#v", temperature)
	}
	if humidity := events[0].Measurements["humidity"]; !humidity.Valid || humidity.Value != 50 {
		t.Fatalf("umidita modificata: %#v", humidity)
	}
	if pressure := events[0].Measurements["pressure"]; !pressure.Valid || pressure.Value != 100000 {
		t.Fatalf("misure modificate: %#v", events[0].Measurements)
	}
}

func TestReplayRejectsMalformedMeasurement(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)

	stats, err := replaySite(
		replayReader(
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:00Z;100000;abc;50",
		),
		config,
		testReplayRuntime(clock, func(string, model.SensorEvent) error { return nil }),
	)
	if err == nil || !strings.Contains(err.Error(), "temperature") {
		t.Fatalf("errore inatteso: %v", err)
	}
	if stats.OfferedEvents != 0 || stats.MQTTPublishAttempts != 0 || stats.EOSSuccesses != 0 {
		t.Fatalf("statistiche inattese: %#v", stats)
	}
}

func TestMaxEventsTruncatesWithoutEndOfReplay(t *testing.T) {
	config := validSimulatorConfig()
	config.MaxEvents = 2
	clock := newFakeClock(config.ReplayStartAt)
	eosCalls := 0
	runtime := testReplayRuntime(clock, func(string, model.SensorEvent) error {
		return nil
	})
	runtime.PublishEndOfReplay = func(string) (PublishResult, error) {
		eosCalls++
		return PublishResult{}, nil
	}

	stats, err := replaySite(
		replayReader(
			"301;BME280;3;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"302;BME280;3;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
			"303;BME280;3;45.0;9.0;2025-01-01T00:00:02Z;100002;22;52",
		),
		config,
		runtime,
	)
	if err != nil {
		t.Fatal(err)
	}
	if stats.OfferedEvents != 2 || stats.MQTTPublishAttempts != 2 ||
		stats.ReachedEOF || eosCalls != 0 {
		t.Fatalf("stats=%#v eos_calls=%d", stats, eosCalls)
	}
}

func TestReplayRejectsDecreasingEventTime(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)

	_, err := replaySite(
		replayReader(
			"301;BME280;3;45.0;9.0;2025-01-01T00:00:02Z;100000;20;50",
			"302;BME280;3;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
		),
		config,
		testReplayRuntime(clock, func(string, model.SensorEvent) error { return nil }),
	)
	if err == nil || !strings.Contains(err.Error(), "non ordinato") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestReplayRejectsLateStartButNotLaterLateness(t *testing.T) {
	config := validSimulatorConfig()
	lateClock := newFakeClock(config.ReplayStartAt.Add(15 * time.Second))

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		testReplayRuntime(lateClock, func(string, model.SensorEvent) error { return nil }),
	)
	if err == nil || !strings.Contains(err.Error(), "avviato troppo tardi") ||
		stats.OfferedEvents != 0 {
		t.Fatalf("stats=%#v err=%v", stats, err)
	}

	clock := newFakeClock(config.ReplayStartAt)
	runtime := testReplayRuntime(clock, func(string, model.SensorEvent) error { return nil })
	runtime.Sleep = func(duration time.Duration) {
		clock.Advance(duration + 2*time.Second)
	}
	stats, err = replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"402;BME280;4;45.0;9.0;2025-01-01T00:00:10Z;100000;20;50",
		),
		config,
		runtime,
	)
	if err != nil {
		t.Fatal(err)
	}
	if stats.OfferedEvents != 2 || stats.SchedulingLagMax != 2*time.Second {
		t.Fatalf("stats=%#v", stats)
	}
}

func TestReplayThroughputUsesOnlyOfferWindow(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt.Add(-10 * time.Second))

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"402;BME280;4;45.0;9.0;2025-01-01T00:00:20Z;100000;20;50",
		),
		config,
		testReplayRuntime(clock, func(string, model.SensorEvent) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.FirstOfferedAt.Equal(config.ReplayStartAt) ||
		!stats.LastOfferedAt.Equal(config.ReplayStartAt.Add(2*time.Second)) ||
		stats.OfferDuration() != 2*time.Second ||
		stats.Throughput() != 0.5 {
		t.Fatalf("statistiche offer inattese: %#v", stats)
	}
}

func TestReplayStatsClampNegativeSchedulingLag(t *testing.T) {
	stats := ReplayStats{}
	stats.RecordOffer(testPublishTime, -time.Second)

	if stats.OfferedEvents != 1 ||
		stats.SchedulingLagTotal != 0 ||
		stats.SchedulingLagMax != 0 ||
		!stats.FirstOfferedAt.Equal(testPublishTime) ||
		!stats.LastOfferedAt.Equal(testPublishTime) ||
		stats.OfferDuration() != 0 ||
		stats.Throughput() != 0 {
		t.Fatalf("statistiche inattese: %#v", stats)
	}
}

func TestNewTelemetryEgressRejectsNonPositiveCapacity(t *testing.T) {
	for _, test := range []struct {
		name     string
		capacity int
	}{
		{name: "zero", capacity: 0},
		{name: "negative", capacity: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newTelemetryEgress(
				test.capacity,
				func(string, model.SensorEvent) error { return nil },
				time.Now,
			)
			if err == nil || !strings.Contains(err.Error(), "TELEMETRY_QUEUE_CAPACITY") {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}
}

func TestTelemetryEgressDropsWithoutBlockingWhenQueueIsFull(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	egress, err := newTelemetryEgress(
		1,
		func(_ string, _ model.SensorEvent) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !egress.TryEnqueue(testSensorEvent("1", 0, 1)) {
		t.Fatal("prima telemetry non accettata")
	}
	<-started
	if !egress.TryEnqueue(testSensorEvent("1", 1, 2)) {
		t.Fatal("seconda telemetry non accettata nella coda libera")
	}

	enqueueResult := make(chan bool, 1)
	go func() {
		enqueueResult <- egress.TryEnqueue(testSensorEvent("1", 2, 3))
	}()
	select {
	case accepted := <-enqueueResult:
		if accepted {
			t.Fatal("telemetry accettata nonostante la coda piena")
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("TryEnqueue bloccata sulla coda piena")
	}

	close(release)
	stats := egress.CloseAndWait()
	if stats.QueueCapacity != 1 ||
		stats.CurrentQueueDepth != 0 ||
		stats.PublishAttempts != 2 ||
		stats.PublishErrors != 0 {
		t.Fatalf("egress stats=%#v", stats)
	}
}

func TestTelemetryEgressTracksCurrentQueueDepth(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	egress, err := newTelemetryEgress(
		2,
		func(_ string, _ model.SensorEvent) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}

	initial := egress.Stats()
	if initial.QueueCapacity != 2 ||
		initial.CurrentQueueDepth != 0 {
		t.Fatalf("stats iniziali=%#v", initial)
	}

	if !egress.TryEnqueue(testSensorEvent("1", 0, 1)) {
		t.Fatal("prima telemetry non accettata")
	}
	<-started
	if current := egress.Stats().CurrentQueueDepth; current != 0 {
		t.Fatalf("depth dopo dequeue=%d, attesa 0", current)
	}
	if !egress.TryEnqueue(testSensorEvent("1", 1, 2)) ||
		!egress.TryEnqueue(testSensorEvent("1", 2, 3)) {
		t.Fatal("queue non riempita fino alla capacity")
	}
	if egress.TryEnqueue(testSensorEvent("1", 3, 4)) {
		t.Fatal("enqueue su queue piena non scartata")
	}

	full := egress.Stats()
	if full.CurrentQueueDepth != 2 {
		t.Fatalf("stats queue piena=%#v", full)
	}

	close(release)
	final := egress.CloseAndWait()
	if final.CurrentQueueDepth != 0 ||
		final.PublishAttempts != 3 {
		t.Fatalf("stats finali=%#v", final)
	}
}

func TestTelemetryEgressDrainsAndTracksPublishErrors(t *testing.T) {
	published := make([]model.SensorEvent, 0, 2)
	egress, err := newTelemetryEgress(
		2,
		func(_ string, event model.SensorEvent) error {
			published = append(published, event)
			if event.Sequence == 2 {
				return errors.New("publish fallito")
			}
			return nil
		},
		func() time.Time { return testPublishTime },
	)
	if err != nil {
		t.Fatal(err)
	}

	if !egress.TryEnqueue(testSensorEvent("1", 0, 1)) ||
		!egress.TryEnqueue(testSensorEvent("1", 1, 2)) {
		t.Fatal("telemetry non accettata con spazio disponibile")
	}

	stats := egress.CloseAndWait()
	if len(published) != 2 ||
		published[0].Sequence != 1 ||
		published[1].Sequence != 2 ||
		stats.CurrentQueueDepth != 0 ||
		stats.PublishAttempts != 2 ||
		stats.PublishErrors != 1 {
		t.Fatalf("eventi=%#v stats=%#v", published, stats)
	}
}

func TestTelemetryEgressConcurrentCloseIsSafe(t *testing.T) {
	egress, err := newTelemetryEgress(
		1,
		func(string, model.SensorEvent) error { return nil },
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !egress.TryEnqueue(testSensorEvent("1", 0, 1)) {
		t.Fatal("telemetry non accettata con spazio disponibile")
	}

	results := make(chan TelemetryEgressStats, 2)
	for range 2 {
		go func() {
			results <- egress.CloseAndWait()
		}()
	}

	for range 2 {
		select {
		case stats := <-results:
			if stats.CurrentQueueDepth != 0 || stats.PublishAttempts != 1 {
				t.Fatalf("stats dopo close=%#v", stats)
			}
		case <-time.After(time.Second):
			t.Fatal("CloseAndWait concorrente non terminata")
		}
	}
}

func TestReplayCountsOffersAndKeepsLastEventTimeWhenLastEventDrops(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	queue := &scriptedTelemetryQueue{
		accept: []bool{true, false, false},
		stats: TelemetryEgressStats{
			PublishAttempts: 1,
		},
	}
	var endTopic string
	runtime := testReplayRuntime(clock, func(string, model.SensorEvent) error { return nil })
	runtime.NewTelemetryEgress = func(
		int,
		TelemetryPublisher,
		func() time.Time,
	) (TelemetryQueue, error) {
		return queue, nil
	}
	runtime.PublishEndOfReplay = func(
		topic string,
	) (PublishResult, error) {
		endTopic = topic
		return PublishResult{
			Token:       newCompletedToken(nil),
			PublishedAt: clock.Now(),
		}, nil
	}

	stats, err := replaySite(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"502;BME280;5;45.0;9.0;2025-01-01T00:00:01Z;100000;20;50",
			"503;BME280;5;45.0;9.0;2025-01-01T00:00:02Z;100000;20;50",
		),
		config,
		runtime,
	)
	if err != nil {
		t.Fatal(err)
	}
	last := mustTime("2025-01-01T00:00:02Z")
	if stats.OfferedEvents != 3 ||
		stats.TelemetryEnqueued != 1 ||
		stats.TelemetryLocallyDropped != 2 ||
		stats.MQTTPublishAttempts != 1 ||
		!stats.LastEventTime.Equal(last) ||
		endTopic != mqtttopic.ReplayEnd(config.SiteID) ||
		queue.closeCalls != 1 {
		t.Fatalf("stats=%#v eos_topic=%q queue=%#v", stats, endTopic, queue)
	}
}

func TestReplayDrainsLocalQueueBeforeEndOfReplay(t *testing.T) {
	config := validSimulatorConfig()
	clock := newFakeClock(config.ReplayStartAt)
	endToken := newAwaitableToken(nil)
	var mu sync.Mutex
	order := make([]string, 0, 4)
	runtime := testReplayRuntime(clock, func(_ string, event model.SensorEvent) error {
		mu.Lock()
		order = append(order, "telemetry-"+event.EventID)
		mu.Unlock()
		return nil
	})
	runtime.PublishEndOfReplay = func(
		_ string,
	) (PublishResult, error) {
		mu.Lock()
		order = append(order, "publish-end")
		mu.Unlock()
		endToken.onWait = func() {
			mu.Lock()
			order = append(order, "ack-end")
			mu.Unlock()
		}
		return PublishResult{
			Token:       endToken,
			PublishedAt: clock.Now(),
		}, nil
	}

	stats, err := replaySite(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"502;BME280;5;45.0;9.0;2025-01-01T00:00:01Z;100000;20;50",
		),
		config,
		runtime,
	)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"telemetry-501-1",
		"telemetry-502-1",
		"publish-end",
		"ack-end",
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if strings.Join(got, ",") != strings.Join(want, ",") ||
		endToken.waitCalls != 1 ||
		stats.EOSSuccesses != 1 ||
		stats.EOSFailures != 0 {
		t.Fatalf("ordine=%v stats=%#v waits=%d", got, stats, endToken.waitCalls)
	}
}

func TestReplayFailsOnEndOfReplayPublishErrorAndTimeout(t *testing.T) {
	for _, test := range []struct {
		name      string
		publisher EndOfReplayPublisher
		contains  string
	}{
		{
			name: "publish_error",
			publisher: func(string) (PublishResult, error) {
				return PublishResult{}, errors.New("publish end fallito")
			},
			contains: "publish end fallito",
		},
		{
			name: "ack_timeout",
			publisher: func(string) (PublishResult, error) {
				return PublishResult{
					Token:       newTimeoutToken(),
					PublishedAt: testPublishTime,
				}, nil
			},
			contains: "timeout PUBACK",
		},
		{
			name: "ack_error",
			publisher: func(string) (PublishResult, error) {
				return PublishResult{
					Token:       newAwaitableToken(errors.New("PUBACK fallito")),
					PublishedAt: testPublishTime,
				}, nil
			},
			contains: "PUBACK fallito",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validSimulatorConfig()
			clock := newFakeClock(config.ReplayStartAt)
			runtime := testReplayRuntime(clock, func(string, model.SensorEvent) error { return nil })
			runtime.PublishEndOfReplay = test.publisher

			stats, err := replaySite(
				replayReader(
					"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
				),
				config,
				runtime,
			)
			if err == nil || !strings.Contains(err.Error(), test.contains) ||
				stats.EOSFailures != 1 || stats.EOSSuccesses != 0 {
				t.Fatalf("stats=%#v err=%v", stats, err)
			}
		})
	}
}

func TestPublishSensorEventUsesQoSZeroWithoutWaiting(t *testing.T) {
	token := newTimeoutToken()
	var capturedQoS byte
	var capturedRetained bool
	var capturedPayload []byte
	event := model.SensorEvent{
		EventID:   "101-1",
		SensorID:  "101",
		EventTime: mustTime("2025-01-01T00:00:00Z"),
		EmittedAt: testPublishTime,
	}

	err := publishSensorEvent(
		func(
			_ string,
			qos byte,
			retained bool,
			payload interface{},
		) mqtt.Token {
			capturedQoS = qos
			capturedRetained = retained
			capturedPayload = append([]byte(nil), payload.([]byte)...)
			return token
		},
		"sensors/101/telemetry",
		event,
	)
	if err != nil {
		t.Fatal(err)
	}
	if capturedQoS != 0 || capturedRetained || token.waitCalls != 0 {
		t.Fatalf("qos=%d retained=%t waits=%d", capturedQoS, capturedRetained, token.waitCalls)
	}
	if strings.Contains(string(capturedPayload), `"schema_version"`) {
		t.Fatalf("payload contiene schema_version: %s", capturedPayload)
	}

	var decoded model.SensorEvent
	if err := json.Unmarshal(capturedPayload, &decoded); err != nil || decoded.EventID != event.EventID {
		t.Fatalf("payload=%#v err=%v", decoded, err)
	}
}

func TestPublishEndOfReplayUsesQoSOneWithoutRetain(t *testing.T) {
	token := newAwaitableToken(nil)
	var qos byte
	var retained bool
	var payload []byte
	var payloadIsBytes bool

	result, err := publishEndOfReplay(
		func(
			_ string,
			gotQoS byte,
			gotRetained bool,
			gotPayload interface{},
		) mqtt.Token {
			qos = gotQoS
			retained = gotRetained
			payload, payloadIsBytes = gotPayload.([]byte)
			return token
		},
		"replay/edge-3/end",
		func() time.Time { return testPublishTime },
	)
	if err != nil {
		t.Fatal(err)
	}
	if qos != 1 || retained || !payloadIsBytes || len(payload) != 0 ||
		result.Token != token || token.waitCalls != 0 {
		t.Fatalf(
			"qos=%d retained=%t payload=%#v payload_is_bytes=%t result=%#v waits=%d",
			qos,
			retained,
			payload,
			payloadIsBytes,
			result,
			token.waitCalls,
		)
	}
}

func TestLoadSimulatorConfig(t *testing.T) {
	config, err := loadSimulatorConfig(envFrom(validSimulatorEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if config.SiteID != "edge-3" ||
		config.MQTTEndpoint != "tcp://mqtt-edge-3:1883" ||
		config.MaxEvents != 25 ||
		config.AccelerationFactor != 2.5 ||
		config.TelemetryQueueCapacity != 123 {
		t.Fatalf("configurazione inattesa: %#v", config)
	}
}

func TestLoadSimulatorConfigValidatesTelemetryQueueCapacity(t *testing.T) {
	for _, value := range []string{"0", "-1", "invalid"} {
		t.Run(value, func(t *testing.T) {
			values := validSimulatorEnv()
			values["TELEMETRY_QUEUE_CAPACITY"] = value
			_, err := loadSimulatorConfig(envFrom(values))
			if err == nil || !strings.Contains(err.Error(), "TELEMETRY_QUEUE_CAPACITY") {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}

	values := validSimulatorEnv()
	delete(values, "TELEMETRY_QUEUE_CAPACITY")
	config, err := loadSimulatorConfig(envFrom(values))
	if err != nil || config.TelemetryQueueCapacity != defaultTelemetryQueueCapacity {
		t.Fatalf("config=%#v err=%v", config, err)
	}
}

func TestLoadSimulatorConfigValidatesReplayInputs(t *testing.T) {
	for _, missing := range []string{"SITE_ID", "MQTT_ENDPOINT", "REPLAY_FILE", "REPLAY_START_AT"} {
		t.Run("missing_"+missing, func(t *testing.T) {
			values := validSimulatorEnv()
			delete(values, missing)
			_, err := loadSimulatorConfig(envFrom(values))
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}

	for _, factor := range []string{"0", "-1", "invalid", "NaN", "+Inf"} {
		t.Run("factor_"+factor, func(t *testing.T) {
			values := validSimulatorEnv()
			values["ACCELERATION_FACTOR"] = factor
			_, err := loadSimulatorConfig(envFrom(values))
			if err == nil || !strings.Contains(err.Error(), "ACCELERATION_FACTOR") {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}
}

func TestSharedMQTTTopics(t *testing.T) {
	if got := mqtttopic.Telemetry("87575"); got != "sensors/87575/telemetry" {
		t.Fatalf("topic telemetry=%q", got)
	}
	if got := mqtttopic.ReplayEnd("edge-3"); got != "replay/edge-3/end" {
		t.Fatalf("topic end=%q", got)
	}
}

func TestTelemetryEgressUsesOneSenderGoroutine(t *testing.T) {
	file := parseGoSource(t, "telemetry_egress.go")
	goStatements := 0
	ast.Inspect(file, func(node ast.Node) bool {
		if _, ok := node.(*ast.GoStmt); ok {
			goStatements++
		}
		return true
	})
	if goStatements != 1 {
		t.Fatalf("goroutine nel telemetry egress=%d, attesa 1", goStatements)
	}
}

func TestSimulatorCreatesExactlyOneMQTTClientAndHasNoPendingQueue(t *testing.T) {
	productiveFiles := []string{
		"main.go",
		"config.go",
		"replay.go",
		"mqtt.go",
		"telemetry_egress.go",
	}

	newClientCalls := 0
	var source strings.Builder
	for _, path := range productiveFiles {
		file := parseGoSource(t, path)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "NewClient" {
				newClientCalls++
			}
			return true
		})

		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		source.Write(contents)
	}

	if newClientCalls != 1 {
		t.Fatalf("chiamate mqtt.NewClient=%d, attesa 1", newClientCalls)
	}

	if strings.Contains(source.String(), "Pending"+"Publishes") ||
		strings.Contains(source.String(), "MQTT_MAX"+"_IN_FLIGHT") {
		t.Fatal("rimasta logica QoS1 in-flight per raw telemetry")
	}
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Sleep(duration time.Duration) {
	clock.mu.Lock()
	clock.sleeps = append(clock.sleeps, duration)
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

type fakeToken struct {
	mu         sync.Mutex
	done       chan struct{}
	completed  bool
	waitResult bool
	err        error
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

func (token *fakeToken) complete() {
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.completed {
		return
	}
	token.completed = true
	close(token.done)
}

func (token *fakeToken) Wait() bool {
	<-token.done
	return true
}

func (token *fakeToken) WaitTimeout(_ time.Duration) bool {
	token.mu.Lock()
	token.waitCalls++
	onWait := token.onWait
	result := token.waitResult
	token.mu.Unlock()
	if onWait != nil {
		onWait()
	}
	if result {
		token.complete()
	}
	return result
}

func (token *fakeToken) Done() <-chan struct{} {
	return token.done
}

func (token *fakeToken) Error() error {
	token.mu.Lock()
	defer token.mu.Unlock()
	return token.err
}

type scriptedTelemetryQueue struct {
	accept     []bool
	calls      int
	closeCalls int
	stats      TelemetryEgressStats
}

func (
	queue *scriptedTelemetryQueue,
) TryEnqueue(
	_ model.SensorEvent,
) bool {
	accepted := queue.accept[queue.calls]
	queue.calls++
	return accepted
}

func (
	queue *scriptedTelemetryQueue,
) CloseAndWait() TelemetryEgressStats {
	queue.closeCalls++
	return queue.stats
}

func testReplayRuntime(
	clock *fakeClock,
	publish TelemetryPublisher,
) ReplayRuntime {
	return ReplayRuntime{
		Now:                clock.Now,
		Sleep:              clock.Sleep,
		PublishTelemetry:   publish,
		PublishEndOfReplay: completedEndPublisher(clock),
	}
}

func completedEndPublisher(clock *fakeClock) EndOfReplayPublisher {
	return func(
		_ string,
	) (PublishResult, error) {
		return PublishResult{
			Token:       newCompletedToken(nil),
			PublishedAt: clock.Now(),
		}, nil
	}
}

func validSimulatorConfig() SimulatorConfig {
	return SimulatorConfig{
		SiteID:                 "edge-3",
		ReplayEpoch:            mustTime("2025-01-01T00:00:00Z"),
		ReplayStartAt:          mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor:     10,
		TelemetryQueueCapacity: 10,
	}
}

func validSimulatorEnv() map[string]string {
	return map[string]string{
		"SITE_ID":                  "edge-3",
		"MQTT_ENDPOINT":            "tcp://mqtt-edge-3:1883",
		"REPLAY_FILE":              "/app/dataset/derived/replay_by_edge/edge-3.csv",
		"MAX_EVENTS":               "25",
		"REPLAY_EPOCH":             "2025-01-01T00:00:00Z",
		"REPLAY_START_AT":          "2026-08-28T20:00:10Z",
		"ACCELERATION_FACTOR":      "2.5",
		"TELEMETRY_QUEUE_CAPACITY": "123",
	}
}

func replayReader(rows ...string) *csv.Reader {
	const header = "sensor_id;sensor_type;location;lat;lon;timestamp;pressure;temperature;humidity"
	reader := csv.NewReader(strings.NewReader(
		header + "\n" + strings.Join(rows, "\n") + "\n",
	))
	reader.Comma = ';'
	return reader
}

func envFrom(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func mustTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed.UTC()
}

func testMeasurement(sensorID string, second int) SensorMeasurement {
	return SensorMeasurement{
		SensorID:    sensorID,
		SensorType:  "BME280",
		LocationID:  "1",
		EventTime:   mustTime("2025-01-01T00:00:00Z").Add(time.Duration(second) * time.Second),
		Pressure:    "100000",
		Temperature: "20",
		Humidity:    "50",
	}
}

func testSensorEvent(sensorID string, second int, sequence uint64) model.SensorEvent {
	event, err := buildSensorEvent(testMeasurement(sensorID, second), sequence)
	if err != nil {
		panic(err)
	}
	return event
}

func parseGoSource(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("lettura %s fallita: %v", path, err)
	}
	return file
}

var testPublishTime = mustTime("2026-08-28T20:00:00Z")
