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

	scheduled := pacer.ScheduledTime(
		mustTime("2025-01-01T00:10:00Z"),
	)
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
			got := pacer.ScheduledTime(epoch.Add(test.offset))
			if want := start.Add(test.want); !got.Equal(want) {
				t.Fatalf("scheduled=%s, atteso %s", got, want)
			}
		})
	}
}

func TestReplayPacerTranslatesEventTimeRelativeToEpoch(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 10,
	}

	scheduled := pacer.ScheduledTime(pacer.Epoch.Add(-10 * time.Second))
	if !scheduled.Before(pacer.StartAt) {
		t.Fatalf("scheduled=%s non prima di startAt=%s", scheduled, pacer.StartAt)
	}
}

func TestReplayPacerDoesNotUseFirstShardEventAsEpoch(t *testing.T) {
	pacer := ReplayPacer{
		Epoch:              mustTime("2025-01-01T00:00:00Z"),
		StartAt:            mustTime("2026-08-28T20:00:00Z"),
		AccelerationFactor: 100,
	}
	eventTime := pacer.Epoch.Add(10 * time.Minute)

	first := pacer.ScheduledTime(eventTime)
	_ = pacer.ScheduledTime(pacer.Epoch.Add(2 * time.Hour))
	second := pacer.ScheduledTime(eventTime)
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
	var topics []string
	var events []model.SensorEvent

	stats, err := replaySite(
		replayReader(
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"102;BME280;2;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:02Z;100002;22;52",
		),
		config,
		testReplayRuntime(func(topic string, event model.SensorEvent) error {
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

	stats, err := replaySite(
		replayReader(
			"101;BME280;1;45.0;9.0;2025-01-01T00:00:00Z;100000;abc;50",
		),
		config,
		testReplayRuntime(func(string, model.SensorEvent) error { return nil }),
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
	eosCalls := 0
	runtime := testReplayRuntime(func(string, model.SensorEvent) error {
		return nil
	})
	runtime.PublishEndOfReplay = func(string) error {
		eosCalls++
		return nil
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

	_, err := replaySite(
		replayReader(
			"301;BME280;3;45.0;9.0;2025-01-01T00:00:02Z;100000;20;50",
			"302;BME280;3;45.0;9.0;2025-01-01T00:00:01Z;100001;21;51",
		),
		config,
		testReplayRuntime(func(string, model.SensorEvent) error { return nil }),
	)
	if err == nil || !strings.Contains(err.Error(), "non ordinato") {
		t.Fatalf("errore inatteso: %v", err)
	}
}

func TestReplayRejectsLateStartButNotLaterLateness(t *testing.T) {
	config := validSimulatorConfig()
	config.ReplayStartAt = time.Now().UTC().Add(-15 * time.Second)

	stats, err := replaySite(
		replayReader(
			"401;BME280;4;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
		),
		config,
		testReplayRuntime(func(string, model.SensorEvent) error { return nil }),
	)
	if err == nil || !strings.Contains(err.Error(), "avviato troppo tardi") ||
		stats.OfferedEvents != 0 {
		t.Fatalf("stats=%#v err=%v", stats, err)
	}

	config = validSimulatorConfig()
	config.ReplayStartAt = time.Now().UTC().Add(-9 * time.Second)
	runtime := testReplayRuntime(func(string, model.SensorEvent) error { return nil })
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
	if stats.OfferedEvents != 2 || stats.SchedulingLagMax < 8*time.Second {
		t.Fatalf("stats=%#v", stats)
	}
}

func TestReplayStatsThroughputUsesOnlyOfferWindow(t *testing.T) {
	firstOfferedAt := mustTime("2026-08-28T20:00:00Z")
	stats := ReplayStats{}
	stats.RecordOffer(firstOfferedAt, 0)
	stats.RecordOffer(firstOfferedAt.Add(2*time.Second), 0)
	stats.CompletedAt = firstOfferedAt.Add(time.Minute)

	if stats.OfferDuration() != 2*time.Second ||
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

func TestTelemetryEgressDropsWithoutBlockingWhenQueueIsFull(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	egress := newReplayEgress(
		"edge-3",
		1,
		func(_ string, _ model.SensorEvent) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		},
		func(string) error { return nil },
	)

	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 0, 1)) {
		t.Fatal("prima telemetry non accettata")
	}
	<-started
	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 1, 2)) {
		t.Fatal("seconda telemetry non accettata nella coda libera")
	}

	enqueueResult := make(chan bool, 1)
	go func() {
		enqueueResult <- egress.TryEnqueueTelemetry(testSensorEvent("1", 2, 3))
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
	stats, err := egress.CloseAndWait()
	if err != nil {
		t.Fatal(err)
	}
	if stats.CurrentQueueDepth != 0 ||
		stats.PublishAttempts != 2 ||
		stats.PublishErrors != 0 {
		t.Fatalf("egress stats=%#v", stats)
	}
}

func TestTelemetryEgressDrainsAndTracksPublishErrors(t *testing.T) {
	published := make([]model.SensorEvent, 0, 2)
	egress := newReplayEgress(
		"edge-3",
		2,
		func(_ string, event model.SensorEvent) error {
			published = append(published, event)
			if event.Sequence == 2 {
				return errors.New("publish fallito")
			}
			return nil
		},
		func(string) error { return nil },
	)

	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 0, 1)) ||
		!egress.TryEnqueueTelemetry(testSensorEvent("1", 1, 2)) {
		t.Fatal("telemetry non accettata con spazio disponibile")
	}

	stats, err := egress.CloseAndWait()
	if err != nil {
		t.Fatal(err)
	}
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
	egress := newReplayEgress(
		"edge-3",
		1,
		func(string, model.SensorEvent) error { return nil },
		func(string) error { return nil },
	)
	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 0, 1)) {
		t.Fatal("telemetry non accettata con spazio disponibile")
	}

	results := make(chan ReplayEgressStats, 2)
	for range 2 {
		go func() {
			stats, _ := egress.CloseAndWait()
			results <- stats
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

func TestRunReplayLoopCountsOffersAndKeepsLastEventTimeWhenLastEventDrops(t *testing.T) {
	config := validSimulatorConfig()
	egress := &ReplayEgress{
		queue: make(chan ReplayEgressRecord, 1),
	}
	pacer := ReplayPacer{
		Epoch:              config.ReplayEpoch,
		StartAt:            localReplayStart(time.Now(), config.ReplayStartAt),
		AccelerationFactor: config.AccelerationFactor,
	}
	stats := ReplayStats{}
	err := runReplayLoop(
		replayReader(
			"501;BME280;5;45.0;9.0;2025-01-01T00:00:00Z;100000;20;50",
			"502;BME280;5;45.0;9.0;2025-01-01T00:00:01Z;100000;20;50",
			"503;BME280;5;45.0;9.0;2025-01-01T00:00:02Z;100000;20;50",
		),
		config,
		pacer,
		egress,
		&stats,
	)
	if err != nil {
		t.Fatal(err)
	}
	last := mustTime("2025-01-01T00:00:02Z")
	if stats.OfferedEvents != 3 ||
		stats.TelemetryEnqueued != 1 ||
		stats.TelemetryLocallyDropped != 2 ||
		!stats.LastEventTime.Equal(last) {
		t.Fatalf("stats=%#v", stats)
	}
}

func TestReplayDrainsLocalQueueBeforeEndOfReplay(t *testing.T) {
	config := validSimulatorConfig()
	var mu sync.Mutex
	order := make([]string, 0, 4)
	runtime := testReplayRuntime(func(_ string, event model.SensorEvent) error {
		mu.Lock()
		order = append(order, "telemetry-"+event.EventID)
		mu.Unlock()
		return nil
	})
	runtime.PublishEndOfReplay = func(_ string) error {
		mu.Lock()
		order = append(order, "publish-end")
		order = append(order, "ack-end")
		mu.Unlock()
		return nil
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
		stats.EOSSuccesses != 1 ||
		stats.EOSFailures != 0 {
		t.Fatalf("ordine=%v stats=%#v", got, stats)
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
			publisher: func(string) error {
				return errors.New("publish end fallito")
			},
			contains: "publish end fallito",
		},
		{
			name: "ack_timeout",
			publisher: func(string) error {
				return errors.New("timeout PUBACK MQTT")
			},
			contains: "timeout PUBACK",
		},
		{
			name: "ack_error",
			publisher: func(string) error {
				return errors.New("PUBACK fallito")
			},
			contains: "PUBACK fallito",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validSimulatorConfig()
			runtime := testReplayRuntime(func(string, model.SensorEvent) error { return nil })
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

	err := publishEndOfReplay(
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
	)
	if err != nil {
		t.Fatal(err)
	}
	if qos != 1 || retained || !payloadIsBytes || len(payload) != 0 ||
		token.waitCalls != 1 {
		t.Fatalf(
			"qos=%d retained=%t payload=%#v payload_is_bytes=%t waits=%d",
			qos,
			retained,
			payload,
			payloadIsBytes,
			token.waitCalls,
		)
	}
}

func TestLoadSimulatorConfig(t *testing.T) {
	setSimulatorEnv(t, validSimulatorEnv())
	config, err := loadSimulatorConfig()
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
			setSimulatorEnv(t, values)
			_, err := loadSimulatorConfig()
			if err == nil || !strings.Contains(err.Error(), "TELEMETRY_QUEUE_CAPACITY") {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}

	values := validSimulatorEnv()
	delete(values, "TELEMETRY_QUEUE_CAPACITY")
	setSimulatorEnv(t, values)
	config, err := loadSimulatorConfig()
	if err != nil || config.TelemetryQueueCapacity != defaultTelemetryQueueCapacity {
		t.Fatalf("config=%#v err=%v", config, err)
	}
}

func TestLoadSimulatorConfigValidatesReplayInputs(t *testing.T) {
	for _, missing := range []string{"SITE_ID", "MQTT_ENDPOINT", "REPLAY_FILE", "REPLAY_START_AT"} {
		t.Run("missing_"+missing, func(t *testing.T) {
			values := validSimulatorEnv()
			delete(values, missing)
			setSimulatorEnv(t, values)
			_, err := loadSimulatorConfig()
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}

	for _, factor := range []string{"0", "-1", "invalid", "NaN", "+Inf"} {
		t.Run("factor_"+factor, func(t *testing.T) {
			values := validSimulatorEnv()
			values["ACCELERATION_FACTOR"] = factor
			setSimulatorEnv(t, values)
			_, err := loadSimulatorConfig()
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

func TestReplayEgressUsesOneSenderGoroutine(t *testing.T) {
	file := parseGoSource(t, "replay_egress.go")
	goStatements := 0
	ast.Inspect(file, func(node ast.Node) bool {
		if _, ok := node.(*ast.GoStmt); ok {
			goStatements++
		}
		return true
	})
	if goStatements != 1 {
		t.Fatalf("goroutine nel replay egress=%d, attesa 1", goStatements)
	}
}

func TestSimulatorCreatesExactlyOneMQTTClientAndHasNoPendingQueue(t *testing.T) {
	productiveFiles := []string{
		"main.go",
		"config.go",
		"replay.go",
		"mqtt.go",
		"replay_egress.go",
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

func testReplayRuntime(
	publish TelemetryPublisher,
) ReplayRuntime {
	return ReplayRuntime{
		PublishTelemetry:   publish,
		PublishEndOfReplay: completedEndPublisher(),
	}
}

func completedEndPublisher() EndOfReplayPublisher {
	return func(string) error {
		return nil
	}
}

func validSimulatorConfig() SimulatorConfig {
	return SimulatorConfig{
		SiteID:                 "edge-3",
		ReplayEpoch:            mustTime("2025-01-01T00:00:00Z"),
		ReplayStartAt:          time.Now().UTC().Add(-time.Second),
		AccelerationFactor:     10,
		TelemetryQueueCapacity: 10,
		StartLateTolerance:     10 * time.Second,
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

func setSimulatorEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for _, name := range []string{
		"SITE_ID",
		"MQTT_ENDPOINT",
		"REPLAY_FILE",
		"MAX_EVENTS",
		"REPLAY_EPOCH",
		"REPLAY_START_AT",
		"ACCELERATION_FACTOR",
		"TELEMETRY_QUEUE_CAPACITY",
		"START_LATE_TOLERANCE",
	} {
		t.Setenv(name, values[name])
	}
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

func TestReplayEgressSingleStreamOrder(t *testing.T) {
	order := make([]string, 0, 4)
	var mu sync.Mutex

	egress := newReplayEgress(
		"edge-3",
		10,
		func(_ string, event model.SensorEvent) error {
			mu.Lock()
			order = append(order, "T:"+event.EventID)
			mu.Unlock()
			return nil
		},
		func(topic string) error {
			mu.Lock()
			order = append(order, "EOS:"+topic)
			mu.Unlock()
			return nil
		},
	)

	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 0, 1)) ||
		!egress.TryEnqueueTelemetry(testSensorEvent("1", 1, 2)) ||
		!egress.TryEnqueueTelemetry(testSensorEvent("1", 2, 3)) {
		t.Fatal("telemetrie non accettate con coda capiente")
	}

	egress.EnqueueEndOfReplay()

	stats, err := egress.CloseAndWait()
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()

	want := []string{
		"T:1-1",
		"T:1-2",
		"T:1-3",
		"EOS:replay/edge-3/end",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ordine errato: got=%v want=%v", got, want)
	}
	if stats.PublishAttempts != 3 || stats.EOSSuccesses != 1 || stats.EOSFailures != 0 {
		t.Fatalf("stats inattese: %#v", stats)
	}
}

func TestReplayEgressBlockingEndOfReplayDoesNotDropWhenQueueIsFull(t *testing.T) {
	started := make(chan struct{})
	consumerRelease := make(chan struct{})
	var once sync.Once

	eventsProcessed := make([]string, 0, 4)
	var mu sync.Mutex

	egress := newReplayEgress(
		"edge-3",
		2, // capacità 2
		func(_ string, event model.SensorEvent) error {
			once.Do(func() { close(started) })
			<-consumerRelease
			mu.Lock()
			eventsProcessed = append(eventsProcessed, "T:"+event.EventID)
			mu.Unlock()
			return nil
		},
		func(topic string) error {
			mu.Lock()
			eventsProcessed = append(eventsProcessed, "EOS:"+topic)
			mu.Unlock()
			return nil
		},
	)

	// Riempie la coda con 2 telemetrie (consumer bloccato su item 1)
	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 0, 1)) {
		t.Fatal("T1 non accettata")
	}
	<-started // consumer ha prelevato T1 ed è bloccato prima del release
	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 1, 2)) {
		t.Fatal("T2 non accettata nello slot 1")
	}
	if !egress.TryEnqueueTelemetry(testSensorEvent("1", 2, 3)) {
		t.Fatal("T3 non accettata nello slot 2")
	}

	// Ora la coda è piena (2 elementi: T2 e T3). Una nuova telemetria viene droppata non-bloccante
	if egress.TryEnqueueTelemetry(testSensorEvent("1", 3, 4)) {
		t.Fatal("T4 accettata su coda piena!")
	}

	// EnqueueEndOfReplay deve BLOCCARE finché la coda non si libera
	eosEnqueued := make(chan struct{})
	go func() {
		egress.EnqueueEndOfReplay()
		close(eosEnqueued)
	}()

	// Verifica che EnqueueEndOfReplay stia effettivamente bloccando
	select {
	case <-eosEnqueued:
		t.Fatal("EnqueueEndOfReplay non ha bloccato su coda piena")
	case <-time.After(50 * time.Millisecond):
		// Corretto: sta bloccando
	}

	// Sblocca il consumer per consumare tutto
	close(consumerRelease)

	// Ora EnqueueEndOfReplay deve sbloccarsi ed entrare in coda
	select {
	case <-eosEnqueued:
		// Corretto
	case <-time.After(time.Second):
		t.Fatal("EnqueueEndOfReplay non si è sbloccato dopo il drenaggio")
	}

	stats, err := egress.CloseAndWait()
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	got := append([]string(nil), eventsProcessed...)
	mu.Unlock()

	want := []string{
		"T:1-1",
		"T:1-2",
		"T:1-3",
		"EOS:replay/edge-3/end",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sequenza elaborata inattesa: got=%v want=%v", got, want)
	}
	if stats.PublishAttempts != 3 || stats.EOSSuccesses != 1 || stats.EOSFailures != 0 {
		t.Fatalf("stats inattese: %#v", stats)
	}
}

func TestReplayEgressPreservesCompletedAtBeforeEOSDelay(t *testing.T) {
	beforeDrain := time.Now()
	eosDelay := 60 * time.Millisecond

	egress := newReplayEgress(
		"edge-3",
		10,
		func(string, model.SensorEvent) error {
			return nil
		},
		func(string) error {
			time.Sleep(eosDelay)
			return nil
		},
	)

	egress.TryEnqueueTelemetry(testSensorEvent("1", 0, 1))
	egress.EnqueueEndOfReplay()

	stats, err := egress.CloseAndWait()
	if err != nil {
		t.Fatal(err)
	}

	afterClose := time.Now()

	// TelemetryDrainedAt deve essere registrato PRIMA del delay di pubblicazione dell'EOS
	if stats.TelemetryDrainedAt.Before(beforeDrain) {
		t.Fatalf("TelemetryDrainedAt=%s precedente a beforeDrain=%s", stats.TelemetryDrainedAt, beforeDrain)
	}
	elapsedSinceDrain := afterClose.Sub(stats.TelemetryDrainedAt)
	if elapsedSinceDrain < eosDelay {
		t.Fatalf("TelemetryDrainedAt include il tempo di attesa dell'EOS: elapsed=%s eosDelay=%s", elapsedSinceDrain, eosDelay)
	}
}

func TestReplayEgressPropagatesEOSErrorToCloseAndWait(t *testing.T) {
	expectedErr := errors.New("simulato errore PUBACK")

	egress := newReplayEgress(
		"edge-3",
		10,
		func(string, model.SensorEvent) error {
			return nil
		},
		func(string) error {
			return expectedErr
		},
	)

	egress.TryEnqueueTelemetry(testSensorEvent("1", 0, 1))
	egress.EnqueueEndOfReplay()

	stats, err := egress.CloseAndWait()
	if err == nil || !strings.Contains(err.Error(), "fallita") {
		t.Fatalf("atteso errore di EOS da CloseAndWait, ottenuto: %v", err)
	}
	if stats.EOSFailures != 1 || stats.EOSSuccesses != 0 {
		t.Fatalf("stats EOS errate: %#v", stats)
	}
	if stats.PublishAttempts != 1 {
		t.Fatalf("publish telemetry tentate errate: %d", stats.PublishAttempts)
	}
}
