package globalaggregator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"continuum/internal/model"
)

func TestAggregatorComposesThirteenEdgesWithoutAverageOfAverages(
	t *testing.T,
) {
	expected := testEdgeIDs(13)
	outputs := make([]model.GlobalAggregate, 0)
	aggregator := newTestAggregator(t, expected, func(
		_ context.Context,
		output model.GlobalAggregate,
	) error {
		outputs = append(outputs, output)
		return nil
	})
	start := globalTestTime(10, 0)

	var expectedEvents uint64
	var expectedValid uint64
	var expectedInvalid uint64
	var expectedSum float64
	var unweightedAverageSum float64
	for index, edgeID := range expected {
		valid := uint64(index + 1)
		invalid := uint64(index % 2)
		average := float64(10 + index)
		sum := average * float64(valid)
		expectedEvents += valid + invalid
		expectedValid += valid
		expectedInvalid += invalid
		expectedSum += sum
		unweightedAverageSum += average

		if err := aggregator.Add(
			context.Background(),
			testCloudAggregate(
				edgeID,
				start,
				valid,
				invalid,
				sum,
				average,
				average,
			),
		); err != nil {
			t.Fatalf("Add(%s) fallita: %v", edgeID, err)
		}
		if index < len(expected)-1 && len(outputs) != 0 {
			t.Fatalf("finestra emessa dopo soli %d contributi", index+1)
		}
	}

	if len(outputs) != 1 {
		t.Fatalf("output=%d, atteso 1", len(outputs))
	}
	output := outputs[0]
	if output.ExpectedEdges != 13 || output.ContributingEdges != 13 {
		t.Fatalf(
			"edge counts inattesi: expected=%d contributing=%d",
			output.ExpectedEdges,
			output.ContributingEdges,
		)
	}
	if output.Events != expectedEvents {
		t.Fatalf("Events=%d, atteso %d", output.Events, expectedEvents)
	}
	assertGlobalMetric(
		t,
		output.Temperature,
		expectedValid,
		expectedInvalid,
		expectedSum,
		10,
		22,
	)

	weightedAverage := expectedSum / float64(expectedValid)
	unweightedAverage := unweightedAverageSum / 13
	if math.Abs(*output.Temperature.Average-weightedAverage) > 1e-9 {
		t.Fatalf(
			"Average=%f, attesa Sum/Valid=%f",
			*output.Temperature.Average,
			weightedAverage,
		)
	}
	if math.Abs(*output.Temperature.Average-unweightedAverage) < 1e-9 {
		t.Fatal("Average globale calcolata come average-of-averages")
	}
	if err := ValidateGlobalAggregate(output); err != nil {
		t.Fatalf("GlobalAggregate emesso non valido: %v", err)
	}
}

func TestAggregatorDoesNotEmitWithMissingEdge(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	aggregator := newTestAggregator(t, testEdgeIDs(3), collectSink(&outputs))
	start := globalTestTime(10, 0)

	for _, edgeID := range []string{"edge-0", "edge-1"} {
		if err := aggregator.Add(
			context.Background(),
			testCloudAggregate(edgeID, start, 1, 0, 10, 10, 10),
		); err != nil {
			t.Fatal(err)
		}
	}
	if len(outputs) != 0 {
		t.Fatalf("finestra incompleta emessa: %#v", outputs)
	}
}

func TestAggregatorKeepsDifferentWindowsSeparate(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	aggregator := newTestAggregator(t, testEdgeIDs(2), collectSink(&outputs))
	first := globalTestTime(10, 0)
	second := globalTestTime(10, 15)

	for _, input := range []model.CloudEdgeAggregate{
		testCloudAggregate("edge-0", first, 1, 0, 10, 10, 10),
		testCloudAggregate("edge-1", first, 1, 0, 40, 40, 40),
		testCloudAggregate("edge-0", second, 1, 0, 20, 20, 20),
		testCloudAggregate("edge-1", second, 1, 0, 30, 30, 30),
	} {
		if err := aggregator.Add(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}

	if len(outputs) != 2 {
		t.Fatalf("output=%d, attesi 2", len(outputs))
	}
	if !outputs[0].WindowStart.Equal(first) ||
		!outputs[1].WindowStart.Equal(second) {
		t.Fatalf("finestre mescolate: %#v", outputs)
	}
}

func TestAggregatorRejectsStructuralWindowViolations(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*Aggregator)
		input   model.CloudEdgeAggregate
		want    string
	}{
		{
			name:  "unknown edge",
			input: testCloudAggregate("edge-9", globalTestTime(10, 0), 1, 0, 10, 10, 10),
			want:  "non atteso",
		},
		{
			name: "wrong duration",
			input: func() model.CloudEdgeAggregate {
				input := testCloudAggregate("edge-0", globalTestTime(10, 0), 1, 0, 10, 10, 10)
				input.WindowEnd = input.WindowStart.Add(10 * time.Minute)
				return input
			}(),
			want: "GLOBAL_WINDOW_SIZE",
		},
		{
			name:  "misaligned window",
			input: testCloudAggregate("edge-0", globalTestTime(10, 1), 1, 0, 10, 10, 10),
			want:  "non allineato",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			aggregator := newTestAggregator(t, testEdgeIDs(2), discardSink)
			if test.prepare != nil {
				test.prepare(aggregator)
			}
			err := aggregator.Add(context.Background(), test.input)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}
}

func TestAggregatorRejectsSecondContributionFromSameEdge(t *testing.T) {
	aggregator := newTestAggregator(t, testEdgeIDs(3), discardSink)
	input := testCloudAggregate(
		"edge-0",
		globalTestTime(10, 0),
		1,
		0,
		10,
		10,
		10,
	)
	if err := aggregator.Add(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	err := aggregator.Add(context.Background(), input)
	if err == nil || !strings.Contains(err.Error(), "piu volte") {
		t.Fatalf("seconda contribuzione non rifiutata: %v", err)
	}
}

func TestAggregatorRejectsReopeningClosedWindow(t *testing.T) {
	aggregator := newTestAggregator(t, testEdgeIDs(2), discardSink)
	start := globalTestTime(10, 0)
	for _, edgeID := range testEdgeIDs(2) {
		if err := aggregator.Add(
			context.Background(),
			testCloudAggregate(edgeID, start, 1, 0, 10, 10, 10),
		); err != nil {
			t.Fatal(err)
		}
	}

	err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", start, 1, 0, 10, 10, 10),
	)
	if err == nil || !strings.Contains(err.Error(), "riaprire") {
		t.Fatalf("finestra chiusa riaperta: %v", err)
	}
}

func TestEndReplayFlushesIncompleteWindowsOnlyAfterAllEdges(
	t *testing.T,
) {
	outputs := make([]model.GlobalAggregate, 0)
	aggregator := newTestAggregator(t, testEdgeIDs(3), collectSink(&outputs))
	later := globalTestTime(10, 15)
	earlier := globalTestTime(10, 0)

	for _, start := range []time.Time{later, earlier} {
		if err := aggregator.Add(
			context.Background(),
			testCloudAggregate("edge-0", start, 2, 1, 30, 10, 20),
		); err != nil {
			t.Fatal(err)
		}
	}

	complete, err := aggregator.EndReplay(
		context.Background(),
		testEndOfReplay("edge-0"),
	)
	if err != nil || complete || len(outputs) != 0 {
		t.Fatalf("flush prematuro: complete=%t outputs=%d err=%v", complete, len(outputs), err)
	}
	complete, err = aggregator.EndReplay(
		context.Background(),
		testEndOfReplay("edge-0"),
	)
	if err != nil || complete || len(outputs) != 0 {
		t.Fatalf("EOS duplicato non idempotente: complete=%t outputs=%d err=%v", complete, len(outputs), err)
	}
	if complete, err = aggregator.EndReplay(
		context.Background(),
		testEndOfReplay("edge-1"),
	); err != nil || complete {
		t.Fatalf("secondo EOS: complete=%t err=%v", complete, err)
	}
	if complete, err = aggregator.EndReplay(
		context.Background(),
		testEndOfReplay("edge-2"),
	); err != nil || !complete {
		t.Fatalf("ultimo EOS: complete=%t err=%v", complete, err)
	}

	if len(outputs) != 2 {
		t.Fatalf("output finali=%d, attesi 2", len(outputs))
	}
	if !outputs[0].WindowStart.Equal(earlier) ||
		!outputs[1].WindowStart.Equal(later) {
		t.Fatalf("flush non ordinato: %#v", outputs)
	}
	for _, output := range outputs {
		if output.ExpectedEdges != 3 || output.ContributingEdges != 1 {
			t.Fatalf(
				"counts flush errati: expected=%d contributing=%d",
				output.ExpectedEdges,
				output.ContributingEdges,
			)
		}
	}
	if !aggregator.IsComplete() {
		t.Fatal("aggregatore non marcato completo")
	}

	complete, err = aggregator.EndReplay(
		context.Background(),
		testEndOfReplay("edge-2"),
	)
	if err != nil || !complete || len(outputs) != 2 {
		t.Fatalf("EOS dopo completion non idempotente: complete=%t outputs=%d err=%v", complete, len(outputs), err)
	}
}

func TestAggregatorRejectsUnknownEOSAndDataAfterEOS(t *testing.T) {
	aggregator := newTestAggregator(t, testEdgeIDs(2), discardSink)
	if _, err := aggregator.EndReplay(
		context.Background(),
		testEndOfReplay("edge-9"),
	); err == nil || !strings.Contains(err.Error(), "non atteso") {
		t.Fatalf("EOS sconosciuto accettato: %v", err)
	}
	if _, err := aggregator.EndReplay(
		context.Background(),
		testEndOfReplay("edge-0"),
	); err != nil {
		t.Fatal(err)
	}
	err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 0), 1, 0, 10, 10, 10),
	)
	if err == nil || !strings.Contains(err.Error(), "dopo EndOfReplay") {
		t.Fatalf("data post-EOS accettata: %v", err)
	}
}

func TestAggregatorBuildsNilStatisticsWhenEveryValueIsInvalid(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	aggregator := newTestAggregator(t, []string{"edge-0"}, collectSink(&outputs))
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 0), 0, 3, 0, 0, 0),
	); err != nil {
		t.Fatal(err)
	}
	metric := outputs[0].Temperature
	if metric.Valid != 0 || metric.Invalid != 3 || metric.Sum != 0 ||
		metric.Average != nil || metric.Min != nil || metric.Max != nil {
		t.Fatalf("metrica all-invalid inattesa: %#v", metric)
	}
}

func TestNewValidatesExpectedEdges(t *testing.T) {
	for name, edgeIDs := range map[string][]string{
		"empty list": nil,
		"empty id":   {"edge-0", " "},
		"duplicate":  {"edge-0", "edge-0"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(
				edgeIDs,
				15*time.Minute,
				15*time.Minute,
				5*time.Second,
				discardSink,
			); err == nil {
				t.Fatal("configurazione attesa invalida")
			}
		})
	}
	if _, err := New(
		[]string{"edge-0"},
		15*time.Minute,
		15*time.Minute,
		0,
		discardSink,
	); err == nil {
		t.Fatal("edge idle timeout nullo accettato")
	}
}

func TestSinkFailureIsReturned(t *testing.T) {
	want := errors.New("sink unavailable")
	aggregator := newTestAggregator(t, []string{"edge-0"}, func(
		context.Context,
		model.GlobalAggregate,
	) error {
		return want
	})
	err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 0), 1, 0, 10, 10, 10),
	)
	if !errors.Is(err, want) {
		t.Fatalf("sink failure non propagato: %v", err)
	}
}

func TestValidateGlobalAggregate(t *testing.T) {
	valid := model.GlobalAggregate{
		SchemaVersion:     model.GlobalAggregateSchemaVersion,
		AggregateID:       "global:window",
		WindowStart:       globalTestTime(10, 0),
		WindowEnd:         globalTestTime(10, 15),
		ExpectedEdges:     13,
		ContributingEdges: 12,
		Events:            3,
		Temperature:       testMetric(2, 1, 30, 10, 20),
		Humidity:          testMetric(2, 1, 30, 10, 20),
		Pressure:          testMetric(2, 1, 30, 10, 20),
		EmittedAt:         globalTestTime(12, 0),
	}
	if err := ValidateGlobalAggregate(valid); err != nil {
		t.Fatalf("GlobalAggregate valido rifiutato: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*model.GlobalAggregate)
		want   string
	}{
		{"schema", func(a *model.GlobalAggregate) { a.SchemaVersion = 0 }, "schema_version"},
		{"id", func(a *model.GlobalAggregate) { a.AggregateID = " " }, "aggregate_id"},
		{"start", func(a *model.GlobalAggregate) { a.WindowStart = time.Time{} }, "window_start"},
		{"end order", func(a *model.GlobalAggregate) { a.WindowEnd = a.WindowStart }, "finestra"},
		{"expected", func(a *model.GlobalAggregate) { a.ExpectedEdges = 0 }, "attesi"},
		{"contributing zero", func(a *model.GlobalAggregate) { a.ContributingEdges = 0 }, "contribuenti"},
		{"contributing excess", func(a *model.GlobalAggregate) { a.ContributingEdges = 14 }, "maggiori"},
		{"events", func(a *model.GlobalAggregate) { a.Events = 0 }, "eventi"},
		{"emitted", func(a *model.GlobalAggregate) { a.EmittedAt = time.Time{} }, "emitted_at"},
		{"metric counts", func(a *model.GlobalAggregate) { a.Temperature.Invalid = 2 }, "valid"},
		{"metric average", func(a *model.GlobalAggregate) { wrong := 99.0; a.Temperature.Average = &wrong }, "average"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			err := ValidateGlobalAggregate(candidate)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("errore inatteso: %v", err)
			}
		})
	}
}

func newTestAggregator(
	t *testing.T,
	expected []string,
	sink GlobalAggregateSink,
) *Aggregator {
	t.Helper()
	clock := &testClock{current: globalTestTime(12, 0)}
	return newTestAggregatorWithClock(
		t,
		expected,
		5*time.Second,
		clock,
		sink,
	)
}

func newTestAggregatorWithClock(
	t *testing.T,
	expected []string,
	edgeIdleTimeout time.Duration,
	clock *testClock,
	sink GlobalAggregateSink,
) *Aggregator {
	t.Helper()
	aggregator, err := newAggregator(
		expected,
		15*time.Minute,
		15*time.Minute,
		edgeIdleTimeout,
		sink,
		clock.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return aggregator
}

type testClock struct {
	current time.Time
}

func (clock *testClock) Now() time.Time {
	return clock.current
}

func (clock *testClock) Advance(duration time.Duration) {
	clock.current = clock.current.Add(duration)
}

func collectSink(outputs *[]model.GlobalAggregate) GlobalAggregateSink {
	return func(_ context.Context, output model.GlobalAggregate) error {
		*outputs = append(*outputs, output)
		return nil
	}
}

func discardSink(context.Context, model.GlobalAggregate) error { return nil }

func testEdgeIDs(count int) []string {
	edgeIDs := make([]string, count)
	for index := range edgeIDs {
		edgeIDs[index] = fmt.Sprintf("edge-%d", index)
	}
	return edgeIDs
}

func testCloudAggregate(
	edgeID string,
	start time.Time,
	valid uint64,
	invalid uint64,
	sum float64,
	minimum float64,
	maximum float64,
) model.CloudEdgeAggregate {
	end := start.Add(15 * time.Minute)
	metric := testMetric(valid, invalid, sum, minimum, maximum)
	return model.CloudEdgeAggregate{
		SchemaVersion: model.CloudEdgeAggregateSchemaVersion,
		AggregateID: fmt.Sprintf(
			"cloud:%s:%s:%s",
			edgeID,
			start.Format(time.RFC3339),
			end.Format(time.RFC3339),
		),
		EdgeID:          edgeID,
		WindowStart:     start,
		WindowEnd:       end,
		InputAggregates: 1,
		Events:          valid + invalid,
		Temperature:     metric,
		Humidity:        metric,
		Pressure:        metric,
		EmittedAt:       globalTestTime(11, 0),
	}
}

func testMetric(
	valid uint64,
	invalid uint64,
	sum float64,
	minimum float64,
	maximum float64,
) model.MetricAggregate {
	if valid == 0 {
		return model.MetricAggregate{
			Invalid: invalid,
			Sum:     0,
		}
	}
	average := sum / float64(valid)
	return model.MetricAggregate{
		Valid:   valid,
		Invalid: invalid,
		Sum:     sum,
		Average: &average,
		Min:     &minimum,
		Max:     &maximum,
	}
}

func testEndOfReplay(edgeID string) model.EndOfReplay {
	return model.EndOfReplay{
		EdgeID:        edgeID,
		LastEventTime: globalTestTime(10, 14),
		EmittedAt:     globalTestTime(11, 0),
	}
}

func globalTestTime(hour int, minute int) time.Time {
	return time.Date(2025, time.January, 1, hour, minute, 0, 0, time.UTC)
}

func assertGlobalMetric(
	t *testing.T,
	actual model.MetricAggregate,
	valid uint64,
	invalid uint64,
	sum float64,
	minimum float64,
	maximum float64,
) {
	t.Helper()
	if actual.Valid != valid || actual.Invalid != invalid {
		t.Fatalf(
			"metric counts=(%d,%d), attesi=(%d,%d)",
			actual.Valid,
			actual.Invalid,
			valid,
			invalid,
		)
	}
	if math.Abs(actual.Sum-sum) > 1e-9 {
		t.Fatalf("Sum=%f, attesa %f", actual.Sum, sum)
	}
	if actual.Average == nil ||
		math.Abs(*actual.Average-sum/float64(valid)) > 1e-9 {
		t.Fatalf("Average=%v", actual.Average)
	}
	if actual.Min == nil || *actual.Min != minimum ||
		actual.Max == nil || *actual.Max != maximum {
		t.Fatalf("range=[%v,%v], atteso=[%f,%f]", actual.Min, actual.Max, minimum, maximum)
	}
}

func TestFastEdgeDoesNotCloseWindowWhileActiveEdgeIsBehind(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	clock := &testClock{current: globalTestTime(12, 0)}
	aggregator := newTestAggregatorWithClock(
		t,
		testEdgeIDs(2),
		5*time.Second,
		clock,
		collectSink(&outputs),
	)

	behind := globalTestTime(9, 45)
	target := globalTestTime(10, 0)
	ahead := globalTestTime(10, 30)
	for _, input := range []model.CloudEdgeAggregate{
		testCloudAggregate("edge-1", behind, 1, 0, 10, 10, 10),
		testCloudAggregate("edge-0", target, 1, 0, 20, 20, 20),
		testCloudAggregate("edge-0", ahead, 1, 0, 30, 30, 30),
	} {
		if err := aggregator.Add(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}

	if len(outputs) != 0 {
		t.Fatalf("Edge veloce ha chiuso finestre con Edge attivo indietro: %#v", outputs)
	}
}

func TestGlobalWatermarkIsMinimumOfActiveEdgeWatermarks(t *testing.T) {
	clock := &testClock{current: globalTestTime(12, 0)}
	aggregator := newTestAggregatorWithClock(
		t,
		testEdgeIDs(2),
		5*time.Second,
		clock,
		discardSink,
	)

	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 30), 1, 0, 10, 10, 10),
	); err != nil {
		t.Fatal(err)
	}
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-1", globalTestTime(10, 0), 1, 0, 10, 10, 10),
	); err != nil {
		t.Fatal(err)
	}

	want := globalTestTime(10, 0)
	if !aggregator.Watermark().Equal(want) {
		t.Fatalf("watermark=%s, atteso minimo %s", aggregator.Watermark(), want)
	}
}

func TestEdgeSkippingWindowAdvancesWatermarkWithLaterWindow(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	aggregator := newTestAggregator(t, testEdgeIDs(2), collectSink(&outputs))

	firstWindow := globalTestTime(10, 0)
	secondWindow := globalTestTime(10, 15)
	for _, input := range []model.CloudEdgeAggregate{
		testCloudAggregate("edge-0", firstWindow, 1, 0, 10, 10, 10),
		testCloudAggregate("edge-0", secondWindow, 1, 0, 20, 20, 20),
		testCloudAggregate("edge-1", secondWindow, 1, 0, 30, 30, 30),
	} {
		if err := aggregator.Add(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}

	if len(outputs) != 2 {
		t.Fatalf("output=%d, attesi finestra completa e finestra saltata", len(outputs))
	}
	var skipped *model.GlobalAggregate
	for index := range outputs {
		if outputs[index].WindowStart.Equal(firstWindow) {
			skipped = &outputs[index]
		}
	}
	if skipped == nil || skipped.ContributingEdges != 1 || skipped.ExpectedEdges != 2 {
		t.Fatalf("finestra saltata non emessa correttamente: %#v", skipped)
	}
}

func TestNeverSeenEdgeBlocksUntilIdleTimeout(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	clock := &testClock{current: globalTestTime(12, 0)}
	aggregator := newTestAggregatorWithClock(
		t,
		testEdgeIDs(2),
		5*time.Second,
		clock,
		collectSink(&outputs),
	)

	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 0), 1, 0, 10, 10, 10),
	); err != nil {
		t.Fatal(err)
	}
	clock.Advance(4 * time.Second)
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 30), 1, 0, 20, 20, 20),
	); err != nil {
		t.Fatal(err)
	}
	if !aggregator.Watermark().IsZero() || len(outputs) != 0 {
		t.Fatalf("Edge mai visto escluso prima del timeout: watermark=%s outputs=%d", aggregator.Watermark(), len(outputs))
	}

	clock.Advance(time.Second)
	if err := aggregator.AdvanceWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 1 || !outputs[0].WindowStart.Equal(globalTestTime(10, 0)) {
		t.Fatalf("finestra non chiusa dopo idle timeout: %#v", outputs)
	}
}

func TestSeenEdgeBecomesIdleAfterTimeout(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	clock := &testClock{current: globalTestTime(12, 0)}
	aggregator := newTestAggregatorWithClock(
		t,
		testEdgeIDs(2),
		5*time.Second,
		clock,
		collectSink(&outputs),
	)

	for _, input := range []model.CloudEdgeAggregate{
		testCloudAggregate("edge-1", globalTestTime(9, 45), 1, 0, 10, 10, 10),
		testCloudAggregate("edge-0", globalTestTime(10, 0), 1, 0, 20, 20, 20),
	} {
		if err := aggregator.Add(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(4 * time.Second)
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 30), 1, 0, 30, 30, 30),
	); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := aggregator.AdvanceWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(outputs) != 2 {
		t.Fatalf("finestre non chiuse dopo inattivita dell'Edge visto: %#v", outputs)
	}
}

func TestReturningIdleEdgeDoesNotMoveWatermarkBackward(t *testing.T) {
	clock := &testClock{current: globalTestTime(12, 0)}
	aggregator := newTestAggregatorWithClock(
		t,
		testEdgeIDs(2),
		5*time.Second,
		clock,
		discardSink,
	)

	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 30), 1, 0, 10, 10, 10),
	); err != nil {
		t.Fatal(err)
	}
	clock.Advance(4 * time.Second)
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 45), 1, 0, 20, 20, 20),
	); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := aggregator.AdvanceWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}
	beforeReturn := aggregator.Watermark()

	err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-1", globalTestTime(10, 15), 1, 0, 30, 30, 30),
	)
	if err != nil && !errors.Is(err, ErrClosedWindow) {
		t.Fatal(err)
	}
	candidate, available := aggregator.watermarkCandidate(clock.Now())
	if !available || !candidate.Equal(globalTestTime(10, 15)) {
		t.Fatalf("Edge rientrato non riattivato: candidate=%s available=%t", candidate, available)
	}
	if !aggregator.Watermark().Equal(beforeReturn) {
		t.Fatalf("watermark arretrato da %s a %s", beforeReturn, aggregator.Watermark())
	}
}

func TestAggregateForWatermarkClosedWindowIsLate(t *testing.T) {
	clock := &testClock{current: globalTestTime(12, 0)}
	aggregator := newTestAggregatorWithClock(
		t,
		testEdgeIDs(2),
		5*time.Second,
		clock,
		discardSink,
	)
	firstWindow := globalTestTime(10, 0)
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", firstWindow, 1, 0, 10, 10, 10),
	); err != nil {
		t.Fatal(err)
	}
	clock.Advance(4 * time.Second)
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 30), 1, 0, 20, 20, 20),
	); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := aggregator.AdvanceWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}

	err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-1", firstWindow, 1, 0, 30, 30, 30),
	)
	if !errors.Is(err, ErrClosedWindow) {
		t.Fatalf("atteso ErrClosedWindow, ottenuto %v", err)
	}
	if aggregator.LateAggregatesDropped() != 1 {
		t.Fatalf("atteso lateAggregatesDropped=1, ottenuto %d", aggregator.LateAggregatesDropped())
	}
}

func TestAllIdleEdgesKeepLastWatermark(t *testing.T) {
	clock := &testClock{current: globalTestTime(12, 0)}
	aggregator := newTestAggregatorWithClock(
		t,
		testEdgeIDs(2),
		5*time.Second,
		clock,
		discardSink,
	)
	for _, input := range []model.CloudEdgeAggregate{
		testCloudAggregate("edge-0", globalTestTime(10, 0), 1, 0, 10, 10, 10),
		testCloudAggregate("edge-1", globalTestTime(10, 15), 1, 0, 20, 20, 20),
	} {
		if err := aggregator.Add(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	want := aggregator.Watermark()
	if want.IsZero() {
		t.Fatal("watermark iniziale non avanzato")
	}

	clock.Advance(5 * time.Second)
	if err := aggregator.AdvanceWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !aggregator.Watermark().Equal(want) {
		t.Fatalf("tutti gli Edge idle hanno mosso il watermark da %s a %s", want, aggregator.Watermark())
	}
}

func TestEndOfReplayDoesNotAdvanceOnlineWatermark(t *testing.T) {
	outputs := make([]model.GlobalAggregate, 0)
	aggregator := newTestAggregator(t, testEdgeIDs(2), collectSink(&outputs))
	if err := aggregator.Add(
		context.Background(),
		testCloudAggregate("edge-0", globalTestTime(10, 0), 1, 0, 10, 10, 10),
	); err != nil {
		t.Fatal(err)
	}

	complete, err := aggregator.EndReplay(context.Background(), testEndOfReplay("edge-1"))
	if err != nil || complete || !aggregator.Watermark().IsZero() || len(outputs) != 0 {
		t.Fatalf("EOS ha influenzato il watermark: complete=%t watermark=%s outputs=%d err=%v", complete, aggregator.Watermark(), len(outputs), err)
	}
	complete, err = aggregator.EndReplay(context.Background(), testEndOfReplay("edge-0"))
	if err != nil || !complete || len(outputs) != 1 {
		t.Fatalf("flush EOS finale inatteso: complete=%t outputs=%d err=%v", complete, len(outputs), err)
	}
}
