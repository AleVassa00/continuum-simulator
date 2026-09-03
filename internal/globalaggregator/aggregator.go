package globalaggregator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/model"
)

type GlobalAggregateSink func(
	context.Context,
	model.GlobalAggregate,
) error

var ErrClosedWindow = errors.New("finestra globale gia chiusa")

type windowKey struct {
	start int64
	end   int64
}

type metricState struct {
	valid   uint64
	invalid uint64
	sum     float64
	min     float64
	max     float64
}

type windowState struct {
	start time.Time
	end   time.Time

	contributors map[string]struct{}
	events       uint64

	temperature metricState
	humidity    metricState
	pressure    metricState
}

type Aggregator struct {
	expectedEdges   map[string]struct{}
	endedEdges      map[string]struct{}
	windowSize      time.Duration
	watermarkDelay  time.Duration
	edgeIdleTimeout time.Duration

	maxWindowEndByEdge map[string]time.Time
	lastActivityByEdge map[string]time.Time
	firstAggregateAt   time.Time
	watermark          time.Time

	windows       map[windowKey]*windowState
	closedWindows map[windowKey]struct{}
	complete      bool

	lateAggregatesDropped uint64

	sink GlobalAggregateSink
	now  func() time.Time
}

func New(
	expectedEdgeIDs []string,
	windowSize time.Duration,
	watermarkDelay time.Duration,
	edgeIdleTimeout time.Duration,
	sink GlobalAggregateSink,
) (*Aggregator, error) {
	return newAggregator(
		expectedEdgeIDs,
		windowSize,
		watermarkDelay,
		edgeIdleTimeout,
		sink,
		time.Now,
	)
}

func newAggregator(
	expectedEdgeIDs []string,
	windowSize time.Duration,
	watermarkDelay time.Duration,
	edgeIdleTimeout time.Duration,
	sink GlobalAggregateSink,
	now func() time.Time,
) (*Aggregator, error) {
	if len(expectedEdgeIDs) == 0 {
		return nil, fmt.Errorf("EXPECTED_EDGE_IDS non puo essere vuota")
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf(
			"GLOBAL_WINDOW_SIZE deve essere maggiore di zero",
		)
	}
	if watermarkDelay <= 0 {
		watermarkDelay = windowSize
	}
	if edgeIdleTimeout <= 0 {
		return nil, fmt.Errorf(
			"GLOBAL_EDGE_IDLE_TIMEOUT deve essere maggiore di zero",
		)
	}
	if sink == nil {
		return nil, fmt.Errorf("GlobalAggregate sink non configurato")
	}
	if now == nil {
		return nil, fmt.Errorf("clock Global Aggregator non configurato")
	}

	expected := make(map[string]struct{}, len(expectedEdgeIDs))
	for _, rawEdgeID := range expectedEdgeIDs {
		edgeID := strings.TrimSpace(rawEdgeID)
		if edgeID == "" {
			return nil, fmt.Errorf(
				"EXPECTED_EDGE_IDS contiene un edge_id vuoto",
			)
		}
		if _, found := expected[edgeID]; found {
			return nil, fmt.Errorf(
				"EXPECTED_EDGE_IDS contiene edge_id duplicato %q",
				edgeID,
			)
		}
		expected[edgeID] = struct{}{}
	}
	return &Aggregator{
		expectedEdges:      expected,
		endedEdges:         make(map[string]struct{}, len(expected)),
		windowSize:         windowSize,
		watermarkDelay:     watermarkDelay,
		edgeIdleTimeout:    edgeIdleTimeout,
		maxWindowEndByEdge: make(map[string]time.Time, len(expected)),
		lastActivityByEdge: make(map[string]time.Time, len(expected)),
		windows:            make(map[windowKey]*windowState),
		closedWindows:      make(map[windowKey]struct{}),
		sink:               sink,
		now:                now,
	}, nil
}

func (aggregator *Aggregator) LateAggregatesDropped() uint64 {
	return aggregator.lateAggregatesDropped
}

func (aggregator *Aggregator) Watermark() time.Time {
	return aggregator.watermark
}

func (aggregator *Aggregator) Add(
	ctx context.Context,
	input model.CloudEdgeAggregate,
) error {
	if err := cloudworker.ValidateCloudEdgeAggregate(input); err != nil {
		return fmt.Errorf(
			"CloudEdgeAggregate %q non valido: %w",
			input.AggregateID,
			err,
		)
	}
	if _, found := aggregator.expectedEdges[input.EdgeID]; !found {
		return fmt.Errorf("CloudEdgeAggregate da Edge non atteso %q", input.EdgeID)
	}
	if aggregator.complete {
		return fmt.Errorf(
			"CloudEdgeAggregate %q ricevuto dopo completamento globale",
			input.AggregateID,
		)
	}
	if _, ended := aggregator.endedEdges[input.EdgeID]; ended {
		return fmt.Errorf(
			"CloudEdgeAggregate %q ricevuto dopo EndOfReplay edge=%s",
			input.AggregateID,
			input.EdgeID,
		)
	}
	if err := aggregator.validateWindow(input); err != nil {
		return err
	}

	now := aggregator.now().UTC()
	if aggregator.firstAggregateAt.IsZero() {
		aggregator.firstAggregateAt = now
	}
	// L'attivita e processing-time: anche un record late segnala che l'Edge e tornato attivo.
	aggregator.lastActivityByEdge[input.EdgeID] = now

	// Il record e late soltanto rispetto al watermark valido prima del suo arrivo.
	key := makeWindowKey(input.WindowStart, input.WindowEnd)
	_, explicitlyClosed := aggregator.closedWindows[key]
	closedByWatermark := !aggregator.watermark.IsZero() &&
		!input.WindowEnd.After(aggregator.watermark)
	if explicitlyClosed || closedByWatermark {
		aggregator.lateAggregatesDropped++
		return fmt.Errorf(
			"CloudEdgeAggregate %q tenta di riaprire la finestra globale gia chiusa [%s,%s): %w",
			input.AggregateID,
			input.WindowStart.Format(time.RFC3339),
			input.WindowEnd.Format(time.RFC3339),
			ErrClosedWindow,
		)
	}

	state := aggregator.windows[key]
	if state == nil {
		state = &windowState{
			start:        input.WindowStart.UTC(),
			end:          input.WindowEnd.UTC(),
			contributors: make(map[string]struct{}),
		}
		aggregator.windows[key] = state
	}
	if _, duplicate := state.contributors[input.EdgeID]; duplicate {
		return fmt.Errorf(
			"violazione strutturale: edge=%s ha contribuito piu volte alla finestra globale [%s,%s)",
			input.EdgeID,
			state.start.Format(time.RFC3339),
			state.end.Format(time.RFC3339),
		)
	}

	if input.WindowEnd.After(aggregator.maxWindowEndByEdge[input.EdgeID]) {
		aggregator.maxWindowEndByEdge[input.EdgeID] = input.WindowEnd.UTC()
	}

	state.add(input)

	// 1. Fast path: se tutti gli Edge attesi sono arrivati per questa finestra, emetti subito.
	if len(state.contributors) == len(aggregator.expectedEdges) {
		if err := aggregator.emit(ctx, state); err != nil {
			return err
		}
		delete(aggregator.windows, key)
		aggregator.closedWindows[key] = struct{}{}
	}

	// 2. Watermark trigger: chiudi finestre aperte per cui Watermark >= WindowEnd.
	return aggregator.advanceWatermarkAt(ctx, now)
}

func (aggregator *Aggregator) AdvanceWatermark(ctx context.Context) error {
	if aggregator.complete {
		return nil
	}

	return aggregator.advanceWatermarkAt(ctx, aggregator.now().UTC())
}

func (aggregator *Aggregator) advanceWatermarkAt(
	ctx context.Context,
	now time.Time,
) error {
	candidate, available := aggregator.watermarkCandidate(now)
	if available && candidate.After(aggregator.watermark) {
		aggregator.watermark = candidate
	}

	if aggregator.watermark.IsZero() {
		return nil
	}
	for _, key := range aggregator.sortedOpenWindowKeys() {
		state := aggregator.windows[key]
		if state == nil || aggregator.watermark.Before(state.end) {
			continue
		}
		if err := aggregator.emit(ctx, state); err != nil {
			return err
		}
		delete(aggregator.windows, key)
		aggregator.closedWindows[key] = struct{}{}
	}

	return nil
}

func (aggregator *Aggregator) watermarkCandidate(
	now time.Time,
) (time.Time, bool) {
	if aggregator.firstAggregateAt.IsZero() {
		return time.Time{}, false
	}

	var candidate time.Time
	activeEdges := 0
	startupGraceElapsed := now.Sub(aggregator.firstAggregateAt) >=
		aggregator.edgeIdleTimeout

	for edgeID := range aggregator.expectedEdges {
		lastActivity, seen := aggregator.lastActivityByEdge[edgeID]
		if !seen {
			if !startupGraceElapsed {
				return time.Time{}, false
			}
			continue
		}

		if now.Sub(lastActivity) >= aggregator.edgeIdleTimeout {
			continue
		}

		activeEdges++
		maxWindowEnd := aggregator.maxWindowEndByEdge[edgeID]
		if maxWindowEnd.IsZero() {
			return time.Time{}, false
		}
		edgeWatermark := maxWindowEnd.Add(-aggregator.watermarkDelay)
		if candidate.IsZero() || edgeWatermark.Before(candidate) {
			candidate = edgeWatermark
		}
	}

	return candidate, activeEdges > 0
}

func (aggregator *Aggregator) EndReplay(
	ctx context.Context,
	record model.EndOfReplay,
) (bool, error) {
	if err := model.ValidateEndOfReplay(record); err != nil {
		return false, err
	}
	if _, found := aggregator.expectedEdges[record.EdgeID]; !found {
		return false, fmt.Errorf("EndOfReplay da Edge non atteso %q", record.EdgeID)
	}
	if _, duplicate := aggregator.endedEdges[record.EdgeID]; duplicate {
		return aggregator.complete, nil
	}
	if aggregator.complete {
		return true, nil
	}

	aggregator.endedEdges[record.EdgeID] = struct{}{}
	if len(aggregator.endedEdges) != len(aggregator.expectedEdges) {
		return false, nil
	}

	keys := aggregator.sortedOpenWindowKeys()
	for _, key := range keys {
		if err := aggregator.emit(ctx, aggregator.windows[key]); err != nil {
			return false, fmt.Errorf(
				"flush finestra globale [%s,%s) fallito: %w",
				aggregator.windows[key].start.Format(time.RFC3339),
				aggregator.windows[key].end.Format(time.RFC3339),
				err,
			)
		}
	}
	for _, key := range keys {
		delete(aggregator.windows, key)
		aggregator.closedWindows[key] = struct{}{}
	}
	aggregator.complete = true

	return true, nil
}

func (aggregator *Aggregator) IsComplete() bool {
	return aggregator.complete
}

func (aggregator *Aggregator) validateWindow(
	input model.CloudEdgeAggregate,
) error {
	duration := input.WindowEnd.Sub(input.WindowStart)
	if duration != aggregator.windowSize {
		return fmt.Errorf(
			"CloudEdgeAggregate %q ha finestra %s, attesa GLOBAL_WINDOW_SIZE %s",
			input.AggregateID,
			duration,
			aggregator.windowSize,
		)
	}
	start := input.WindowStart.UTC()
	if !start.Equal(start.Truncate(aggregator.windowSize)) {
		return fmt.Errorf(
			"CloudEdgeAggregate %q non allineato a GLOBAL_WINDOW_SIZE %s",
			input.AggregateID,
			aggregator.windowSize,
		)
	}
	return nil
}

func (aggregator *Aggregator) emit(
	ctx context.Context,
	state *windowState,
) error {
	output := state.buildAggregate(
		uint64(len(aggregator.expectedEdges)),
		aggregator.now().UTC(),
	)
	if err := ValidateGlobalAggregate(output); err != nil {
		return fmt.Errorf(
			"GlobalAggregate %q non valido: %w",
			output.AggregateID,
			err,
		)
	}
	if err := aggregator.sink(ctx, output); err != nil {
		return fmt.Errorf(
			"sink GlobalAggregate %q fallito: %w",
			output.AggregateID,
			err,
		)
	}
	return nil
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

func (state *windowState) add(input model.CloudEdgeAggregate) {
	state.contributors[input.EdgeID] = struct{}{}
	state.events += input.Events
	state.temperature.add(input.Temperature)
	state.humidity.add(input.Humidity)
	state.pressure.add(input.Pressure)
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
	expectedEdges uint64,
	emittedAt time.Time,
) model.GlobalAggregate {
	return model.GlobalAggregate{
		SchemaVersion: model.GlobalAggregateSchemaVersion,
		AggregateID:   buildGlobalAggregateID(state.start, state.end),
		WindowStart:   state.start,
		WindowEnd:     state.end,
		ExpectedEdges: expectedEdges,
		ContributingEdges: uint64(
			len(state.contributors),
		),
		Events:      state.events,
		Temperature: state.temperature.buildAggregate(),
		Humidity:    state.humidity.buildAggregate(),
		Pressure:    state.pressure.buildAggregate(),
		EmittedAt:   emittedAt,
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
		start.UTC().Format(time.RFC3339),
		end.UTC().Format(time.RFC3339),
	)
}
