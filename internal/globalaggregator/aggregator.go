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
	expectedEdges  map[string]struct{}
	endedEdges     map[string]struct{}
	windowSize     time.Duration
	watermarkDelay time.Duration

	maxEventTime time.Time

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
	sink GlobalAggregateSink,
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
	if sink == nil {
		return nil, fmt.Errorf("GlobalAggregate sink non configurato")
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
		expectedEdges:  expected,
		endedEdges:     make(map[string]struct{}, len(expected)),
		windowSize:     windowSize,
		watermarkDelay: watermarkDelay,
		windows:        make(map[windowKey]*windowState),
		closedWindows:  make(map[windowKey]struct{}),
		sink:           sink,
		now:            time.Now,
	}, nil
}

func (aggregator *Aggregator) LateAggregatesDropped() uint64 {
	return aggregator.lateAggregatesDropped
}

func (aggregator *Aggregator) Watermark() time.Time {
	if aggregator.maxEventTime.IsZero() {
		return time.Time{}
	}
	return aggregator.maxEventTime.Add(-(aggregator.watermarkDelay + aggregator.windowSize))
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

	key := makeWindowKey(input.WindowStart, input.WindowEnd)
	if _, closed := aggregator.closedWindows[key]; closed {
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

	state.add(input)

	if input.WindowEnd.After(aggregator.maxEventTime) {
		aggregator.maxEventTime = input.WindowEnd
	}

	// 1. Fast path: se tutti i 13 Edge sono arrivati per questa finestra, emetti subito
	if len(state.contributors) == len(aggregator.expectedEdges) {
		if err := aggregator.emit(ctx, state); err != nil {
			return err
		}
		delete(aggregator.windows, key)
		aggregator.closedWindows[key] = struct{}{}
	}

	// 2. Watermark trigger: chiudi finestre aperte per cui Watermark >= WindowEnd
	watermark := aggregator.Watermark()
	if !watermark.IsZero() {
		for _, wKey := range aggregator.sortedOpenWindowKeys() {
			wState := aggregator.windows[wKey]
			if wState == nil {
				continue
			}
			if !watermark.Before(wState.end) {
				if err := aggregator.emit(ctx, wState); err != nil {
					return err
				}
				delete(aggregator.windows, wKey)
				aggregator.closedWindows[wKey] = struct{}{}
			}
		}
	}

	return nil
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
