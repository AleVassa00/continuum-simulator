package globalaggregator

import (
	"context"
	"errors"
	"fmt"
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
}

func New(
	expectedEdgeIDs []string,
	windowSize time.Duration,
	watermarkDelay time.Duration,
	edgeIdleTimeout time.Duration,
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
	if edgeIdleTimeout <= 0 {
		return nil, fmt.Errorf(
			"GLOBAL_EDGE_IDLE_TIMEOUT deve essere maggiore di zero",
		)
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
	}, nil
}

func (aggregator *Aggregator) LateAggregatesDropped() uint64 {
	return aggregator.lateAggregatesDropped
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
	state := aggregator.windows[key]
	if state != nil {
		if aggregateID, found := state.contributors[input.EdgeID]; found {
			// Un retry dello stesso contributo non rappresenta nuova attivita.
			if aggregateID == input.AggregateID {
				return nil
			}
			return fmt.Errorf(
				"violazione strutturale: edge=%s ha contribuito piu volte alla finestra globale [%s,%s)",
				input.EdgeID,
				state.start.Format(time.RFC3339),
				state.end.Format(time.RFC3339),
			)
		}
	}

	now := time.Now().UTC()
	if aggregator.firstAggregateAt.IsZero() {
		aggregator.firstAggregateAt = now
	}
	// L'attivita e processing-time: anche un record late segnala che l'Edge e tornato attivo.
	aggregator.lastActivityByEdge[input.EdgeID] = now

	// Il record e late soltanto rispetto al watermark valido prima del suo arrivo.
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

	if state == nil {
		state = &windowState{
			start:        input.WindowStart.UTC(),
			end:          input.WindowEnd.UTC(),
			contributors: make(map[string]string),
		}
		aggregator.windows[key] = state
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

func (aggregator *Aggregator) EndReplay(
	ctx context.Context,
	edgeID string,
) (bool, error) {
	if strings.TrimSpace(edgeID) == "" {
		return false, fmt.Errorf("edge_id EOS mancante")
	}
	if _, found := aggregator.expectedEdges[edgeID]; !found {
		return false, fmt.Errorf("EndOfReplay da Edge non atteso %q", edgeID)
	}
	if _, duplicate := aggregator.endedEdges[edgeID]; duplicate {
		return aggregator.complete, nil
	}
	if aggregator.complete {
		return true, nil
	}

	// EOS e solo controllo: non aggiorna attivita, progresso temporale o watermark.
	aggregator.endedEdges[edgeID] = struct{}{}
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

func (aggregator *Aggregator) emit(
	ctx context.Context,
	state *windowState,
) error {
	output := state.buildAggregate(
		uint64(len(aggregator.expectedEdges)),
		time.Now().UTC(),
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
