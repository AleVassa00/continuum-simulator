package globalaggregator

import (
	"context"
	"time"
)

func (aggregator *Aggregator) Watermark() time.Time {
	return aggregator.watermark
}

func (aggregator *Aggregator) AdvanceWatermark(ctx context.Context) error {
	if aggregator.complete {
		return nil
	}

	return aggregator.advanceWatermarkAt(ctx, time.Now().UTC())
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
