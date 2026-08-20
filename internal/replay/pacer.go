package replay

import (
	"context"
	"fmt"
	"time"
)

// TimelinePacer maps the CSV event-time timeline onto an accelerated wall-clock
// timeline. Deadlines are always relative to the first event, so publication
// time does not accumulate sleep drift. Equal timestamps have equal deadlines.
type TimelinePacer struct {
	speedup       float64
	replayStart   time.Time
	traceStart    time.Time
	lastEventTime time.Time
}

func NewTimelinePacer(speedup float64) (*TimelinePacer, error) {
	if speedup <= 0 {
		return nil, fmt.Errorf("replay speedup must be positive")
	}
	return &TimelinePacer{speedup: speedup}, nil
}

func (p *TimelinePacer) Wait(ctx context.Context, eventTime time.Time) error {
	if eventTime.IsZero() {
		return fmt.Errorf("event time must not be zero")
	}

	if p.traceStart.IsZero() {
		p.traceStart = eventTime
		p.lastEventTime = eventTime
		p.replayStart = time.Now()
		return nil
	}

	if eventTime.Before(p.lastEventTime) {
		return fmt.Errorf(
			"replay trace is not ordered: %s precedes %s",
			eventTime.Format(time.RFC3339Nano),
			p.lastEventTime.Format(time.RFC3339Nano),
		)
	}
	p.lastEventTime = eventTime

	traceOffset := eventTime.Sub(p.traceStart)
	replayOffset := time.Duration(float64(traceOffset) / p.speedup)
	deadline := p.replayStart.Add(replayOffset)
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
