package replay

import (
	"context"
	"testing"
	"time"
)

func TestTimelinePacerDoesNotDelayEqualTimestamps(t *testing.T) {
	pacer, err := NewTimelinePacer(1)
	if err != nil {
		t.Fatal(err)
	}
	eventTime := time.Date(2025, 1, 1, 1, 11, 22, 0, time.UTC)
	if err := pacer.Wait(context.Background(), eventTime); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := pacer.Wait(context.Background(), eventTime); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("equal timestamp introduced delay %s", elapsed)
	}
}

func TestTimelinePacerRejectsUnorderedTrace(t *testing.T) {
	pacer, err := NewTimelinePacer(1_000)
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2025, 1, 1, 1, 11, 23, 0, time.UTC)
	if err := pacer.Wait(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := pacer.Wait(context.Background(), first.Add(-time.Second)); err == nil {
		t.Fatal("unordered trace was accepted")
	}
}
