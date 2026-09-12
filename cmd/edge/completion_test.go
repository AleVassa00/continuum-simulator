package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCompletionRetriesOnlyControlNotification(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost {
			t.Error("not POST")
		}
		if calls == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	if err := notifyEdgeCompletion(context.Background(), server.URL, "edge-0"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestSimulatorEOSFlushesDataWithoutKafkaEdgeEOS(t *testing.T) {
	for _, withData := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "final-window"}[withData], func(t *testing.T) {
			ingress := newEdgeIngress(2)
			ingress.RegisterEndOfReplay()
			a := &WindowAggregator{edgeID: "edge-0", windowSize: 5 * time.Minute}
			if withData {
				_, err := a.Add("e", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), EdgeMeasurement{})
				if err != nil {
					t.Fatal(err)
				}
			}
			out := make(chan EdgeOutputRecord, 2)
			stats := &EdgeStats{}
			if err := runEdgeLoop(ingress, a, out, make(chan struct{}), stats); err != nil {
				t.Fatal(err)
			}
			count := 0
			for record := range out {
				count++
				if record.Kind != EdgeOutputAggregate {
					t.Fatal("nondata Kafka output")
				}
			}
			want := 0
			if withData {
				want = 1
			}
			if count != want {
				t.Fatalf("records %d want %d", count, want)
			}
			if stats.endOfReplayProcessed.Load() != 1 {
				t.Fatal("Simulator EOS not recorded")
			}
		})
	}
}
