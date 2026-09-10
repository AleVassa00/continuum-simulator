package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeOffsets struct {
	fetch func(*kafka.OffsetFetchResponse)
	ends  func(*kafka.ListOffsetsResponse)
	calls []string
}

func (f *fakeOffsets) OffsetFetch(_ context.Context, req *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error) {
	f.calls = append(f.calls, "commits:"+req.GroupID)
	res := &kafka.OffsetFetchResponse{Topics: make(map[string][]kafka.OffsetFetchPartition)}
	for topic, ids := range req.Topics {
		for _, id := range ids {
			res.Topics[topic] = append(res.Topics[topic], kafka.OffsetFetchPartition{Partition: id, CommittedOffset: 10})
		}
	}
	if f.fetch != nil {
		f.fetch(res)
	}
	return res, nil
}

func (f *fakeOffsets) ListOffsets(_ context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
	f.calls = append(f.calls, "ends")
	res := &kafka.ListOffsetsResponse{Topics: make(map[string][]kafka.PartitionOffsets)}
	for topic, ids := range req.Topics {
		for _, id := range ids {
			res.Topics[topic] = append(res.Topics[topic], kafka.PartitionOffsets{Partition: id.Partition, LastOffset: 13})
		}
	}
	if f.ends != nil {
		f.ends(res)
	}
	return res, nil
}

func TestSnapshot(t *testing.T) {
	client := &fakeOffsets{}
	rows, err := snapshot(context.Background(), client, targets[0])
	if err != nil || len(rows) != 6 {
		t.Fatalf("rows=%v error=%v", rows, err)
	}
	for p, row := range rows {
		if row.partition != p || row.committed != 10 || row.end != 13 {
			t.Fatalf("incorrect partition lag: %+v", row)
		}
	}
	if strings.Join(client.calls, ",") != "commits:cloud-workers,ends" {
		t.Fatalf("must read commits before log ends: %v", client.calls)
	}
	for _, scenario := range []struct {
		name string
		fake fakeOffsets
	}{
		{"unknown offset", fakeOffsets{fetch: func(r *kafka.OffsetFetchResponse) { r.Topics["edge-aggregates"][0].CommittedOffset = -1 }}},
		{"missing partition", fakeOffsets{fetch: func(r *kafka.OffsetFetchResponse) { r.Topics["edge-aggregates"] = r.Topics["edge-aggregates"][:5] }}},
		{"group error", fakeOffsets{fetch: func(r *kafka.OffsetFetchResponse) { r.Error = errors.New("unavailable") }}},
		{"partition error", fakeOffsets{ends: func(r *kafka.ListOffsetsResponse) { r.Topics["edge-aggregates"][0].Error = errors.New("unavailable") }}},
		{"end behind commit", fakeOffsets{ends: func(r *kafka.ListOffsetsResponse) { r.Topics["edge-aggregates"][0].LastOffset = 9 }}},
		{"missing log end", fakeOffsets{ends: func(r *kafka.ListOffsetsResponse) { delete(r.Topics, "edge-aggregates") }}},
		{"duplicate partition", fakeOffsets{ends: func(r *kafka.ListOffsetsResponse) { r.Topics["edge-aggregates"][0].Partition = 1 }}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			rows, err := snapshot(context.Background(), &scenario.fake, targets[0])
			if err == nil || rows != nil {
				t.Fatalf("invalid snapshot became data: rows=%v error=%v", rows, err)
			}
		})
	}
}

func TestOnceFormatAndFailure(t *testing.T) {
	var out bytes.Buffer
	err := collect(context.Background(), &fakeOffsets{}, &out, time.Second, true)
	if err != nil || strings.Count(out.String(), "===== query_exit 0 at ") != 2 {
		t.Fatalf("%v: %s", err, &out)
	}
	if !strings.Contains(out.String(), "cloud-workers edge-aggregates 5 10 13 3 - - -\n") ||
		!strings.Contains(out.String(), "global-aggregator cloud-partition-aggregates 0 10 13 3 - - -\n") {
		t.Fatalf("incompatible lag rows: %s", &out)
	}
	out.Reset()
	err = collect(context.Background(), &fakeOffsets{fetch: func(r *kafka.OffsetFetchResponse) {
		r.Error = errors.New("broker unavailable")
	}}, &out, time.Second, true)
	if err == nil || strings.Count(out.String(), "===== query_exit 1 at ") != 2 || strings.Contains(out.String(), " - - -") {
		t.Fatalf("failure must not fabricate zero lag: %v: %s", err, &out)
	}
}

func TestLoopRecoversAndStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var out bytes.Buffer
	calls := 0
	client := &fakeOffsets{fetch: func(r *kafka.OffsetFetchResponse) {
		calls++
		if calls <= 2 {
			r.Error = errors.New("temporary error")
		}
		if calls == 4 {
			cancel()
		}
	}}
	if err := collect(ctx, client, &out, time.Millisecond, false); err != nil {
		t.Fatal(err)
	}
	if calls != 4 || strings.Count(out.String(), "===== query_exit 1 at ") != 2 || strings.Count(out.String(), "===== query_exit 0 at ") != 2 {
		t.Fatalf("collector must retry with the same client and stop: %s", &out)
	}
}
