package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
)

const queryTimeout = 3 * time.Second

type offsetClient interface {
	OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
	ListOffsets(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
}

type target struct {
	group, topic string
	partitions   int
}

var targets = []target{
	{"cloud-workers", "edge-aggregates", 6},
	{"global-aggregator", "cloud-partition-aggregates", 1},
}

type lagRow struct {
	partition      int
	committed, end int64
}

// Fetch commits BEFORE log ends: a commit advancing between requests must not
// be compared with an older log end. This is still not an atomic snapshot.
func snapshot(ctx context.Context, client offsetClient, t target) ([]lagRow, error) {
	ids := make([]int, t.partitions)
	ends := make([]kafka.OffsetRequest, t.partitions)
	for p := range ids {
		ids[p] = p
		ends[p] = kafka.LastOffsetOf(p)
	}
	commits, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: t.group, Topics: map[string][]int{t.topic: ids}})
	if err != nil {
		return nil, err
	}
	if commits == nil {
		return nil, fmt.Errorf("missing OffsetFetch response")
	}
	if commits.Error != nil {
		return nil, commits.Error
	}
	rows := make([]lagRow, t.partitions)
	seen := make(map[int]bool, t.partitions)
	for _, p := range commits.Topics[t.topic] {
		if p.Error != nil || p.Partition < 0 || p.Partition >= t.partitions || seen[p.Partition] || p.CommittedOffset < 0 {
			return nil, fmt.Errorf("unavailable/invalid committed offset for %s partition %d", t.group, p.Partition)
		}
		seen[p.Partition] = true
		rows[p.Partition] = lagRow{partition: p.Partition, committed: p.CommittedOffset}
	}
	if len(seen) != t.partitions {
		return nil, fmt.Errorf("incomplete committed offsets for %s", t.group)
	}
	last, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{t.topic: ends}})
	if err != nil {
		return nil, err
	}
	if last == nil {
		return nil, fmt.Errorf("missing ListOffsets response")
	}
	clear(seen)
	for _, p := range last.Topics[t.topic] {
		if p.Error != nil || p.Partition < 0 || p.Partition >= t.partitions || seen[p.Partition] || p.LastOffset < rows[p.Partition].committed {
			return nil, fmt.Errorf("unavailable/invalid log end for %s partition %d", t.topic, p.Partition)
		}
		seen[p.Partition] = true
		rows[p.Partition].end = p.LastOffset
	}
	if len(seen) != t.partitions {
		return nil, fmt.Errorf("incomplete log ends for %s", t.topic)
	}
	return rows, nil
}

// Retains the existing parser's complete-query envelope and six numeric columns.
// No partial rows or fabricated zeros are emitted when a query fails.
func sample(ctx context.Context, client offsetClient, out io.Writer) bool {
	success := true
	for _, t := range targets {
		if ctx.Err() != nil {
			return false
		}
		fmt.Fprintf(out, "===== group %s sample %s =====\n", t.group, time.Now().UTC().Format(time.RFC3339Nano))
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		rows, err := snapshot(queryCtx, client, t)
		cancel()
		code := 0
		if err != nil {
			success = false
			code = 1
			fmt.Fprintf(out, "# query failed: %v\n", err)
		} else {
			for _, row := range rows {
				fmt.Fprintf(out, "%s %s %d %d %d %d - - -\n", t.group, t.topic, row.partition, row.committed, row.end, row.end-row.committed)
			}
		}
		fmt.Fprintf(out, "===== query_exit %d at %s =====\n", code, time.Now().UTC().Format(time.RFC3339Nano))
	}
	return success
}

func collect(ctx context.Context, client offsetClient, out io.Writer, interval time.Duration, once bool) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		ok := sample(ctx, client, out)
		if once {
			if !ok {
				return fmt.Errorf("Kafka lag snapshot incomplete; see query errors")
			}
			return nil
		}
		// A slow query does not create concurrent samplers or a backlog of polls.
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
