package main

import (
	"context"
	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
)

// Partition readers fetch concurrently; one processing loop per worker keeps
// computation serial within the worker, with distinct state for each assignment
func consume(ctx context.Context, config CloudWorkerConfig, publish KafkaMessagePublisher) (result error) {

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID: config.GroupID, Brokers: []string{config.KafkaBroker}, Topics: []string{config.InputTopic},
		StartOffset: kafka.FirstOffset, WatchPartitionChanges: true, Timeout: operationTimeout,
	})
	if err != nil {
		return err
	}
	failures := make(chan error, 1)
	defer func() {
		// Close waits for generation callbacks, including their final offset flush.
		group.Close()
		select {
		case failure := <-failures:
			if result == nil {
				result = failure
			}
		default:
		}
	}()

	stopClose := context.AfterFunc(ctx, func() { group.Close() })
	defer stopClose()

	var processed atomic.Bool
	for {
		gen, err := group.Next(ctx)
		select {
		case failure := <-failures:
			return failure
		default:
		}

		if ctx.Err() != nil {
			return nil
		}
		if err != nil {

			// A fresh broker may still be initializing __consumer_offsets. Retrying
			// group setup is safe only before any source input has been processed.
			if !processed.Load() && (errors.Is(err, kafka.RebalanceInProgress) || errors.Is(err, kafka.GroupCoordinatorNotAvailable)) {
				continue
			}
			return err
		}
		if processed.Load() {
			return fmt.Errorf("consumer group rebalance after processing: volatile state cannot migrate; restart the entire run")
		}
		if err := validateKafkaTopology(ctx, config.KafkaBroker, config.InputTopic, config.SourcePartitionCount); err != nil {
			return err
		}

		for _, assignment := range gen.Assignments[config.InputTopic] {
			if assignment.Offset > 0 {
				return fmt.Errorf("partition %d already has committed offset %d: fresh run required", assignment.ID, assignment.Offset)
			}
		}
		fmt.Printf("CLOUD_ASSIGNMENT worker=%s generation=%d partitions=%v\n", config.WorkerID, gen.ID, gen.Assignments[config.InputTopic])
		gen.Start(func(genCtx context.Context) {
			err := consumeGeneration(genCtx, gen, config, publish, &processed)
			if err != nil && (genCtx.Err() == nil || errors.Is(err, errOffsetCommit)) {
				select {
				case failures <- err:
				default:
				}
			}
		})
	}
}

func consumeGeneration(ctx context.Context, gen *kafka.Generation, config CloudWorkerConfig, publish KafkaMessagePublisher, processed *atomic.Bool) error {
	commits, err := newOffsetCommitBatch(config.InputTopic, config.ConsumerCommitBatchSize, gen.CommitOffsets)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	messages := make(chan kafka.Message)
	readErrors := make(chan error, config.SourcePartitionCount)
	processors := make(map[int]*CloudMessageProcessor)

	var readers sync.WaitGroup

	defer func() {
		cancel()
		readers.Wait()
	}()

	for _, assignment := range gen.Assignments[config.InputTopic] {

		a, err := cloudworker.NewPartitionAggregator(assignment.ID, config.SourcePartitionCount, config.Membership[assignment.ID], config.WindowSize)
		if err != nil {
			return err
		}

		p := &CloudMessageProcessor{aggregator: a, workerID: config.WorkerID, outputTopic: config.OutputTopic, publishMessage: publish}
		processors[assignment.ID] = p
		if err := p.publishOutput(ctx, a.Initialize()); err != nil {
			return fmt.Errorf("initialize source partition %d: %w", assignment.ID, err)
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{config.KafkaBroker}, Topic: config.InputTopic, Partition: assignment.ID,
			MinBytes: 1, MaxBytes: 10 * 1024 * 1024,
		})

		if err := reader.SetOffset(assignment.Offset); err != nil {
			reader.Close()
			return err
		}
		readers.Add(1)
		go func() {
			defer readers.Done()
			defer reader.Close()
			for {
				msg, err := reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() == nil {
						select {
						case readErrors <- err:
						case <-ctx.Done():
						}
					}
					return
				}
				select {
				case messages <- msg:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			// Flush only successfully processed inputs, using the generation's
			// connection before its callback returns. No wall-clock commit policy.
			return commits.flush()
		case err := <-readErrors:
			return err
		case msg := <-messages:
			processed.Store(true)
			p := processors[msg.Partition]
			if p == nil {
				return fmt.Errorf("record from unassigned partition %d", msg.Partition)
			}
			commit := commits.add
			kind, err := kafkautil.ParseRecordType(msg.Headers)
			if err != nil {
				return fmt.Errorf("worker=%s partition=%d offset=%d: %w", config.WorkerID, msg.Partition, msg.Offset, err)
			}
			if kind == model.RecordTypeEdgeEndOfInput {
				commit = commits.addAndFlush
			}
			if err := processAndCommitMessage(ctx, msg, p, commit); err != nil {
				return fmt.Errorf("worker=%s partition=%d offset=%d: %w", config.WorkerID, msg.Partition, msg.Offset, err)
			}
		}
	}
}

func processAndCommitMessage(ctx context.Context, msg kafka.Message, p *CloudMessageProcessor, commit func(kafka.Message) error) error {
	if err := p.Process(ctx, msg); err != nil {
		return err
	}
	return commit(msg)
}
