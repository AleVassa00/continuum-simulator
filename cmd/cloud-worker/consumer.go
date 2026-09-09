package main

import (
	"context"
	"continuum/internal/cloudworker"
	"continuum/internal/model"
	"fmt"
	"github.com/segmentio/kafka-go"
	"sync"
	"sync/atomic"
)

// Partition readers fetch concurrently; one processing loop per worker keeps
// computation serial within the worker, with distinct state for each assignment.
func consume(ctx context.Context, config CloudWorkerConfig, publish KafkaMessagePublisher) error {
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID: config.GroupID, Brokers: []string{config.KafkaBroker}, Topics: []string{config.InputTopic},
		StartOffset: kafka.FirstOffset, WatchPartitionChanges: true, Timeout: operationTimeout,
	})
	if err != nil {
		return err
	}
	defer group.Close()
	stopClose := context.AfterFunc(ctx, func() { group.Close() })
	defer stopClose()
	failures := make(chan error, 1)
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
			return err
		}
		if processed.Load() {
			return fmt.Errorf("consumer group rebalance after processing: volatile state cannot migrate; restart the entire run")
		}
		if err := validateKafkaTopology(ctx, config.KafkaBroker, config.InputTopic, model.SourcePartitionCount); err != nil {
			return err
		}
		// No recovery from already-committed inputs without their corresponding state.
		for _, assignment := range gen.Assignments[config.InputTopic] {
			if assignment.Offset > 0 {
				return fmt.Errorf("partition %d already has committed offset %d: fresh run required", assignment.ID, assignment.Offset)
			}
		}
		fmt.Printf("CLOUD_ASSIGNMENT worker=%s generation=%d partitions=%v\n", config.WorkerID, gen.ID, gen.Assignments[config.InputTopic])
		gen.Start(func(genCtx context.Context) {
			err := consumeGeneration(genCtx, gen, config, publish, &processed)
			if err != nil && genCtx.Err() == nil {
				select {
				case failures <- err:
				default:
				}
			}
		})
	}
}

func consumeGeneration(ctx context.Context, gen *kafka.Generation, config CloudWorkerConfig, publish KafkaMessagePublisher, processed *atomic.Bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	messages := make(chan kafka.Message)
	readErrors := make(chan error, model.SourcePartitionCount)
	processors := make(map[int]*CloudMessageProcessor)
	var readers sync.WaitGroup
	// Cancel fetchers and join them before releasing generation ownership.
	defer func() { cancel(); readers.Wait() }()
	for _, assignment := range gen.Assignments[config.InputTopic] {
		a, err := cloudworker.NewPartitionAggregator(assignment.ID, config.Membership[assignment.ID], config.WindowSize)
		if err != nil {
			return err
		}
		p := &CloudMessageProcessor{aggregator: a, workerID: config.WorkerID, outputTopic: config.OutputTopic, publishMessage: publish}
		processors[assignment.ID] = p
		if err := p.Initialize(ctx); err != nil {
			return err
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
	// Remain in the group after local EOS; exiting would rebalance other workers.
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-readErrors:
			return err
		case msg := <-messages:
			processed.Store(true)
			p := processors[msg.Partition]
			if p == nil {
				return fmt.Errorf("record from unassigned partition %d", msg.Partition)
			}
			if err := processAndCommitMessage(ctx, msg, p, func(m kafka.Message) error {
				// Generation.CommitOffsets takes the NEXT offset (unlike CommitMessages).
				return gen.CommitOffsets(map[string]map[int]int64{config.InputTopic: {m.Partition: m.Offset + 1}})
			}); err != nil {
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
