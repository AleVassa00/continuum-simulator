package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"continuum/internal/adapters/csvtrace"
	"continuum/internal/adapters/mqtttransport"
	"continuum/internal/config"
	"continuum/internal/replay"
	"continuum/internal/topology"
)

func main() {

	configPath := flag.String("config", "config/project.yml", "path to the project YAML")
	maxEvents := flag.Uint64("max-events", 0, "maximum events to publish; zero means the full trace")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	index, err := topology.Load(cfg.Dataset.TopologyFile)
	if err != nil {
		log.Fatal(err)
	}

	replayStatus := "ready"

	_, err = os.Stat(cfg.Dataset.ReplayFile)

	if err != nil {
		if os.IsNotExist(err) {
			replayStatus = "pending-generation"
		} else {
			log.Fatal(err)
		}
	}

	fmt.Printf(
		"component=simulator status=configured dataset=%s replay=%s sensors=%d edges=%d macroareas=%d speedup=%.0fx broker=%s\n",
		cfg.Dataset.Name,
		replayStatus,
		index.SensorCount(),
		index.EdgeCount(),
		index.MacroareaCount(),
		cfg.Experiment.ReplaySpeedup,
		cfg.Transport.SimulatorToEdge.BrokerURL,
	)
	if replayStatus != "ready" {
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader, err := csvtrace.Open(cfg.Dataset.ReplayFile, cfg.Dataset)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	pacer, err := replay.NewTimelinePacer(cfg.Experiment.ReplaySpeedup)
	if err != nil {
		log.Fatal(err)
	}
	publisher, err := mqtttransport.NewPublisher(
		ctx,
		cfg.Transport.SimulatorToEdge,
		"continuum-simulator",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("close MQTT publisher: %v", err)
		}
	}()

	service, err := replay.NewService(reader, index, pacer, publisher)
	if err != nil {
		log.Fatal(err)
	}
	if err := service.RunN(ctx, *maxEvents); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("component=simulator status=completed event_limit=%d\n", *maxEvents)
}
