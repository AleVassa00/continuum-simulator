package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"continuum/internal/adapters/mqtttransport"
	"continuum/internal/config"
	"continuum/internal/domain"
	edgeservice "continuum/internal/edge"
	"continuum/internal/topology"
)

func main() {
	configPath := flag.String("config", "config/project.yml", "path to the project YAML")
	nodeID := flag.String("node-id", os.Getenv("CONTINUUM_NODE_ID"), "logical Edge ID")
	brokerURL := flag.String(
		"mqtt-broker",
		os.Getenv("CONTINUUM_MQTT_BROKER_URL"),
		"MQTT broker URL used by this Edge",
	)
	flag.Parse()
	if *nodeID == "" {
		log.Fatal("node-id or CONTINUUM_NODE_ID is required")
	}

	if *brokerURL == "" {
		log.Fatal(
			"mqtt-broker or CONTINUUM_MQTT_BROKER_URL is required",
		)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	index, err := topology.Load(cfg.Dataset.TopologyFile)
	if err != nil {
		log.Fatal(err)
	}
	edge, found := index.Edge(*nodeID)
	if !found {
		log.Fatalf("edge %q is not present in %s", *nodeID, cfg.Dataset.TopologyFile)
	}

	fmt.Printf(
		"component=edge status=configured node_id=%s macroarea_id=%s sensors=%d window=%s mqtt_broker=%s upstream=%s\n",
		edge.ID, edge.MacroareaID, edge.SensorCount, cfg.Processing.Edge.WindowSize.Duration,
		*brokerURL, cfg.Transport.EdgeToCloud.Topic,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ingest, err := edgeservice.NewIngestService(
		edge.ID,
		edge.MacroareaID,
		index,
		func(event domain.SensorEvent, accepted uint64) {
			if accepted == 1 {
				fmt.Printf(
					"component=edge event=accepted node_id=%s sensor_id=%s event_id=%s observed_at=%s measurements=%v accepted=%d\n",
					edge.ID,
					event.SensorID,
					event.EventID,
					event.EventTime.Format("2006-01-02T15:04:05Z07:00"),
					event.Measurements,
					accepted,
				)
			} else if accepted%1_000 == 0 {
				fmt.Printf(
					"component=edge event=progress node_id=%s accepted=%d\n",
					edge.ID,
					accepted,
				)
			}
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	subscriber, err := mqtttransport.NewSubscriber(
		ctx,
		cfg.Transport.SimulatorToEdge,
		*brokerURL,
		"continuum-edge-"+edge.ID,
		edge.ID,
		ingest,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := subscriber.Close(); err != nil {
			log.Printf("close MQTT subscriber: %v", err)
		}
	}()

	fmt.Printf("component=edge status=listening node_id=%s\n", edge.ID)
	<-ctx.Done()
	fmt.Printf(
		"component=edge status=stopped node_id=%s accepted=%d\n",
		edge.ID,
		ingest.AcceptedCount(),
	)
}
