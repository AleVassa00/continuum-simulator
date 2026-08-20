package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"continuum/internal/config"
)

func main() {
	configPath := flag.String("config", "config/project.yml", "path to the project YAML")
	workerID := flag.String("worker-id", os.Getenv("CONTINUUM_WORKER_ID"), "worker identity used only in logs")
	flag.Parse()
	if *workerID == "" {
		*workerID = "worker-local"
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"component=cloud-worker status=configured worker_id=%s group=%s topic=%s partitions=%d aggregate_window=%s storage=%s\n",
		*workerID,
		cfg.Cloud.ConsumerGroup,
		cfg.Transport.EdgeToCloud.Topic,
		cfg.Transport.EdgeToCloud.Partitions,
		cfg.Processing.Cloud.WindowSize.Duration,
		cfg.Cloud.Storage.Driver,
	)
}
