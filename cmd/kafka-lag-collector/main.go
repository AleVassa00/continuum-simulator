// kafka-lag-collector observes committed offsets without joining consumer groups.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := flag.String("broker", "", "Kafka bootstrap address (host:port)")
	interval := flag.Duration("interval", 5*time.Second, "sampling interval")
	mode := flag.String("mode", "loop", "loop or once")
	flag.Parse()
	if *broker == "" || *interval <= 0 || (*mode != "loop" && *mode != "once") || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "require -broker, positive -interval and -mode loop|once")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	transport := &kafka.Transport{ClientID: "continuum-lag-collector"}
	client := &kafka.Client{Addr: kafka.TCP(*broker), Transport: transport, Timeout: queryTimeout}
	err := collect(ctx, client, os.Stdout, *interval, *mode == "once")
	transport.CloseIdleConnections()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
