package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"continuum/internal/envutil"
	"continuum/internal/partitioncompletion"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	broker := envutil.Required("KAFKA_BROKER")
	topic := envutil.OrDefault("KAFKA_TOPIC", "edge-aggregates")
	ids := strings.Split(envutil.Required("COORDINATOR_EXPECTED_EDGE_IDS"), ",")
	for i := range ids {
		ids[i] = strings.TrimSpace(ids[i])
	}
	if err := validateSourceTopic(ctx, broker, topic); err != nil {
		return err
	}
	publish, closeWriters := sourceEndPublisher(broker, topic)
	defer closeWriters()
	c, err := partitioncompletion.New(ctx, ids, publish)
	if err != nil {
		return err
	}
	defer func() { cancel(); c.Wait() }()
	server := &http.Server{Addr: ":8081", Handler: coordinatorHandler(c), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	defer func() {
		shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
		defer finish()
		server.Shutdown(shutdown)
	}()
	fmt.Printf("PARTITION_COORDINATOR_READY producers=%d\n", len(ids))
	select {
	case <-ctx.Done():
		return nil
	case err := <-c.Errors():
		return err
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func coordinatorHandler(c *partitioncompletion.Coordinator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		_, failed := c.Status()
		if failed {
			http.Error(w, "coordinator failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		complete, failed := c.Status()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Complete bool `json:"complete"`
			Failed   bool `json:"failed"`
		}{complete, failed})
	})
	mux.HandleFunc("POST /start", func(w http.ResponseWriter, r *http.Request) {
		if err := c.Start(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /completed", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var completion struct {
			EdgeID string `json:"edge_id"`
		}
		if err := decoder.Decode(&completion); err != nil {
			http.Error(w, "invalid completion", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "invalid trailing completion data", http.StatusBadRequest)
			return
		}
		if err := c.Complete(completion.EdgeID); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Registration is acknowledged without waiting for another partition's IO.
		// Publisher failures are fatal to the coordinator and checked by the runner.
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}
