package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

const readinessAddress = ":8080"

type ReadinessState struct {
	ready atomic.Bool
}

func (
	state *ReadinessState,
) MarkReady() {
	state.ready.Store(true)
}

func (
	state *ReadinessState,
) MarkNotReady() {
	state.ready.Store(false)
}

func (
	state *ReadinessState,
) IsReady() bool {
	return state.ready.Load()
}

func (
	state *ReadinessState,
) ServeHTTP(
	response http.ResponseWriter,
	request *http.Request,
) {
	response.Header().Set(
		"Content-Type",
		"text/plain; charset=utf-8",
	)

	if !state.IsReady() {
		response.WriteHeader(http.StatusServiceUnavailable)
		_, _ = response.Write([]byte("not ready\n"))

		return
	}

	response.WriteHeader(http.StatusOK)
	_, _ = response.Write([]byte("ready\n"))
}

func startReadinessServer(
	readiness *ReadinessState,
	edgeID string,
) (*http.Server, error) {
	listener, err := net.Listen(
		"tcp",
		readinessAddress,
	)
	if err != nil {
		return nil,
			fmt.Errorf(
				"%s: avvio readiness server su %s fallito: %w",
				edgeID,
				readinessAddress,
				err,
			)
	}

	mux := http.NewServeMux()
	mux.Handle(
		"GET /readyz",
		readiness,
	)

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      2 * time.Second,
		IdleTimeout:       5 * time.Second,
	}

	go func() {
		err := server.Serve(listener)
		if err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			fmt.Printf(
				"%s: readiness server terminato: %v\n",
				edgeID,
				err,
			)
		}
	}()

	return server, nil
}

func stopReadinessServer(
	server *http.Server,
	edgeID string,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf(
			"%s: arresto readiness server fallito: %v\n",
			edgeID,
			err,
		)
	}
}
