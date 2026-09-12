package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"continuum/internal/partitioncompletion"
)

func TestControlEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c, err := partitioncompletion.New(ctx, []string{"edge-0"}, func(context.Context, int) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); c.Wait() }()
	h := coordinatorHandler(c)
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/readyz", "", 200},
		{"POST", "/completed", `{"edge_id":"edge-0"}`, 400},
		{"POST", "/start", "", 204},
		{"POST", "/start", "", 204},
		{"POST", "/completed", `{"edge_id":"wrong"}`, 400},
		{"POST", "/completed", `{"edge_id":"edge-0","extra":1}`, 400},
		{"POST", "/completed", `{"edge_id":"edge-0"} {}`, 400},
		{"POST", "/completed", `{"edge_id":"edge-0"}`, 204},
		{"POST", "/completed", `{"edge_id":"edge-0"}`, 204},
		{"GET", "/completed", "", http.StatusMethodNotAllowed},
		{"GET", "/status", "", 200},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.status {
			t.Fatalf("%s %s %s: got %d, want %d: %s", tc.method, tc.path, tc.body, w.Code, tc.status, w.Body.String())
		}
	}
}
