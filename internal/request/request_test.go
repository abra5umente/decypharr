package request

import (
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-request-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestDoOnceDoesNotRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "try again", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := New(WithMaxRetries(5))
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.DoOnce(req)
	if err != nil {
		t.Fatal(err)
	}
	DrainAndCloseResponse(resp)

	if got := calls.Load(); got != 1 {
		t.Fatalf("DoOnce made %d requests, want exactly 1", got)
	}
}
