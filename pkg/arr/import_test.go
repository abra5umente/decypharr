package arr

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-arr-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestImportUsesSingleRequests(t *testing.T) {
	var gets atomic.Int32
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/manualimport":
			gets.Add(1)
			if got := r.URL.Query().Get("downloadId"); got != "release-1" {
				t.Errorf("downloadId = %q, want release-1", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `[{
				"path":"/downloads/release/episode.mkv",
				"folderName":"release",
				"series":{"id":10},
				"seasonNumber":1,
				"episodes":[{"id":20}],
				"quality":{"quality":{"id":1,"name":"HDTV-720p","source":"television","resolution":720},"revision":{"version":1}},
				"languages":[],"customFormats":[],"rejections":[]
			}]`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/command":
			posts.Add(1)
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	a := New("sonarr", server.URL, "token", false, nil, "", "manual")
	if err := a.Import("release-1"); err != nil {
		t.Fatal(err)
	}
	if got := gets.Load(); got != 1 {
		t.Fatalf("manual-import scans = %d, want 1", got)
	}
	if got := posts.Load(); got != 1 {
		t.Fatalf("manual-import commands = %d, want 1", got)
	}
}

func TestImportDoesNotRetryServerError(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	a := New("sonarr", server.URL, "token", false, nil, "", "manual")
	if err := a.Import("release-1"); err == nil {
		t.Fatal("Import returned nil error for a 503 response")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("Import made %d requests after a 503, want exactly 1", got)
	}
}

func TestManualImportGuardSpacesAndDeduplicates(t *testing.T) {
	g := newManualImportGuard()
	now := time.Date(2026, time.September, 21, 12, 0, 0, 0, time.UTC)

	if ok, _ := g.reserve("release-1", now); !ok {
		t.Fatal("first import was not reserved")
	}
	if ok, wait := g.reserve("release-2", now.Add(time.Minute)); ok || wait != 4*time.Minute {
		t.Fatalf("second release during global cooldown: ok=%v wait=%v", ok, wait)
	}
	if ok, wait := g.reserve("release-1", now.Add(manualImportGlobalCooldown)); ok || wait != 25*time.Minute {
		t.Fatalf("same release during download cooldown: ok=%v wait=%v", ok, wait)
	}
	if ok, _ := g.reserve("release-2", now.Add(manualImportGlobalCooldown)); !ok {
		t.Fatal("different release was not allowed after global cooldown")
	}
}

func TestManualImportItemsCooldownPreventsDuplicateRequest(t *testing.T) {
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `[{"path":"/downloads/e.mkv","series":{"id":1},"episodes":[{"id":2}],"quality":{"quality":{"id":1}},"languages":[],"customFormats":[],"rejections":[]}]`)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	a := New("sonarr", server.URL, "token", false, nil, "", "manual")
	items := map[string]bool{"release-1": true}
	if err := a.ManualImportItems(items); err != nil {
		t.Fatal(err)
	}
	if err := a.ManualImportItems(items); err != nil {
		t.Fatal(err)
	}
	if got := gets.Load(); got != 1 {
		t.Fatalf("manual-import scans = %d, want 1 after immediate repeat", got)
	}
}

func TestMonitorCoalescesOverlappingCleanup(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/queue" {
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"page":1,"pageSize":200,"totalRecords":0,"records":[]}`)
	}))
	defer server.Close()

	storage := NewStorage()
	storage.AddOrUpdate(New("sonarr", server.URL, "token", false, nil, "", "manual"))

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		storage.Monitor()
		close(done1)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first cleanup did not start")
	}
	go func() {
		storage.Monitor()
		close(done2)
	}()

	// Give the second monitor time to join the singleflight call before the
	// first request is released.
	time.Sleep(25 * time.Millisecond)
	close(release)
	for i, done := range []chan struct{}{done1, done2} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("monitor %d did not finish", i+1)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("overlapping monitors made %d queue requests, want 1", got)
	}
}
