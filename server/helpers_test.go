package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var SMALL_DELAY time.Duration = 10 * time.Millisecond

func setupTestServer(t *testing.T) (*Manager, string, func()) {
	t.Helper()
	manager := NewManager()
	server := httptest.NewServer(http.HandlerFunc(manager.HandleWSUpgradeRequest))
	wsURL := strings.Replace(server.URL, "http", "ws", 1)
	cleanup := func() {
		server.Close()
	}

	return manager, wsURL, cleanup
}

func pollEvent(t *testing.T, delay time.Duration, retries int, callback func() bool) bool {
	t.Helper()
	for range retries {
		if callback() {
			return true
		}
		time.Sleep(delay)
	}
	return false
}
