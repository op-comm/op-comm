package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestManager_WSRequest(t *testing.T) {

	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	connection, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "")

	//byteInput := []byte(`{"type": "chat", "data": {}}`)

	expectedSessionCount := 1
	var actualSessionCount int
	sessionCountTestPassed := pollEvent(t, 50*time.Millisecond, 10, func() bool {
		actualSessionCount = manager.sessionCount()
		return expectedSessionCount == actualSessionCount
	})

	if !sessionCountTestPassed {
		t.Fatalf("Expected manager to have %v session(s), manager has %v", expectedSessionCount, actualSessionCount)
	}
}

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
