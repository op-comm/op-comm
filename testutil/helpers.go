package testutil

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/server"
)

var SMALL_DELAY time.Duration = 10 * time.Millisecond
var TEST_WRITE_TIMEOUT time.Duration = 5 * time.Second
var TEST_READ_TIMEOUT time.Duration = 5 * time.Second

// creates a server with manager.HandleWSUpgradeRequest HandlerFunc
func SetupTestServer(t *testing.T) (*server.Manager, string, func()) {
	t.Helper()
	manager := server.NewManager()
	if testing.Verbose() {
		opts := &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}
		manager.SetLogger(slog.New(slog.NewTextHandler(os.Stdout, opts)))
	}
	server := httptest.NewServer(http.HandlerFunc(manager.HandleWSUpgradeRequest))
	wsURL := strings.Replace(server.URL, "http", "ws", 1)
	cleanup := func() {
		server.Close()
	}

	return manager, wsURL, cleanup
}

func ConnectToServer(t *testing.T, manager *server.Manager, wsURL string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("Unexpected error when dialing server: %v", err)
	}
	return conn

}

func WriteToConnection(t *testing.T, conn *websocket.Conn, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writeErr := conn.Write(ctx, websocket.MessageText, data)
	if writeErr != nil {
		t.Fatalf("Failed to write to socket: %v", writeErr)
	}
}

func PollEvent(t *testing.T, delay time.Duration, retries int, callback func() bool) bool {
	t.Helper()
	for range retries {
		if callback() {
			return true
		}
		time.Sleep(delay)
	}
	return false
}
