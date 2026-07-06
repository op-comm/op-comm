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
func ConnectMultipleToServer(t *testing.T, manager *server.Manager, wsURL string, count int) []*websocket.Conn {
	t.Helper()
	if count <= 0 {
		t.Fatalf("Invalid count for ConnectMultipleToServer func, expected a positive integer")
		return []*websocket.Conn{}
	}

	connectionList := make([]*websocket.Conn, count)
	for i := range count {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, wsURL, nil)
		if err != nil {
			t.Fatalf("Unexpected error when dialing server: %v", err)
		}

		connectionList[i] = conn
	}
	return connectionList
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

func ConnectToServer(t *testing.T, manager *server.Manager, wsURL string) (*websocket.Conn, *server.Session) {
	t.Helper()
	ctx := context.Background()
	clientConn, response, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("Unexpected Server Error: Failed to connect to ws: %v", err)
	}

	sessionID := response.Header.Get("Op-Comm-Session-ID")
	managerCreatedSession := PollEvent(t, SMALL_DELAY, 10, func() bool {
		return manager.GetSession(sessionID) != nil

	})

	if !managerCreatedSession {
		t.Fatal("Server failed to create session in time")
	}

	serverSession := manager.GetSession(sessionID)
	return clientConn, serverSession
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
