package server

import (
	"context"
	"testing"

	"github.com/coder/websocket"
)

func TestSession_Close(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()
	ctx := context.Background()
	connection, _, wsErr := websocket.Dial(ctx, wsURL, nil)
	if wsErr != nil {
		t.Fatal("Unexpected Server Error: Failed to connect to ws")
	}

	var session *Session
	managerCreatedSession := pollEvent(t, SMALL_DELAY, 10, func() bool {
		manager.sessionMutex.RLock()
		defer manager.sessionMutex.RUnlock()
		for _, managerSession := range manager.sessions {
			session = managerSession
			return true
		}
		return false
	})

	if !managerCreatedSession {
		t.Fatal("Server failed to create session")
	}

	go session.Close(websocket.StatusNormalClosure, "session closed")

	_, _, readErr := connection.Read(ctx)
	if readErr == nil {
		t.Fatal("Expected websocket read to fail")
	}

	if websocket.CloseStatus(readErr) != websocket.StatusNormalClosure {
		t.Fatalf("Expected normal closure, got: %v", readErr)
	}

}
