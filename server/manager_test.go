package server

import (
	"context"
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
