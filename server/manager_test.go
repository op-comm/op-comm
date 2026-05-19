package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
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

//TODO: test data lifecycle through manager (from inboundbuffer to socket)

// TODO: add more session management test

//TODO: add stress test?

func TestManager_HandlesCustomEvent(t *testing.T) {
	manager, _, cleanup := setupTestServer(t)
	defer cleanup()

	expectedToBeTrueAfterEvent := false
	manager.On("toggle", func(event *protocol.ClientSentEvent, session *Session) {
		expectedToBeTrueAfterEvent = true
	})

	data, err := json.Marshal("my custom data")
	if err != nil {
		t.Fatalf("Failed to marshal json")
	}
	_, cancel := context.WithCancel(context.Background())
	session := NewSession("123", nil, manager, cancel)
	manager.handleEvent(sessionEventWrapper{
		event:   &protocol.ClientSentEvent{EventType: "toggle", Data: data},
		session: session,
	})

	if !expectedToBeTrueAfterEvent {
		t.Fatalf("Expected custom event to be ran")
	}
}

func TestManager_HandlesCustomService(t *testing.T) {
	manager, _, cleanup := setupTestServer(t)
	defer cleanup()

	expectedToBeTrueAfterEvent := false

	customService := EventServicFunc(func(action string, event *protocol.ClientSentEvent, session *Session) {
		if action == "toggle" {
			expectedToBeTrueAfterEvent = true
		}
	})

	manager.RegisterEventService("bool", customService)

	data, err := json.Marshal("my custom data")
	if err != nil {
		t.Fatalf("Failed to marshal json")
	}
	_, cancel := context.WithCancel(context.Background())
	session := NewSession("123", nil, manager, cancel)
	manager.handleEvent(sessionEventWrapper{
		event:   &protocol.ClientSentEvent{EventType: "bool:toggle", Data: data},
		session: session,
	})

	if !expectedToBeTrueAfterEvent {
		t.Fatalf("Expected custom event service to be ran")
	}
}
