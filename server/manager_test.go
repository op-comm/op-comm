package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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

func TestManager_DeniesUnauthorizedRequest(t *testing.T) {

	manager, wsURL, cleanup := setupTestServer(t)
	manager.SetAuthenticator(rejectAllAuthenticator{})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, response, err := websocket.Dial(ctx, wsURL, nil)

	if err == nil {
		t.Fatal("expected request to fail, but it succeeded")
	}

	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Expected 401 response status, got %v", response.Status)
	}

}

func TestManager_AcceptsAuthorizedRequest(t *testing.T) {

	manager, wsURL, cleanup := setupTestServer(t)
	manager.SetAuthenticator(acceptAllAuthenticator{})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, response, err := websocket.Dial(ctx, wsURL, nil)

	if err != nil {
		t.Fatalf("expected no error from request: got %v", err)
	}

	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("Expected 101 response status, got %v", response.Status)
	}

	managerCreatedSession := pollEvent(t, SMALL_DELAY, 5, func() bool {
		return manager.sessionCount() >= 1
	})
	if !managerCreatedSession {
		t.Fatal("expected session to be created")
	}

}

func TestManager_AcceptsWhenNoAuthenticator(t *testing.T) {

	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, response, err := websocket.Dial(ctx, wsURL, nil)

	if err != nil {
		t.Fatalf("expected no error from request: got %v", err)
	}

	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("Expected 101 response status, got %v", response.Status)
	}

	managerCreatedSession := pollEvent(t, SMALL_DELAY, 5, func() bool {
		return manager.sessionCount() >= 1
	})
	if !managerCreatedSession {
		t.Fatal("expected session to be created")
	}

}

type rejectAllAuthenticator struct{}

func (_ rejectAllAuthenticator) Authenticate(_ *http.Request) (map[string]any, error) {
	return nil, errors.New("Rejected")
}

type acceptAllAuthenticator struct{}

func (_ acceptAllAuthenticator) Authenticate(_ *http.Request) (map[string]any, error) {
	return nil, nil
}

func TestManager_MiddlewareAllowsEvent(t *testing.T) {
	middlewareRan := false
	eventRan := false
	allowMiddleWare := func(event *protocol.ClientSentEvent, session *Session) bool {
		middlewareRan = true
		return true
	}

	event := func(event *protocol.ClientSentEvent, session *Session) {
		eventRan = true
	}

	manager := NewManager()

	manager.On("test_event", event)
	manager.UseMiddleware(allowMiddleWare)

	manager.handleEvent(sessionEventWrapper{
		session: &Session{},
		event: &protocol.ClientSentEvent{
			EventType: "test_event",
		},
	})

	if !middlewareRan {
		t.Fatal("Middleware did not run when expected to")
	}

	if !eventRan {
		t.Fatal("Event did not run when expected to")
	}

}

func TestManager_MiddlewareDeniesEvent(t *testing.T) {
	middlewareRan := false
	eventRan := false
	allowMiddleWare := func(event *protocol.ClientSentEvent, session *Session) bool {
		middlewareRan = true
		return false
	}

	event := func(event *protocol.ClientSentEvent, session *Session) {
		eventRan = true
	}

	manager := NewManager()

	manager.On("test_event", event)
	manager.UseMiddleware(allowMiddleWare)

	manager.handleEvent(sessionEventWrapper{
		session: &Session{},
		event: &protocol.ClientSentEvent{
			EventType: "test_event",
		},
	})

	if !middlewareRan {
		t.Fatal("Middleware did not run when expected to.")
	}

	if eventRan {
		t.Fatal("Event ran when not expected to.")
	}

}
