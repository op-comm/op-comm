package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)

var TEST_WRITE_TIMEOUT time.Duration = 5 * time.Second
var TEST_READ_TIMEOUT time.Duration = 5 * time.Second

func TestSession_Close(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()
	ctx := context.Background()
	connection, session := connectAndFetchSession(t, manager, wsURL)
	go session.Close(websocket.StatusNormalClosure, "session closed")

	_, _, readErr := connection.Read(ctx)
	if readErr == nil {
		t.Fatal("Expected websocket read to fail")
	}

	if websocket.CloseStatus(readErr) != websocket.StatusNormalClosure {
		t.Fatalf("Expected normal closure, got: %v", readErr)
	}

}

func TestSession_ReadPumpForwardsDataToManager(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	connection, _ := connectAndFetchSession(t, manager, wsURL)
	defer connection.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), TEST_WRITE_TIMEOUT)

	expectedType := "test:data"
	clientEvent := protocol.ClientSentEvent{
		EventType: expectedType,
		Data:      []byte(`"Data"`),
	}
	data, marshalErr := json.Marshal(clientEvent)
	if marshalErr != nil {
		t.Fatalf("Unexpected Test error: Failed to marshal json: %v", marshalErr)
	}
	connection.Write(ctx, websocket.MessageText, data)
	cancel()

	var receivedData sessionEventWrapper
	dataIsInBuffer := pollEvent(t, SMALL_DELAY, 10, func() bool {
		select {
		case receivedData = <-manager.InboundBuffer:
			return true
		default:
			return false
		}
	})
	if !dataIsInBuffer {
		t.Fatalf("Expected data to be forwarded to Manager's InboundBuffer, but it never reached")
	}

	if receivedData.event.EventType != expectedType {
		t.Fatalf("Expected eventType to be %v, got %v", expectedType, receivedData.event.EventType)
	}
}

func TestSession_WritePumpSendsDataToClient(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	connection, session := connectAndFetchSession(t, manager, wsURL)
	defer connection.Close(websocket.StatusNormalClosure, "")

	expectedType := "test:data"
	serverEvent := protocol.ServerSentEvent{
		EventType: expectedType,
		Data:      []byte(`"Data"`),
	}
	session.OutputBuffer <- serverEvent

	readCtx, cancel := context.WithTimeout(context.Background(), TEST_READ_TIMEOUT)
	defer cancel()
	_, data, readErr := connection.Read(readCtx)

	if readErr != nil {
		t.Fatalf("Unexpected Read Error: expected successful read operation")
	}

	var event protocol.ServerSentEvent
	jsonErr := json.Unmarshal(data, &event)

	if jsonErr != nil {
		t.Fatalf("Unexpected JSON Error: expected json.Unmarshal to have no errors")
	}

	if event.EventType != expectedType {
		t.Fatalf("Expected eventType to be %v, got %v", expectedType, event.EventType)
	}
}

func TestSession_IsRemovedFromManagerOnSocketClose(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()
	connection, session := connectAndFetchSession(t, manager, wsURL)
	connection.Close(websocket.StatusNormalClosure, "")

	managerDeletedSession := pollEvent(t, SMALL_DELAY, 10, func() bool {
		manager.sessionMutex.RLock()
		defer manager.sessionMutex.RUnlock()
		_, exists := manager.sessions[session.ID]
		return !exists
	})

	if !managerDeletedSession {
		t.Fatalf("Expected session to be removed from session map of manager. Session was not removed.")
	}

}

func TestSession_IgnoresInvalidJSON(t *testing.T) {

	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()
	connection, session := connectAndFetchSession(t, manager, wsURL)
	defer connection.Close(websocket.StatusNormalClosure, "")

	inputData, jsonErr := json.Marshal(`"Data"`)
	if jsonErr != nil {
		t.Fatalf("Unexpected json Marshal error: %v", jsonErr)
	}

	writeCtx, cancel := context.WithTimeout(context.Background(), TEST_WRITE_TIMEOUT)
	connection.Write(writeCtx, websocket.MessageText, inputData)
	cancel()

	dataReachedManagerBuffer := pollEvent(t, SMALL_DELAY, 10, func() bool {
		select {
		case <-manager.InboundBuffer:
			return true
		default:
			return false
		}
	})

	if dataReachedManagerBuffer {
		t.Fatal("Session Error: Invalid Data reached manager.")
	}

	//  this is checked to ensure user is not kicked on invalid data.
	managerDeletedSession := pollEvent(t, SMALL_DELAY, 10, func() bool {
		manager.sessionMutex.RLock()
		defer manager.sessionMutex.RUnlock()
		_, exists := manager.sessions[session.ID]
		return !exists
	})

	if managerDeletedSession {
		t.Fatalf("Expected session to not be removed from session map of manager. Session was removed.")
	}

}

func connectAndFetchSession(t *testing.T, manager *Manager, wsURL string) (*websocket.Conn, *Session) {
	ctx := context.Background()
	clientConn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("Unexpected Server Error: Failed to connect to ws: %v", err)
	}

	var serverSession *Session
	managerCreatedSession := pollEvent(t, SMALL_DELAY, 10, func() bool {
		manager.sessionMutex.RLock()
		defer manager.sessionMutex.RUnlock()
		for _, s := range manager.sessions {
			serverSession = s
			return true
		}
		return false
	})

	if !managerCreatedSession {
		t.Fatal("Server failed to create session in time")
	}

	return clientConn, serverSession
}
