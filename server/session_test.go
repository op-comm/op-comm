package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)

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

func TestSession_ForwardsDataToManager(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	connection, _ := connectAndFetchSession(t, manager, wsURL)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)
	clientEvent := protocol.ClientSentEvent{
		EventType: "test:data",
		Data: []byte(`"Data"`),
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
			case receivedData = <- manager.InboundBuffer:
				return true
			default:
				return false
		}
	})
	if !dataIsInBuffer{
		t.Fatalf("Expected data to be forwarded to Manager's InboundBuffer, but it never reached")
	}

	if receivedData.event.EventType != "test:data" {
		t.Fatal("Expected recieved event to match sent event, mismatched EventType")
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