package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/server"
	"github.com/op-comm/op-comm/testutil"
)

func TestSession_Close(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), testutil.TEST_READ_TIMEOUT)
	defer cancel()
	clientConnection, session := testutil.ConnectToServer(t, manager, wsURL)
	go session.Close(websocket.StatusNormalClosure, "session closed")

	_, _, readErr := clientConnection.Read(ctx)
	if readErr == nil {
		t.Fatal("Expected websocket read to fail")
	}

	if websocket.CloseStatus(readErr) != websocket.StatusNormalClosure {
		t.Fatalf("Expected normal closure, got: %v", readErr)
	}

}

func TestSession_ReadPumpForwardsDataToManager(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()
	clientConnection, _ := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	expectedType := "test:data"
	byteData := []byte(fmt.Sprintf(`{"type": %q}`, expectedType))
	testutil.WriteToConnection(t, clientConnection, byteData)

	select {
	case receivedData := <-manager.InboundBuffer:
		if receivedData.Event.Type != expectedType {
			t.Fatalf("Expected eventType to be %v, got %v", expectedType, receivedData.Event.Type)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Expected manager's Inbound buffer to contain written data")
	}
}

func TestSession_WritePumpSendsDataToSocket(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	clientConnection, session := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	expectedType := "test:data"
	serverEvent := protocol.Response{
		Type: expectedType,
	}
	session.Send(serverEvent)

	readCtx, cancel := context.WithTimeout(context.Background(), testutil.TEST_READ_TIMEOUT)
	defer cancel()
	_, data, readErr := clientConnection.Read(readCtx)

	if readErr != nil {
		t.Fatalf("Unexpected Read Error: expected successful read operation")
	}

	var event protocol.Response
	jsonErr := json.Unmarshal(data, &event)

	if jsonErr != nil {
		t.Fatalf("Unexpected JSON Error: expected json.Unmarshal to have no errors")
	}

	if event.Type != expectedType {
		t.Fatalf("Expected eventType to be %v, got %v", expectedType, event.Type)
	}
}

func TestSession_IsRemovedFromManagerOnSocketClose(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()
	clientConnection, session := testutil.ConnectToServer(t, manager, wsURL)
	clientConnection.Close(websocket.StatusNormalClosure, "")

	managerDeletedSession := testutil.PollEvent(t, testutil.SMALL_DELAY, 10, func() bool {
		sessionMutex := server.GetManagerSessionMutex(manager)
		sessionMutex.RLock()
		defer sessionMutex.RUnlock()
		_, exists := server.GetManagerSessionMap(manager)[session.ID]
		return !exists
	})

	if !managerDeletedSession {
		t.Fatalf("Expected session to be removed from session map of manager. Session was not removed.")
	}

}

func TestSession_IgnoresInvalidJSON(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()
	clientConnection, session := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	testutil.WriteToConnection(t, clientConnection, []byte(`"Data"`))

	expectedType := "valid_data"
	testutil.WriteToConnection(t, clientConnection, []byte(fmt.Sprintf(`{"type": %q}`, expectedType)))

	select {
	case wrapper := <-manager.InboundBuffer:
		if wrapper.Event.Type != expectedType {
			t.Fatalf("expected the first event in manager buffer to have type %v got %v", expectedType, wrapper.Event.Type)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("expected valid data to be passed to manager")
	}

	//  this is checked to ensure user is not kicked on invalid data.
	managerDeletedSession := testutil.PollEvent(t, testutil.SMALL_DELAY, 10, func() bool {
		sessionMutex := server.GetManagerSessionMutex(manager)
		sessionMutex.RLock()
		defer sessionMutex.RUnlock()
		_, exists := server.GetManagerSessionMap(manager)[session.ID]
		return !exists
	})

	if managerDeletedSession {
		t.Fatalf("Expected session to not be removed from session map of manager. Session was removed.")
	}

}

func TestSession_IsRemovedWhenBufferFull(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	clientConnection, session := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	//this needs to exceed buffer size
	for range 1000 {
		session.Send(protocol.Response{})
	}

	managerDeletedSession := testutil.PollEvent(t, testutil.SMALL_DELAY, 10, func() bool {
		sessionMutex := server.GetManagerSessionMutex(manager)
		sessionMutex.RLock()
		defer sessionMutex.RUnlock()
		_, exists := server.GetManagerSessionMap(manager)[session.ID]
		return !exists
	})

	if !managerDeletedSession {
		t.Fatalf("Expected session to be removed from session map of manager. Session was not removed.")
	}
}
