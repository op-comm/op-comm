package server_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/server"
	"github.com/op-comm/op-comm/testutil"
)

func TestRoom_IsRemovedWhenLastSessionIsRemoved(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	_, session := testutil.ConnectToServer(t, manager, wsURL)

	room := manager.CreateRoom("test_room")

	room.AddSession(session)

	if manager.GetRoom("test_room") == nil {
		t.Fatal("Failed to create room")
	}

	session.Close(websocket.StatusNormalClosure, "")

	roomDeleted := testutil.PollEvent(t, testutil.SMALL_DELAY, 10, func() bool {
		return manager.GetRoom("test_room") == nil
	})

	if !roomDeleted {
		t.Fatal("Expected room to be deleted after session removed, room still exists")
	}
}

func TestRoom_IsNotRemovedWhenAnotherSessionRemains(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	_, session := testutil.ConnectToServer(t, manager, wsURL)
	_, session2 := testutil.ConnectToServer(t, manager, wsURL)

	room := manager.CreateRoom("test_room")

	room.AddSession(session)
	room.AddSession(session2)

	if manager.GetRoom("test_room") == nil {
		t.Fatal("Failed to create room")
	}

	server.RemoveSessionFromAllManagerRooms(manager, session)

	roomDeleted := testutil.PollEvent(t, testutil.SMALL_DELAY, 10, func() bool {
		return manager.GetRoom("test_room") == nil
	})

	if roomDeleted {
		t.Fatal("Expected room to not be deleted after only one session removed, room was deleted")
	}

}

func TestRoom_BroadcastReachesAllSessions(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	//ignore for this test
	manager.SetLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{})))

	// a room created with zero sessions should still exist because it was manually created
	// (deleting this room should be handled by the application)
	// TODO: possibly set a timeout for deleting empty rooms
	room := manager.CreateRoom("test_room")
	if manager.GetRoom("test_room") == nil {
		t.Fatal("Failed to create room")
	}
	SESSION_COUNT := 1000
	existingIds := []string{}
	clientConnections := []*websocket.Conn{}
	for range SESSION_COUNT {
		connection, session := testutil.ConnectToServer(t, manager, wsURL)
		defer connection.Close(websocket.StatusNormalClosure, "")
		existingIds = append(existingIds, session.ID)
		clientConnections = append(clientConnections, connection)
		room.AddSession(session)
	}

	expectedEvent := protocol.Broadcast{
		Type: "room:test",
	}
	room.Broadcast(expectedEvent)

	var event protocol.Broadcast

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, connection := range clientConnections {
		_, data, err := connection.Read(ctx)
		if err != nil {
			t.Fatalf("Failed to read from connection: %v", err)
		}
		unmarshalErr := json.Unmarshal(data, &event)
		if unmarshalErr != nil {
			t.Fatalf("Failed to unmarshal json: %v", unmarshalErr)
		}

		if event.Type != expectedEvent.Type {
			t.Fatalf("Recieved Event does not match expected event: expected %s, got %s", expectedEvent.Type, event.Type)
		}
	}

}

func TestRoom_BroadcastToSlowClientCausesDisconnect(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	//ignore for this test
	manager.SetLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{})))

	_, session := testutil.ConnectToServer(t, manager, wsURL)

	room := manager.CreateRoom("test_room")

	room.AddSession(session)

	if manager.GetRoom("test_room") == nil {
		t.Fatal("Failed to create room")
	}

	event := protocol.Broadcast{
		Type: "test:spam",
		Data: []byte(`"data"`),
	}
	// this number needs to exceed the buffer size
	for range 1000 {
		room.Broadcast(event)
	}

	sessionWasRemoved := testutil.PollEvent(t, testutil.SMALL_DELAY, 10, func() bool {
		sessionMutex := server.GetManagerSessionMutex(manager)
		sessionMutex.RLock()
		defer sessionMutex.RUnlock()
		_, exists := server.GetManagerSessionMap(manager)[session.ID]
		return !exists
	})

	if !sessionWasRemoved {
		t.Fatal("Expected session to be removed from manager")
	}

}
