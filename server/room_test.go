package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)

func TestRoom_IsRemovedWhenLastSessionIsRemoved(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	_, session := connectAndFetchSession(t, manager, wsURL, []string{})

	room := manager.CreateRoom("test_room")

	room.AddSession(session)

	if manager.GetRoom("test_room") == nil {
		t.Fatal("Failed to create room")
	}

	manager.removeSessionFromAllRooms(session)

	roomDeleted := pollEvent(t, SMALL_DELAY, 10, func() bool {
		return manager.GetRoom("test_room") == nil
	})

	if !roomDeleted {
		t.Fatal("Expected room to be deleted after session removed, room still exists")
	}
}

func TestRoom_IsNotRemovedWhenAnotherSessionRemains(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	_, session := connectAndFetchSession(t, manager, wsURL, []string{})
	_, session2 := connectAndFetchSession(t, manager, wsURL, []string{session.ID})

	room := manager.CreateRoom("test_room")

	room.AddSession(session)
	room.AddSession(session2)

	if manager.GetRoom("test_room") == nil {
		t.Fatal("Failed to create room")
	}

	manager.removeSessionFromAllRooms(session)

	roomDeleted := pollEvent(t, SMALL_DELAY, 10, func() bool {
		return manager.GetRoom("test_room") == nil
	})

	if roomDeleted {
		t.Fatal("Expected room to not be deleted after only one session removed, room was deleted")
	}

}

func TestRoom_BroadcastReachesAllSessions(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

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
		connection, session := connectAndFetchSession(t, manager, wsURL, existingIds)
		existingIds = append(existingIds, session.ID)
		clientConnections = append(clientConnections, connection)
		room.AddSession(session)
	}

	expectedEvent := protocol.ServerSentEvent{
		EventType: "room:test",
		Data:      []byte(`"test data"`),
	}
	room.Broadcast(expectedEvent)

	var event protocol.ServerSentEvent

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

		if event.EventType != expectedEvent.EventType {
			t.Fatalf("Recieved Event does not match expected event: expected %s, got %s", expectedEvent.EventType, event.EventType)
		}
	}

}

func TestRoom_BroadcastToSlowClientCausesDisconnect(t *testing.T) {
	manager, wsURL, cleanup := setupTestServer(t)
	defer cleanup()

	_, session := connectAndFetchSession(t, manager, wsURL, []string{})

	room := manager.CreateRoom("test_room")

	room.AddSession(session)

	if manager.GetRoom("test_room") == nil {
		t.Fatal("Failed to create room")
	}

	event := protocol.ServerSentEvent{
		EventType: "test:spam",
		Data:      []byte(`"data"`),
	}
	// this number needs to exceed the buffer size
	for range 1000 {
		room.Broadcast(event)
	}

	sessionWasRemoved := pollEvent(t, SMALL_DELAY, 10, func() bool {
		manager.sessionMutex.RLock()
		defer manager.sessionMutex.RUnlock()
		_, exists := manager.sessions[session.ID]
		return !exists
	})

	if !sessionWasRemoved {
		t.Fatal("Expected session to be removed from manager")
	}

}
