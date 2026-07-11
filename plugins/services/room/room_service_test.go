package room

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/testutil"
)

func TestRoomService_ListsAllRooms(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	clientConnection, _ := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	service, _ := NewRoomService(manager)
	manager.RegisterEventService("room", service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.Run(ctx)

	allRooms := []string{"test-room1", "test-room2", "test-room3", "test-room4", "random-room"}

	for _, room := range allRooms {
		manager.CreateRoom(room)
	}
	testutil.WriteToConnection(t, clientConnection, []byte(`{
	"type": "room:list"
	}`))

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	_, data, readErr := clientConnection.Read(readCtx)

	if readErr != nil {
		t.Fatalf("Unexpected read error: %v", readErr)
	}

	var response struct {
		EventType string           `json:"type"`
		Data      RoomListResponse `json:"data"`
	}

	jsonErr := json.Unmarshal(data, &response)

	if jsonErr != nil {
		t.Fatalf("Unexpected json error: %v", jsonErr)
	}

	for _, room := range response.Data.Rooms {
		if !slices.Contains(allRooms, room) {
			t.Fatalf("Expected response to contain all rooms, missing %v", room)
		}
	}
}

func TestRoomService_CreatesRoomOnRequest(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	clientConnection, _ := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	service, _ := NewRoomService(manager)
	manager.RegisterEventService("room", service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.Run(ctx)

	testutil.WriteToConnection(t, clientConnection, []byte(`{
	"type": "room:create",
	"data": { "room_id": "test-room" }
	}`))

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	_, data, readErr := clientConnection.Read(readCtx)

	if readErr != nil {
		t.Fatalf("Unexpected read error: %v", readErr)
	}

	var response struct {
		EventType string             `json:"type"`
		Data      RoomCreateResponse `json:"data"`
	}
	jsonErr := json.Unmarshal(data, &response)

	if jsonErr != nil {
		t.Fatalf("Unexpected json error: %v", jsonErr)
	}

	if response.EventType != "room:create:reply" {
		t.Fatalf("Mismatching event type.\n expected: room:create:reply\n got: %s", response.EventType)
	}

	if response.Data.RoomID != "test-room" {
		t.Fatalf("Mismatching room id.\n expected: test-room\n got: %s", response.Data.RoomID)
	}

}

func TestRoomService_AddsSessionToRoomOnJoinRoomRequest(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	clientConnection, _ := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	service, _ := NewRoomService(manager)
	manager.RegisterEventService("room", service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.Run(ctx)

	manager.CreateRoom("test-room")

	writeCtx, cancel := context.WithTimeout(context.Background(), testutil.TEST_WRITE_TIMEOUT)
	defer cancel()
	clientConnection.Write(writeCtx, websocket.MessageText, []byte(`
		{
			"type": "room:join",
			"data": {
				"room_id": "test-room"
			}
		}
	`))

	room := manager.GetRoom("test-room")

	sessionWasAdded := testutil.PollEvent(t, 10*time.Millisecond, 10, func() bool {
		return room.SessionCount() == 1
	})
	if !sessionWasAdded {
		t.Fatalf("expected session to be added to room, session was not added")
	}

}
func TestRoomService_BroadcastsToRoomWhenUserLeaves(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	service, _ := NewRoomService(manager)
	manager.RegisterEventService("room", service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.Run(ctx)

	manager.CreateRoom("test-room")

	connectionToLeave, session := testutil.ConnectToServer(t, manager, wsURL)

	testutil.WriteToConnection(t, connectionToLeave, []byte(`
	{ 
		"type": "room:join", 
		"data": {
			"room_id": "test-room"
		}
	}
	`))

	connectionInRoom, _ := testutil.ConnectToServer(t, manager, wsURL)

	t.Logf("%v", session.ID)
	testutil.WriteToConnection(t, connectionInRoom, []byte(`
	{ 
		"type": "room:join", 
		"data": {
			"room_id": "test-room"
		}
	}
	`))

	testutil.WriteToConnection(t, connectionToLeave, []byte(`
	{ 
		"type": "room:leave", 
		"data": {
			"room_id": "test-room"
		}
	}
	`))

	expectedUserID := session.ID
	expectedType := "room:user_left"

	data := testutil.WaitForEvent(t, connectionInRoom, expectedType)
	var response struct {
		EventType string                `json:"type"`
		Data      RoomUserLeftBroadcast `json:"data"`
	}
	jsonErr := json.Unmarshal(data, &response)
	if jsonErr != nil {
		t.Fatalf("unexpected json error: %v", jsonErr)
	}

	if response.Data.UserID != expectedUserID {
		t.Fatalf("expected response.Data.UserID to be %v, got %v", expectedUserID, response.Data.UserID)
	}
}

func TestRoomService_BroadcastsToRoomWhenUserJoins(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	service, _ := NewRoomService(manager)
	manager.RegisterEventService("room", service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.Run(ctx)

	manager.CreateRoom("test-room")

	connectionInRoom, _ := testutil.ConnectToServer(t, manager, wsURL)

	testutil.WriteToConnection(t, connectionInRoom, []byte(`
	{ 
		"type": "room:join", 
		"data": {
			"room_id": "test-room"
		}
	}
	`))

	joiningConnection, session := testutil.ConnectToServer(t, manager, wsURL)

	t.Logf("%v", session.ID)
	testutil.WriteToConnection(t, joiningConnection, []byte(`
	{ 
		"type": "room:join", 
		"data": {
			"room_id": "test-room"
		}
	}
	`))

	expectedUserID := session.ID
	expectedType := "room:user_joined"

	data := testutil.WaitForEvent(t, connectionInRoom, expectedType)

	var response struct {
		EventType string                  `json:"type"`
		Data      RoomUserJoinedBroadcast `json:"data"`
	}
	jsonErr := json.Unmarshal(data, &response)
	if jsonErr != nil {
		t.Fatalf("unexpected json error: %v", jsonErr)
	}

	if response.Data.UserID != expectedUserID {
		t.Fatalf("expected response.Data.UserID to be %v, got %v", expectedUserID, response.Data.UserID)
	}
}

func TestRoomService_ReturnsErrorOnUnknownType(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	service, _ := NewRoomService(manager)
	manager.RegisterEventService("room", service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.Run(ctx)

	clientConnection, _ := testutil.ConnectToServer(t, manager, wsURL)
	defer clientConnection.Close(websocket.StatusNormalClosure, "")

	testutil.WriteToConnection(t, clientConnection, []byte(`{ "type": "room:invalid-type" }`))

	readCtx, cancel := context.WithTimeout(context.Background(), testutil.TEST_READ_TIMEOUT)
	defer cancel()

	_, data, readErr := clientConnection.Read(readCtx)
	if readErr != nil {
		t.Fatalf("Unexpected read error: %v", readErr)
	}

	var response protocol.ServerSentEvent

	jsonErr := json.Unmarshal(data, &response)
	if jsonErr != nil {
		t.Fatalf("unexpected json error: %v", jsonErr)
	}

	if response.Error == "" {
		t.Fatalf("Expected unknown action error, got empty string")
	}

	if response.Data != nil {
		t.Fatalf("Expected empty data on error response, got: %v", response.Data)
	}

}

func TestRoomService_IgnoresDifferentNamespace(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.Run(ctx)

	connection, _ := testutil.ConnectToServer(t, manager, wsURL)
	defer connection.Close(websocket.StatusNormalClosure, "")

	testutil.WriteToConnection(t, connection, []byte(`{"type": "other:type"}`))

	readCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, readErr := connection.Read(readCtx)

	if readErr == nil {
		t.Fatal("expected read error, got nil")
	}

	if !errors.Is(readErr, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded error, got %v", readErr)
	}
}
