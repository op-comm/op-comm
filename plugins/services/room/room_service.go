package room

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/server"
)

type RoomHandler func(room server.Room, event *protocol.ClientSentEvent, session *server.Session)
type RoomOption func(*RoomService)

func defaultRoomService() RoomService {
	return RoomService{
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Authorizer: &server.AllowAllRoomAuthorizer{},
		Handlers:   make(map[string]RoomHandler),
	}
}

type RoomService struct {
	Manager    *server.Manager
	Logger     *slog.Logger
	Authorizer server.RoomAuthorizer
	Handlers   map[string]RoomHandler // Not thread safe
}

func WithLogger(logger slog.Logger) RoomOption {
	return func(service *RoomService) {
		service.Logger = &logger
	}
}

func WithAuthorizer(authorizer server.RoomAuthorizer) RoomOption {
	return func(service *RoomService) {
		service.Authorizer = authorizer
	}
}

func WithHandlers(handlers map[string]RoomHandler) RoomOption {
	return func(service *RoomService) {
		service.Handlers = handlers
	}
}

// not thread safe, use before manager.Run()
func (service *RoomService) SetHandler(action string, handler RoomHandler) {
	service.Handlers[action] = handler
}
func NewRoomService(manager *server.Manager, serviceOptions ...RoomOption) (*RoomService, error) {
	if manager == nil {
		return nil, errors.New("manager is required")
	}

	roomService := defaultRoomService()
	roomService.Manager = manager

	for _, option := range serviceOptions {
		option(&roomService)
	}

	return &roomService, nil
}

func (service *RoomService) Handle(action string, event *protocol.ClientSentEvent, session *server.Session) {

	if len(event.Data) == 0 && action != "list" {
		session.Reply(event, nil, "Missing payload")
		return
	}

	var requestData BaseRoomRequest
	if jsonErr := json.Unmarshal(event.Data, &requestData); jsonErr != nil {
		session.Reply(event, nil, "Invalid JSON payload")
		return
	}
	roomID := requestData.RoomID

	room := service.Manager.GetRoom(requestData.RoomID)
	if room == nil && action != "create" {
		session.Reply(event, nil, "Room does not exist")
		return
	}

	authorizeErr := service.Authorizer.Authorize(session, room, action)
	if authorizeErr != nil {
		session.Reply(event, nil, fmt.Sprintf("Unauthorized: %v", authorizeErr))
		return
	}

	// custom actions have priority, allows to override our defaults
	handler, exists := service.Handlers[action]
	if exists {
		handler(room, event, session)
		return
	}

	switch action {
	case "list":
		session.Reply(event, RoomListReply{
			Rooms: service.Manager.GetRoomIDs(),
		}, "")
	case "create":
		room = service.Manager.CreateRoom(roomID)
		room.AddSession(session)
		service.Manager.GlobalBroadcastToOthers(protocol.ServerSentEvent{
			EventType: "room:created",
			Data: BaseRoomBroadcast{
				RoomID: roomID,
			},
		}, session.ID)
		session.Reply(event, BaseRoomReply{
			RoomID: roomID,
		}, "")
	case "delete":
		service.Manager.DeleteRoom(roomID)
		room.BroadcastToOthers(protocol.ServerSentEvent{
			EventType: "room:deleted",
			Data: BaseRoomBroadcast{
				RoomID: roomID,
			},
		}, session.ID)

		session.Reply(event, BaseRoomReply{
			RoomID: roomID,
		}, "")

	case "join":
		room.AddSession(session)
		room.BroadcastToOthers(protocol.ServerSentEvent{
			EventType: "room:user_joined",
			Data: BaseRoomBroadcast{
				RoomID: roomID,
				UserID: session.ID,
			},
		}, session.ID)

		session.Reply(event, BaseRoomReply{
			RoomID: roomID,
		}, "")

	case "leave":
		room.RemoveSession(session)
		room.BroadcastToOthers(protocol.ServerSentEvent{
			EventType: "room:user_left",
			Data: BaseRoomBroadcast{
				RoomID: roomID,
				UserID: session.ID,
			},
		}, session.ID)

		session.Reply(event, BaseRoomReply{
			RoomID: roomID,
		}, "")
	default:
		session.Reply(event, nil, fmt.Sprintf("Unknown action: %s", action))
	}
}
