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

	if Command(action) == CommandList {
		if authorizeErr := service.Authorizer.Authorize(session, nil, action); authorizeErr != nil {
			session.Reply(event, nil, fmt.Sprintf("Unauthorized: %v", authorizeErr))
			return
		}
		session.Reply(event, RoomListResponse{Rooms: service.Manager.GetRoomIDs()}, "")
		return
	}

	emptyPayload := len(event.Data) == 0
	if emptyPayload {
		session.Reply(event, nil, string(ErrMissingPayload))
		return
	}

	var requestData struct {
		RoomID string `json:"room_id"`
	}

	if jsonErr := json.Unmarshal(event.Data, &requestData); jsonErr != nil {
		session.Reply(event, nil, string(ErrInvalidJSON))
		return
	}

	roomID := requestData.RoomID
	room := service.Manager.GetRoom(requestData.RoomID)

	if room == nil && Command(action) != CommandCreate {
		session.Reply(event, nil, string(ErrRoomNotFound))
		return
	}

	authorizeErr := service.Authorizer.Authorize(session, room, action)
	if authorizeErr != nil {
		session.Reply(event, nil, fmt.Sprintf("Unauthorized: %v", authorizeErr))
		return
	}

	// custom actions have priority, and are handlers on the room object
	// so overriding list action does not work.
	handler, exists := service.Handlers[action]
	if exists {
		handler(room, event, session)
		return
	}

	switch Command(action) {

	case CommandList:
		session.Reply(event, RoomListResponse{Rooms: service.Manager.GetRoomIDs()}, "")

	case CommandCreate:
		room = service.Manager.CreateRoom(roomID)
		room.AddSession(session)
		service.Manager.GlobalBroadcastToOthers(protocol.ServerSentEvent{
			EventType: string(EventRoomCreated),
			Data: RoomCreatedBroadcast{
				RoomID: roomID,
			},
		}, session.ID)
		session.Reply(event, RoomCreateResponse{RoomID: roomID}, "")
	case CommandDelete:
		service.Manager.DeleteRoom(roomID)
		room.BroadcastToOthers(protocol.ServerSentEvent{
			EventType: string(EventRoomDeleted),
			Data: RoomDeletedBroadcast{
				RoomID: roomID,
			},
		}, session.ID)

		session.Reply(event, RoomDeleteResponse{RoomID: roomID}, "")

	case CommandJoin:
		room.AddSession(session)
		room.BroadcastToOthers(protocol.ServerSentEvent{
			EventType: string(EventUserJoined),
			Data: RoomUserJoinedBroadcast{
				RoomID: roomID,
				UserID: session.ID,
			},
		}, session.ID)

		session.Reply(event, RoomJoinResponse{RoomID: roomID}, "")

	case CommandLeave:
		room.RemoveSession(session)
		room.BroadcastToOthers(protocol.ServerSentEvent{
			EventType: string(EventUserLeft),
			Data: RoomUserLeftBroadcast{
				RoomID: roomID,
				UserID: session.ID,
			},
		}, session.ID)

		session.Reply(event, RoomLeaveResponse{RoomID: roomID}, "")
	default:
		session.Reply(event, nil, fmt.Sprintf("Unknown action: %s", action))
	}
}
