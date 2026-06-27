package services

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/server"
)

type RoomHandler func(room *server.Room, event *protocol.ClientSentEvent, session *server.Session)

type RoomService struct {
	Manager *server.Manager
	Logger *slog.Logger
	Authorizer server.RoomAuthorizer
	Handlers map[string]RoomHandler // Not thread safe
}

func NewRoomService(manager *server.Manager, logger *slog.Logger, authorizer server.RoomAuthorizer, handlers map[string]RoomHandler) (*RoomService, error) {
	if manager == nil {
		return nil, errors.New("manager is required")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil));
	}
	if authorizer == nil {
		authorizer = &server.AllowAllRoomAuthorizer{}
	}
	if handlers == nil {
		handlers = make(map[string]RoomHandler)
	}
	return &RoomService{
		Manager: manager,
		Logger: logger,
		Authorizer: authorizer,
		Handlers: handlers,
	}, nil
}

type BaseRoomRequest struct {
	RoomID string `json:"room_id"`
}
type BaseRoomReply struct {
	RoomID string `json:"room_id"`
	Status string `json:"status"`
}

func (service *RoomService) Handle(action string, event *protocol.ClientSentEvent, session *server.Session){

	if len(event.Data) == 0 {
		session.Reply(event, nil, "Missing payload")
		return
	}

	var requestData BaseRoomRequest
	if jsonErr := json.Unmarshal(event.Data, &requestData); jsonErr != nil {
		session.Reply(event, nil, "Invalid JSON payload")
		return
	} 

	room := service.Manager.GetRoom(requestData.RoomID)
	if room == nil && action != "create" {
		session.Reply(event, nil, "Room does not exist")
		return
	}

	authorizeErr := service.Authorizer.Authorize(session, &room, action)
	if authorizeErr != nil {
		session.Reply(event, nil, fmt.Sprintf("Unauthorized: %v", authorizeErr))
		return
	}

	// custom actions have priority allows to override our defaults
	handler, exists := service.Handlers[action]
	if exists{
		handler(&room, event, session)
		return
	}

	switch action {
		case "create":
		case "delete":
		case "join":
		case "leave":
		default:
			session.Reply(event, nil, fmt.Sprintf("Unkown action: %s", action))
	}
}

