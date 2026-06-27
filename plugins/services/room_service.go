package services

import (
	"log/slog"

	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/server"
)

type RoomService struct {
	logger *slog.Logger,
	
}

func (service *RoomService) Handle(action string, event *protocol.ClientSentEvent, session *server.Session){

	switch action {
		case "join":

	}
}

