package server

import "github.com/op-comm/op-comm/protocol"

type EventHandler func(event *protocol.ClientSentEvent, session *Session)

type EventService interface {
	Handle(action string, event *protocol.ClientSentEvent, session *Session)
}
