package server

import "github.com/op-comm/op-comm/protocol"

type Middleware func(event *protocol.ClientSentEvent, session *Session) bool

type EventHandler func(event *protocol.ClientSentEvent, session *Session)

type EventService interface {
	Handle(action string, event *protocol.ClientSentEvent, session *Session)
}

type EventServiceFunc func(action string, event *protocol.ClientSentEvent, session *Session)

func (eventServiceFunc EventServiceFunc) Handle(action string, event *protocol.ClientSentEvent, session *Session) {
	eventServiceFunc(action, event, session)
}
