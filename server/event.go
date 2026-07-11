package server

import "github.com/op-comm/op-comm/protocol"

type Middleware func(event *protocol.Request, session *Session) bool

type EventHandler func(event *protocol.Request, session *Session)

type EventService interface {
	Handle(action string, event *protocol.Request, session *Session)
}

type EventServiceFunc func(action string, event *protocol.Request, session *Session)

func (eventServiceFunc EventServiceFunc) Handle(action string, event *protocol.Request, session *Session) {
	eventServiceFunc(action, event, session)
}
