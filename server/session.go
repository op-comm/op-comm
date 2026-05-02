package server

import (
	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)
type Session struct {
	ID string
	connection *websocket.Conn
	Manager *Manager
	OutputBuffer chan protocol.ServerSentEvent
}