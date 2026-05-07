package server

import (
	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)

// the output buffer should have a max size that determines when it's disconnected
// because if the server places X events without the client reading from it in time,
// it's likely a network issue and the session should be removed
var MAX_BUFFER_EVENTS_BEFORE_DISCONNECT int = 512

type Session struct {
	ID           string
	connection   *websocket.Conn
	Manager      *Manager
	OutputBuffer chan protocol.ServerSentEvent
}

func NewSession(id string, connection *websocket.Conn, manager *Manager) *Session {
	return &Session{
		ID:           id,
		connection:   connection,
		Manager:      manager,
		OutputBuffer: make(chan protocol.ServerSentEvent, MAX_BUFFER_EVENTS_BEFORE_DISCONNECT),
	}
}

func (session *Session) SendIncomingEventsToManager() {
	for {
		//TODO:
		// read socket
		// transfer into event struct
		// write to manager input buffer
	}
}

func (session *Session) HandleOutboundEventsFromManager() {
	for {
		//TODO: write to socket
	}
}

func (session *Session) Close(status websocket.StatusCode, reason string) {
	// TODO: close socket connection
	session.connection.Close(status, reason)
}
