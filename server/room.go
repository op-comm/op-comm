package server

import (
	"sync"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)

//TODO: make an interface to allow different ways of storing rooms
// inMemory (this) vs something like redis to allow multiple server instances to access the same rooms

type Room struct {
	sessions     map[string]*Session
	sessionMutex sync.RWMutex
}

func NewRoom() *Room {
	return &Room{
		sessions:     make(map[string]*Session),
		sessionMutex: sync.RWMutex{},
	}
}

func (room *Room) AddSession(session *Session) {
	room.sessionMutex.Lock()
	defer room.sessionMutex.Unlock()
	room.sessions[session.ID] = session
}

// returns the length of the remaining sessions in the room.
// Used to delete the room when the last session is removed
func (room *Room) RemoveSession(session *Session) int {
	room.sessionMutex.Lock()
	defer room.sessionMutex.Unlock()
	delete(room.sessions, session.ID)
	return len(room.sessions)
}

func (room *Room) Broadcast(event protocol.ServerSentEvent) {
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()
	for _, session := range room.sessions {
		select {
		case session.OutputBuffer <- event:
			continue
		default:
			// reaching here means the output buffer is full
			// which likely points to network issues on the client
			// we can disconnect here to prevent further blocking
			session.Close(websocket.StatusAbnormalClosure, "Too many messages in buffer")
		}
	}
}
