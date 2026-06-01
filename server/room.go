package server

import (
	"sync"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)

type Room interface{
	AddSession(session *Session)
	RemoveSession(session *Session) int
	Broadcast(event protocol.ServerSentEvent) 
}
type InMemoryRoom struct {
	sessions     map[string]*Session
	sessionMutex sync.RWMutex
}

func NewInMemoryRoom() *InMemoryRoom {
	return &InMemoryRoom{
		sessions:     make(map[string]*Session),
		sessionMutex: sync.RWMutex{},
	}
}

func (room *InMemoryRoom) AddSession(session *Session) {
	room.sessionMutex.Lock()
	defer room.sessionMutex.Unlock()
	room.sessions[session.ID] = session
}

// returns the length of the remaining sessions in the room.
// Used to delete the room when the last session is removed
func (room *InMemoryRoom) RemoveSession(session *Session) int {
	room.sessionMutex.Lock()
	defer room.sessionMutex.Unlock()
	delete(room.sessions, session.ID)
	return len(room.sessions)
}

func (room *InMemoryRoom) Broadcast(event protocol.ServerSentEvent) {
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
