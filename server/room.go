package server

import (
	"sync"

	"github.com/op-comm/op-comm/internal"
	"github.com/op-comm/op-comm/protocol"
)

type Room interface {
	AddSession(session *Session)
	RemoveSession(session *Session) int
	HasSession(session *Session) bool
	Broadcast(event protocol.ServerSentEvent)
	BroadcastToOthers(event protocol.ServerSentEvent, senderID string)
	BroadcastExclude(event protocol.ServerSentEvent, sessionIds []string)
	SendToOnly(event protocol.ServerSentEvent, sessionIds []string)
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

func (room *InMemoryRoom) HasSession(targetSession *Session) bool {
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()
	_, exists := room.sessions[targetSession.ID]
	return exists
}

func (room *InMemoryRoom) Broadcast(event protocol.ServerSentEvent) {
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()
	for _, session := range room.sessions {
		session.Send(event)
	}
}

func (room *InMemoryRoom) BroadcastToOthers(event protocol.ServerSentEvent, senderID string){
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()

	for _, session := range room.sessions {
		if senderID != session.ID {
			session.Send(event)
		}
	}
}

func (room *InMemoryRoom) BroadcastExclude(event protocol.ServerSentEvent, sessionIdsToExclude []string){
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()
	blackList := internal.SetFromList(sessionIdsToExclude)
	for _, session := range room.sessions {
		if !blackList.Has(session.ID) {
			session.Send(event)
		}
	}
}

func (room *InMemoryRoom) SendToOnly(event protocol.ServerSentEvent, sessionIds []string) {
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()
	for _, id := range sessionIds {
		if session, exists := room.sessions[id]; exists {
			session.Send(event)
		}
	}

}
