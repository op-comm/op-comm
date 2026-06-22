package server

import (
	"log/slog"
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
	id string
	logger *slog.Logger
}

func NewInMemoryRoom(id string, logger *slog.Logger) *InMemoryRoom {
	return &InMemoryRoom{
		sessions:     make(map[string]*Session),
		sessionMutex: sync.RWMutex{},
		id: id,
		logger: logger,
	}
}

func (room *InMemoryRoom) AddSession(session *Session) {
	room.sessionMutex.Lock()
	defer room.sessionMutex.Unlock()
	room.sessions[session.ID] = session
	room.logger.Debug("added session to room", "session_id", session.ID)
}

// returns the length of the remaining sessions in the room.
// Used to delete the room when the last session is removed
func (room *InMemoryRoom) RemoveSession(session *Session) int {
	room.sessionMutex.Lock()
	defer room.sessionMutex.Unlock()
	delete(room.sessions, session.ID)
	room.logger.Debug("removed session from room", "session_id", session.ID)
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
		room.logger.Debug("room broadcasting event to session", "session_id", session.ID, "event_type", event.EventType)
		session.Send(event)
	}
}

func (room *InMemoryRoom) BroadcastToOthers(event protocol.ServerSentEvent, senderID string){
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()

	for _, session := range room.sessions {
		if senderID != session.ID {
			room.logger.Debug("room broadcasting event to session", "session_id", session.ID, "event_type", event.EventType, "sender_id", senderID)
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
			room.logger.Debug("room broadcasting event to session", "session_id", session.ID, "event_type", event.EventType)
			session.Send(event)
		}
	}
}

func (room *InMemoryRoom) SendToOnly(event protocol.ServerSentEvent, sessionIds []string) {
	room.sessionMutex.RLock()
	defer room.sessionMutex.RUnlock()
	for _, id := range sessionIds {
		if session, exists := room.sessions[id]; exists {
			room.logger.Debug("room broadcasting event to session", "session_id", session.ID, "event_type", event.EventType)
			session.Send(event)
		}
	}

}
