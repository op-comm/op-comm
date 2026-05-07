package server

import (
	"context"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/op-comm/op-comm/protocol"
)

type Manager struct {
	InboundBuffer  chan *protocol.ClientSentEvent
	sessions       map[string]*Session
	sessionMutex   sync.RWMutex
	clientIDMethod func(*http.Request) string
}

func NewManager() *Manager {
	return &Manager{
		InboundBuffer: make(chan *protocol.ClientSentEvent),
		sessions:      make(map[string]*Session),
		sessionMutex:  sync.RWMutex{},
		clientIDMethod: func(request *http.Request) string {
			return uuid.NewString()
		},
	}
}

// TODO: replace with functional options pattern?
func (manager *Manager) SetClientIDMethod(method func(request *http.Request) string) {
	manager.clientIDMethod = method
}

// ends with the context and will simply just handle every inbound event
func (manager *Manager) Run(ctx context.Context) {
	defer manager.cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case inboundEvent := <-manager.InboundBuffer:
			manager.handleEvent(*inboundEvent)
		}
	}
}

func (manager *Manager) HandleWSUpgradeRequest(writer http.ResponseWriter, request *http.Request) {
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}

	clientID := manager.clientIDMethod(request)
	clientSession := NewSession(clientID, connection, manager)

	manager.addSession(clientSession)

	go clientSession.HandleOutboundEventsFromManager()
	// since each request is its own goroutine we can use
	// the current one to handle reading from the socket
	clientSession.SendIncomingEventsToManager()
}

func (manager *Manager) handleEvent(event protocol.ClientSentEvent) {
	//TODO: process events
}

func (manager *Manager) cleanup() {
	//TODO: implement cleanup process

	manager.sessionMutex.Lock()

	sessionsToClose := make([]*Session, 0, len(manager.sessions))
	for _, session := range manager.sessions {
		sessionsToClose = append(sessionsToClose, session)
	}
	clear(manager.sessions)

	manager.sessionMutex.Unlock()

	for _, session := range sessionsToClose {
		session.Close()
	}

}

func (manager *Manager) addSession(session *Session) {
	manager.sessionMutex.Lock()
	defer manager.sessionMutex.Unlock()
	manager.sessions[session.ID] = session
}

func (manager *Manager) removeSession(clientID string) {
	manager.sessionMutex.Lock()
	defer manager.sessionMutex.Unlock()
	session, sessionExists := manager.sessions[clientID]
	if sessionExists {
		session.Close()
		delete(manager.sessions, clientID)
	}
}

func (manager *Manager) sessionCount() int {
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	return len(manager.sessions)
}
