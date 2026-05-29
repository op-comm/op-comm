package server

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type Manager struct {
	InboundBuffer  chan sessionEventWrapper
	sessions       map[string]*Session
	sessionMutex   sync.RWMutex
	clientIDMethod func(*http.Request) string
	handlers       map[string]EventHandler
	services       map[string]EventService
	rooms          map[string]*Room
	roomMutex      sync.RWMutex
	authenticator  Authenticator
	middlewares    []Middleware
}

func NewManager() *Manager {
	return &Manager{
		InboundBuffer: make(chan sessionEventWrapper),
		sessions:      make(map[string]*Session),
		sessionMutex:  sync.RWMutex{},
		handlers:      make(map[string]EventHandler),
		services:      make(map[string]EventService),
		clientIDMethod: func(request *http.Request) string {
			return uuid.NewString()
		},
		authenticator: nil,

		rooms:       make(map[string]*Room),
		roomMutex:   sync.RWMutex{},
		middlewares: []Middleware{},
	}
}

// TODO: replace with functional options pattern?
func (manager *Manager) SetClientIDMethod(method func(request *http.Request) string) {
	manager.clientIDMethod = method
}

func (manager *Manager) SetAuthenticator(authenticator Authenticator) {
	manager.authenticator = authenticator
}

// ends with the context and will simply just handle every inbound event
func (manager *Manager) Run(ctx context.Context) {
	defer manager.cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case inboundEvent := <-manager.InboundBuffer:
			manager.handleEvent(inboundEvent)
		}
	}
}

func (manager *Manager) HandleWSUpgradeRequest(writer http.ResponseWriter, request *http.Request) {

	var authState map[string]any
	if manager.authenticator != nil {
		var authError error
		authState, authError = manager.authenticator.Authenticate(request)
		if authError != nil {
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write([]byte(authError.Error()))
			return
		}
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}

	clientCtx, cancel := context.WithCancel(context.Background())
	clientID := manager.clientIDMethod(request)
	clientSession := NewSession(clientID, connection, manager, cancel)

	if authState != nil {
		clientSession.CopyIntoState(authState)
	}
	manager.addSession(clientSession)

	go clientSession.writePump(clientCtx)
	clientSession.readPump()
}

// Note: this is NOT threadsafe, this must be used before the Run method.
// Middlewares are run in the order they are defined.
func (manager *Manager) UseMiddleware(middleware Middleware) {
	manager.middlewares = append(manager.middlewares, middleware)
}

// Note: this is NOT threadsafe, this must be used before the Run method
func (manager *Manager) On(action string, callback EventHandler) {
	manager.handlers[action] = callback
}

// Note: this is NOT threadsafe, this must be used before the Run method
func (manager *Manager) RegisterEventService(namespace string, service EventService) {
	manager.services[namespace] = service
}

func (manager *Manager) handleEvent(wrapper sessionEventWrapper) {

	session := wrapper.session
	event := wrapper.event

	for _, middleware := range manager.middlewares {
		if !middleware(event, session) { // rejected
			return
		}
	}

	if handler, exists := manager.handlers[event.EventType]; exists {
		handler(event, session)
		return
	}

	eventType := event.EventType
	typeSplit := strings.SplitN(eventType, ":", 2)
	if len(typeSplit) < 2 {
		return
	}
	namespace, action := typeSplit[0], typeSplit[1]

	if service, exists := manager.services[namespace]; exists {
		service.Handle(action, event, session)
	}
}

func (manager *Manager) cleanup() {

	manager.roomMutex.Lock()
	defer manager.roomMutex.Unlock()
	clear(manager.rooms)

	// clear rooms first so we don't access broken sessions within rooms

	manager.sessionMutex.Lock()

	sessionsToClose := make([]*Session, 0, len(manager.sessions))
	for _, session := range manager.sessions {
		sessionsToClose = append(sessionsToClose, session)
	}
	clear(manager.sessions)

	manager.sessionMutex.Unlock()

	for _, session := range sessionsToClose {
		session.Close(websocket.StatusGoingAway, "Server shutting down")
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
		session.Close(websocket.StatusNormalClosure, "session closed")
		session.cancel()
		manager.removeSessionFromAllRooms(session)
		delete(manager.sessions, clientID)
	}
}

func (manager *Manager) sessionCount() int {
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	return len(manager.sessions)
}

func (manager *Manager) GetRoom(roomID string) *Room {
	manager.roomMutex.RLock()
	defer manager.roomMutex.RUnlock()
	return manager.rooms[roomID]
}

func (manager *Manager) CreateRoom(roomID string) *Room {
	manager.roomMutex.Lock()
	defer manager.roomMutex.Unlock()
	room, exists := manager.rooms[roomID]
	if !exists {
		room = NewRoom()
		manager.rooms[roomID] = room
	}
	return room
}

func (manager *Manager) DeleteRoom(roomID string) {
	manager.roomMutex.Lock()
	defer manager.roomMutex.Unlock()
	delete(manager.rooms, roomID)
}

func (manager *Manager) removeSessionFromAllRooms(session *Session) {
	manager.roomMutex.Lock()
	defer manager.roomMutex.Unlock()

	for roomID, room := range manager.rooms {
		remainingSessionsCount := room.RemoveSession(session)
		if remainingSessionsCount <= 0 {
			delete(manager.rooms, roomID)
		}
	}
}
