package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/op-comm/op-comm/internal"
	"github.com/op-comm/op-comm/protocol"
)

type Manager struct {
	InboundBuffer  chan SessionEventWrapper
	sessions       map[string]*Session
	sessionMutex   sync.RWMutex
	clientIDMethod func(*http.Request) string
	handlers       map[string]EventHandler
	services       map[string]EventService
	rooms          map[string]Room
	roomMutex      sync.RWMutex
	roomFactory    func(id string) Room
	authenticator  RequestAuthenticator
	middlewares    []Middleware

	allowedOrigins []string

	logger *slog.Logger
}

func NewManager() *Manager {
	manager := &Manager{
		InboundBuffer: make(chan SessionEventWrapper),
		sessions:      make(map[string]*Session),
		sessionMutex:  sync.RWMutex{},
		handlers:      make(map[string]EventHandler),
		services:      make(map[string]EventService),
		clientIDMethod: func(request *http.Request) string {
			return uuid.NewString()
		},
		authenticator: nil,

		rooms:       make(map[string]Room),
		roomMutex:   sync.RWMutex{},
		middlewares: []Middleware{},

		allowedOrigins: []string{},
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)), // no logs
	}
	// create this after manager is already created, so we can inject its logger
	manager.roomFactory = func(id string) Room {
		return NewInMemoryRoom(id, manager.logger.With("room_id", id))
	}

	return manager
}

// TODO: replace with functional options pattern?
func (manager *Manager) SetClientIDMethod(method func(request *http.Request) string) {
	manager.clientIDMethod = method
}

func (manager *Manager) SetRoomFactory(factory func(roomID string) Room) {
	manager.roomFactory = factory
}

func (manager *Manager) SetAuthenticator(authenticator RequestAuthenticator) {
	manager.authenticator = authenticator
}

func (manager *Manager) SetLogger(logger *slog.Logger) {
	manager.logger = logger
}

func (manager *Manager) SetAllowedOrigins(origins []string) {
	manager.allowedOrigins = origins
}

// ends with the context and will simply just handle every inbound event
func (manager *Manager) Run(ctx context.Context) {
	defer manager.cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		//TODO: setup worker pool?
		case inboundEvent := <-manager.InboundBuffer:
			go manager.handleEvent(inboundEvent)
		}
	}
}

func (manager *Manager) HandleWSUpgradeRequest(writer http.ResponseWriter, request *http.Request) {

	var authState map[string]any
	if manager.authenticator != nil {

		var authError error
		authState, authError = manager.authenticator.Authenticate(request)
		if authError != nil {
			manager.logger.Warn("request failed authentication")
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write([]byte(authError.Error()))
			return
		}
	}

	clientID := manager.clientIDMethod(request)
	writer.Header().Set("Op-Comm-Session-ID", clientID)

	options := &websocket.AcceptOptions{}
	if len(manager.allowedOrigins) > 0 {
		options.OriginPatterns = manager.allowedOrigins
	}

	connection, err := websocket.Accept(writer, request, options)
	if err != nil {
		manager.logger.Warn("client failed to connect", "error", err)
		return
	}

	clientCtx, cancel := context.WithCancel(context.Background())
	clientSession := NewSession(clientID, connection, manager, cancel)

	if authState != nil {
		manager.logger.Debug("client connected with auth state:", "auth_state", authState)
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
	manager.logger.Debug("registered new action", "action", action)
	manager.handlers[action] = callback
}

// Note: this is NOT threadsafe, this must be used before the Run method
func (manager *Manager) RegisterEventService(namespace string, service EventService) {
	manager.services[namespace] = service
}

func (manager *Manager) GlobalBroadcast(event protocol.ServerSentEvent) {
	manager.logger.Debug("global broadcast", "event", event)
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	for _, session := range manager.sessions {
		session.Send(event)
	}
}

func (manager *Manager) GlobalBroadcastToOthers(event protocol.ServerSentEvent, senderID string) {
	manager.logger.Debug("global broadcast from sender", "event", event, "sender_id", senderID)
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	for _, session := range manager.sessions {
		if senderID != session.ID {
			session.Send(event)
		}
	}

}

func (manager *Manager) GlobalBroadcastExclude(event protocol.ServerSentEvent, sessionIdsToExclude []string) {
	manager.logger.Debug("global broadcast exclude", "event", event, "exclude_count", len(sessionIdsToExclude))
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	blackList := internal.SetFromList(sessionIdsToExclude)
	for _, session := range manager.sessions {
		if !blackList.Has(session.ID) {
			session.Send(event)
		}
	}
}

func (manager *Manager) SendToOnly(event protocol.ServerSentEvent, sessionIds []string) {
	manager.logger.Debug("sending event to sessions", "event", event, "session_count", len(sessionIds))
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	for _, id := range sessionIds {
		if session, exists := manager.sessions[id]; exists {
			session.Send(event)
		}
	}

}

func (manager *Manager) handleEvent(wrapper SessionEventWrapper) {

	session := wrapper.Session
	event := wrapper.Event

	manager.logger.Debug("handling event", "event", event, "session_id", session.ID)

	for _, middleware := range manager.middlewares {
		if !middleware(event, session) { // rejected
			//TODO: possibly add an optional rejection response?
			manager.logger.Debug("middleware rejected event", "event", event, "session_id", session.ID)
			return
		}
	}

	if handler, exists := manager.handlers[event.EventType]; exists {
		manager.logger.Debug("executing handler for event", "event", event)
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
		manager.logger.Debug("executing service for event", "event", event)
		service.Handle(action, event, session)
	}
}

func (manager *Manager) cleanup() {

	manager.roomMutex.Lock()

	manager.logger.Debug("cleanup started")
	clear(manager.rooms)
	manager.roomMutex.Unlock()

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
	manager.logger.Debug("added session", "session_id", session.ID)
}

func (manager *Manager) removeSession(clientID string) {
	manager.sessionMutex.Lock()
	session, sessionExists := manager.sessions[clientID]
	if sessionExists {
		delete(manager.sessions, clientID)
	}
	manager.sessionMutex.Unlock() //unlock as soon as possible
	if sessionExists {
		session.Close(websocket.StatusNormalClosure, "session closed")
		session.cancel()
		manager.removeSessionFromAllRooms(session)
		manager.logger.Debug("deleted session", "session_id", clientID)
	}
}

func (manager *Manager) sessionCount() int {
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	return len(manager.sessions)
}

func (manager *Manager) GetSessionIDs() []string {
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	sessionIDs := make([]string, len(manager.sessions))

	index := 0
	for id := range manager.sessions {
		sessionIDs[index] = id
		index++
	}
	return sessionIDs

}

func (manager *Manager) GetSession(id string) *Session {
	manager.sessionMutex.RLock()
	defer manager.sessionMutex.RUnlock()
	return manager.sessions[id]
}

func (manager *Manager) GetRoom(roomID string) Room {
	manager.roomMutex.RLock()
	defer manager.roomMutex.RUnlock()
	return manager.rooms[roomID]
}

func (manager *Manager) GetRoomIDs() []string {
	manager.roomMutex.RLock()
	defer manager.roomMutex.RUnlock()
	roomIDs := make([]string, len(manager.rooms))

	index := 0
	for id := range manager.rooms {
		roomIDs[index] = id
		index++
	}
	return roomIDs
}

func (manager *Manager) CreateRoom(roomID string) Room {
	manager.roomMutex.Lock()
	defer manager.roomMutex.Unlock()
	room, exists := manager.rooms[roomID]
	if !exists {
		room = manager.roomFactory(roomID)
		manager.rooms[roomID] = room
		manager.logger.Debug("created room", "room_id", roomID)
	}
	return room
}

func (manager *Manager) DeleteRoom(roomID string) {
	manager.roomMutex.Lock()
	defer manager.roomMutex.Unlock()
	manager.deleteRoomUnlocked(roomID)
}

func (manager *Manager) removeSessionFromAllRooms(session *Session) {
	manager.roomMutex.Lock()
	defer manager.roomMutex.Unlock()

	roomIDs := session.GetRooms()
	for _, roomID := range roomIDs {
		if room, exists := manager.rooms[roomID]; exists {
			remainingSessions := room.RemoveSession(session)
			if remainingSessions <= 0 {
				manager.deleteRoomUnlocked(roomID)
			}
		}
	}
}

// Only use when the roomMutex has been locked
// You should use manager.DeleteRoom unless the roomMutex is already locked
func (manager *Manager) deleteRoomUnlocked(roomID string) {
	room, exists := manager.rooms[roomID]
	if exists {
		room.Cleanup()
	}

	delete(manager.rooms, roomID)
	manager.logger.Debug("deleted room", "room_id", roomID)

}
