package server

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/op-comm/op-comm/protocol"
)

type Manager struct {
	InboundBuffer chan *protocol.ClientSentEvent
	//TODO: store the clients connected to the manager
	// so we can properly handle messages between clients
	// these can be represented as "sessions" each session
	// is a connected client
	// the string key should be the session id however that is stored
	sessions map[string]*Session

	// handles how clients are identified (the key for sessions map)
	clientIDMethod func(*http.Request) string
}

func NewManager() *Manager {
	return &Manager{
		InboundBuffer: make(chan *protocol.ClientSentEvent),
		sessions:      make(map[string]*Session),
		clientIDMethod: func(request *http.Request) string {
			return uuid.NewString()
		},
	}
}

//TODO: replace with functional options pattern?
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

	//TODO: add session to manager map

	go clientSession.HandleOutboundEventsFromManager()
	// since each request is its own goroutine we can use the current one to handle reading from
	// the socket
	clientSession.SendIncomingEventsToManager()
}

func (manager *Manager) handleEvent(event protocol.ClientSentEvent) {
	//TODO: process events
}

func (manager *Manager) cleanup() {
	//TODO: implement cleanup process
}
