package server

import (
	"context"

	"github.com/op-comm/op-comm/protocol"
)


type Manager struct {
	InboundBuffer chan *protocol.ClientSentEvent
	//TODO: store the clients connected to the manager
	// so we can properly handle messages between clients
	// these can be represented as "sessions" each session
	// is a connected client
	// the string key should be the session id, however that is stored
	sessions map[string]*Session

	// handles how clients our identified the key for sessions map
	clientIDMethod func() string
}

func NewManager() *Manager {
	return &Manager{
		InboundBuffer: make(chan *protocol.ClientSentEvent),
		sessions:      make(map[string]*Session),
		clientIDMethod: func() string {
			//TODO: implement UUID?
			return ""
		},
	}
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

func (manager *Manager) handleEvent(event protocol.ClientSentEvent) {
	//TODO: process events
}

func (manager *Manager) cleanup() {
	//TODO: implement cleanup process
}
