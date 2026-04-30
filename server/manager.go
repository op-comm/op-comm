package server

import "context"

//TODO: create event struct
// define in protocol / type package
// it should be accessible to future client packages aswell
type Event struct {}

type Session struct {}

type Manager struct {
	InboundBuffer chan *Event
	//TODO: store the clients connected to the manager
	// so we can properly handle messages between clients
	// these can be represented as "sessions" each session
	// is a connected client
	// the string key should be the session id, however that is stored
	sessions map[string]*Session

	// handles how clients our identified the key for sessions map
	clientIDMethod func() string
}

func NewManager() *Manager{
	return &Manager {
		InboundBuffer: make(chan *Event),
		sessions: make(map[string]*Session),
		clientIDMethod: func() string {
			//TODO: implement UUID?
			return ""
		},
	}
}

// Handles the main loop, ends with the context and will simply just handle every inbound event
func (manager *Manager) Run(ctx context.Context) {
	defer manager.cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case inboundEvent := <- manager.InboundBuffer:
			manager.handleEvent(*inboundEvent)
		}
	}
}


func (manager *Manager) handleEvent(event Event){
	//TODO: process events
}

func(manager *Manager) cleanup(){
	//TODO: implement cleanup process
}
