package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
)

// the output buffer should have a max size that determines when it's disconnected
// because if the server places X events without the client reading from it in time,
// it's likely a network issue and the session should be removed
var MAX_BUFFER_EVENTS_BEFORE_DISCONNECT int = 512

var WRITE_TIMEOUT = 10 * time.Second

type SessionEventWrapper struct {
	Event   *protocol.ClientSentEvent
	Session *Session
}

type Session struct {
	ID           string
	connection   *websocket.Conn
	Manager      *Manager
	OutputBuffer chan protocol.ServerSentEvent
	cancel       context.CancelFunc
	state        map[string]any
	stateMutex   sync.RWMutex
}

func NewSession(id string, connection *websocket.Conn, manager *Manager, cancel context.CancelFunc) *Session {
	return &Session{
		ID:           id,
		connection:   connection,
		Manager:      manager,
		cancel:       cancel,
		OutputBuffer: make(chan protocol.ServerSentEvent, MAX_BUFFER_EVENTS_BEFORE_DISCONNECT),
		state:        make(map[string]any),
		stateMutex:   sync.RWMutex{},
	}
}

// reads from socket
// our socket is the actual network connection representaiton, so once that errors
// we can actually remove our session from the manager and clean it up since it is having
// issues
func (session *Session) readPump() {
	defer session.Manager.removeSession(session.ID)
	for {

		_, byteData, err := session.connection.Read(context.Background())
		if err != nil {
			break
		}
		var event protocol.ClientSentEvent

		unmarshalErr := json.Unmarshal(byteData, &event)
		if unmarshalErr != nil {
			fmt.Printf("Invalid Message\n")
			continue //ignore invalid messages
		}

		session.Manager.logger.Debug("received data from session", "session_id", session.ID, "event", event)
		
		session.Manager.InboundBuffer <- SessionEventWrapper{
			Event:   &event,
			Session: session,
		}
	}
}

// writes to socket
func (session *Session) writePump(ctx context.Context) {
	for {

		select {
		case <-ctx.Done():
			return

		case event := <-session.OutputBuffer:
			byteData, marshalErr := json.Marshal(event)
			if marshalErr != nil {
				fmt.Printf("Failed to marshal")
				continue
			}
			writeCtx, cancel := context.WithTimeout(ctx, WRITE_TIMEOUT)
			err := session.connection.Write(writeCtx, websocket.MessageText, byteData)
			cancel()
			if err != nil {
				fmt.Printf("Failed to write to socket")
				// when writing fails we want to close the socket, then the read goroutine will detect the closure,
				// stop itself and cleanup
				session.Close(websocket.StatusAbnormalClosure, "write to socket timeout")
				return
			}
		}
	}
}

func (session *Session) Close(status websocket.StatusCode, reason string) {
	session.connection.Close(status, reason)
}

func (session *Session) Get(key string) (any, bool) {
	session.stateMutex.RLock()
	defer session.stateMutex.RUnlock()
	value, exists := session.state[key]
	return value, exists
}

func (session *Session) Set(key string, value any) {
	session.stateMutex.Lock()
	defer session.stateMutex.Unlock()
	session.state[key] = value
}

func (session *Session) CopyIntoState(pairs map[string]any) {
	session.stateMutex.Lock()
	defer session.stateMutex.Unlock()

	for key, value := range pairs {
		session.state[key] = value
	}
}

func (session *Session) Send(event protocol.ServerSentEvent) {
	select {
	case session.OutputBuffer <- event:
		session.Manager.logger.Debug("sending event to session", "session_id", session.ID, "event", event)
	default:
		// reaching here means the output buffer is full
		// which likely points to network issues on the client
		// we can disconnect here to prevent further blocking
		session.Close(websocket.StatusAbnormalClosure, "Too many messages in buffer")
	}
}

func (session *Session) Reply(request *protocol.ClientSentEvent, data interface{}, err string) {
	response := protocol.ServerSentEvent{
		EventType: request.EventType + ":reply",
		RequestID: request.RequestID,
		Data:      data,
		Error:     err,
	}
	session.Send(response)

}
