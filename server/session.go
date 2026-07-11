package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/internal"
	"github.com/op-comm/op-comm/protocol"
)

// the output buffer should have a max size that determines when it's disconnected
// because if the server places X events without the client reading from it in time,
// it's likely a network issue and the session should be removed
var MAX_BUFFER_EVENTS_BEFORE_DISCONNECT int = 512
var MAX_BYTES_PER_MESSAGE int64 = 4096 // 4 kilobytes
var WRITE_TIMEOUT = 10 * time.Second

type SessionEventWrapper struct {
	Event   *protocol.Request
	Session *Session
}

type Session struct {
	ID           string
	connection   *websocket.Conn
	Manager      *Manager
	OutputBuffer chan []byte
	cancel       context.CancelFunc

	RoomIDs *internal.ConcurrentSet[string]

	state      map[string]any
	stateMutex sync.RWMutex

	closeOnce sync.Once
	isClosed  atomic.Bool // allows us to deny fast on requests that sneak in during closing
}

func NewSession(id string, connection *websocket.Conn, manager *Manager, cancel context.CancelFunc) *Session {
	return &Session{
		ID:           id,
		connection:   connection,
		Manager:      manager,
		cancel:       cancel,
		OutputBuffer: make(chan []byte, MAX_BUFFER_EVENTS_BEFORE_DISCONNECT),
		state:        make(map[string]any),
		stateMutex:   sync.RWMutex{},
		RoomIDs:      internal.NewConcurrentSet[string](),
	}
}

// reads from socket
// our socket is the actual network connection representaiton, so once that errors
// we can actually remove our session from the manager and clean it up since it is having
// issues
func (session *Session) readPump() {

	// we don't need to attempt to close after our loop because the socket closing causes the loop to break
	// in the first place
	defer session.Cleanup()

	session.connection.SetReadLimit(MAX_BYTES_PER_MESSAGE)

	for {

		if session.isClosed.Load() {
			return
		}
		_, byteData, err := session.connection.Read(context.Background())
		if err != nil {
			closeStatus := websocket.CloseStatus(err)
			if closeStatus == websocket.StatusNormalClosure || closeStatus == websocket.StatusGoingAway {
				session.Manager.logger.Debug("client disconnected", "session_id", session.ID)
			} else {
				session.Manager.logger.Error("failed to read from socket", "error", err, "session_id", session.ID)
			}
			break
		}
		var event protocol.Request

		unmarshalErr := json.Unmarshal(byteData, &event)
		if unmarshalErr != nil {
			session.Manager.logger.Warn("received invalid message", "error", unmarshalErr, "session_id", session.ID)
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

		if session.isClosed.Load() {
			return
		}

		select {
		case <-ctx.Done():
			return

		case byteData := <-session.OutputBuffer:
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
	// we don't need to worry about attempting to close multiple times because if it fails that means the connection already is destroyed
	session.closeOnce.Do(func() {
		session.isClosed.Store(true)
		session.Manager.logger.Debug("closing socket", "status", status, "reason", reason, "session_id", session.ID)
		session.connection.Close(status, reason)
	})
}

func (session *Session) Cleanup() {
	session.Manager.removeSession(session.ID)
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

func (session *Session) Send(data any) {

	if session.isClosed.Load() {
		return
	}

	byteData, err := json.Marshal(data)
	if err != nil {
		session.Manager.logger.Error("Failed to marshal outgoing message", "error", err, "data", data)
		return
	}

	select {
	case session.OutputBuffer <- byteData:
	default:
		// reaching here means the output buffer is full
		// which likely points to network issues on the client
		// we can disconnect here to prevent further blocking
		session.Close(websocket.StatusAbnormalClosure, "Too many messages in buffer")
	}
}

func (session *Session) Respond(request *protocol.Request, data interface{}, err string) {

	if session.isClosed.Load() {
		return
	}

	response := protocol.Response{
		Type:  request.Type + ":response",
		ID:    request.ID,
		Data:  data,
		Error: err,
	}
	session.Send(response)

}

func (session *Session) GetRooms() []string {
	return session.RoomIDs.AsList()
}
func (session *Session) IsInRoom(roomID string) bool {
	return session.RoomIDs.Has(roomID)
}

func (session *Session) AddRoom(roomID string) {
	session.RoomIDs.Add(roomID)
}

func (session *Session) RemoveRoom(roomID string) {
	session.RoomIDs.Delete(roomID)
}
