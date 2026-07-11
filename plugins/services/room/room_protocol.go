package room

// - create ----
type RoomCreateRequest struct {
	RoomID string `json:"room_id"`
}
type RoomCreateResponse struct {
	RoomID string `json:"room_id"`
}
type RoomCreatedBroadcast struct {
	RoomID string `json:"room_id"`
}

// - delete ----
type RoomDeleteRequest struct {
	RoomID string `json:"room_id"`
}
type RoomDeleteResponse struct {
	RoomID string `json:"room_id"`
}
type RoomDeletedBroadcast struct {
	RoomID string `json:"room_id"`
}

// - join ----
type RoomJoinRequest struct {
	RoomID string `json:"room_id"`
}
type RoomJoinResponse struct {
	RoomID string `json:"room_id"`
}
type RoomUserJoinedBroadcast struct {
	RoomID string `json:"room_id"`
	UserID string `json:"user_id"`
}

// - leave ----
type RoomLeaveRequest struct {
	RoomID string `json:"room_id"`
}
type RoomLeaveResponse struct {
	RoomID string `json:"room_id"`
}
type RoomUserLeftBroadcast struct {
	RoomID string `json:"room_id"`
	UserID string `json:"user_id"`
}

// - list ----
type RoomListRequest struct {
	RoomID string `json:"room_id"`
}
type RoomListResponse struct {
	Rooms []string `json:"rooms"`
}

type Command string
type Event string
type Error string

// the commands are only the action because the namespace is already detached
// then the handler is ran based on the command
const (
	CommandCreate Command = "create"
	CommandJoin   Command = "join"
	CommandLeave  Command = "leave"
	CommandDelete Command = "delete"
	CommandList   Command = "list"
)

// broadcasted to session
const (
	EventRoomCreated Event = "room:created"
	EventRoomDeleted Event = "room:deleted"
	EventUserJoined  Event = "room:user_joined"
	EventUserLeft    Event = "room:user_left"
)

const (
	ErrMissingPayload Error = "ERR_MISSING_PAYLOAD"
	ErrRoomNotFound   Error = "ERR_ROOM_NOT_FOUND"
	ErrInvalidJSON    Error = "ERR-INVALID_JSON"
)
