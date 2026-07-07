package room

type BaseRoomRequest struct {
	RoomID string `json:"room_id"`
}
type BaseRoomReply struct {
	RoomID string `json:"room_id"`
}
type BaseRoomBroadcast struct {
	RoomID string `json:"room_id"`
	UserID string `json:"user_id,omitempty"`
}

type RoomListReply struct {
	Rooms []string `json:"rooms"`
}
