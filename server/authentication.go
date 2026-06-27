package server

import "net/http"

type RequestAuthenticator interface {
	Authenticate(request *http.Request) (map[string]any, error)
}

type RoomAuthorizer interface {
	Authorize(session *Session, room *Room, action string) error
}