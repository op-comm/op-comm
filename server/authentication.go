package server

import "net/http"

type Authenticator interface {
	Authenticate(request *http.Request) (map[string]any, error)
}
