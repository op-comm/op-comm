package server_test

import (
	"context"
	"slices"
	"testing"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/server"
	"github.com/op-comm/op-comm/testutil"
)

func ConnectAndFetchSession(t *testing.T, manager *server.Manager, wsURL string, existingIds []string) (*websocket.Conn, *server.Session) {
	ctx := context.Background()
	clientConn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("Unexpected Server Error: Failed to connect to ws: %v", err)
	}

	var serverSession *server.Session
	managerCreatedSession := testutil.PollEvent(t, testutil.SMALL_DELAY, 10, func() bool {
		sessionMutex := server.GetManagerSessionMutex(manager)
		sessionMutex.RLock()
		defer sessionMutex.RUnlock()
		for _, s := range server.GetManagerSessionMap(manager) {
			if slices.Contains(existingIds, s.ID) {
				continue
			}
			serverSession = s
			return true
		}
		return false
	})

	if !managerCreatedSession {
		t.Fatal("Server failed to create session in time")
	}

	return clientConn, serverSession
}

