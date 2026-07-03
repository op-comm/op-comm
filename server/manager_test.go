package server_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/server"
	"github.com/op-comm/op-comm/testutil"
)

func TestManager_WSRequest(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	connection, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "")

	expectedSessionCount := 1
	var actualSessionCount int
	sessionCountTestPassed := testutil.PollEvent(t, 50*time.Millisecond, 10, func() bool {
		actualSessionCount = server.GetManagerSessionCount(manager)
		return expectedSessionCount == actualSessionCount
	})

	if !sessionCountTestPassed {
		t.Fatalf("Expected manager to have %v session(s), manager has %v", expectedSessionCount, actualSessionCount)
	}
}

//TODO: test data lifecycle through manager (from inboundbuffer to socket)

// TODO: add more session management test
	//TODO: test that session is removed when socket is closed
//TODO: add stress test?


func TestManager_HandlesCustomEvent(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()
	
	 eventRan := make(chan struct{}, 1)
	manager.On("toggle", func(event *protocol.ClientSentEvent, session *server.Session) {
		// Will only ever send one signal (prevents blocking/errors for multiple calls)
		select  {
		case eventRan <- struct{}{}:
		default:
		}
	})
	
	ctx, cancel := context.WithCancel(context.Background()) 
	defer cancel()
	go manager.Run(ctx)

	clientConn := testutil.ConnectToServer(t, manager, wsURL)

	testutil.WriteToConnection(t, clientConn, []byte(`
		{ "type": "toggle" }
	`))

	select {
	case <-eventRan:
		return
	case <-time.After(1 * time.Second):
		t.Fatalf("Expected custom event to be ran")
	}
}

func TestManager_HandlesCustomService(t *testing.T) {
	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()
	
	eventRan := make(chan struct{}, 1)
	customService := server.EventServiceFunc(func(action string, event *protocol.ClientSentEvent, session *server.Session) {
		if action == "toggle" {
			select  {
			case eventRan <- struct{}{}:
			default:
			}
		}
	})

	manager.RegisterEventService("bool", customService)
	
	ctx, cancel := context.WithCancel(context.Background()) 
	defer cancel()
	go manager.Run(ctx)

	clientConn := testutil.ConnectToServer(t, manager, wsURL)

	testutil.WriteToConnection(t, clientConn, []byte(`
		{ "type": "bool:toggle" }
	`))
	
	select {
	case <-eventRan:
		return
	case <-time.After(1 * time.Second):
		t.Fatalf("Expected custom event to be ran")
	}

}

func TestManager_DeniesUnauthorizedRequest(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	manager.SetAuthenticator(rejectAllAuthenticator{})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, response, err := websocket.Dial(ctx, wsURL, nil)

	if err == nil {
		t.Fatal("expected request to fail, but it succeeded")
	}

	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Expected 401 response status, got %v", response.Status)
	}

}

func TestManager_AcceptsAuthorizedRequest(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	manager.SetAuthenticator(acceptAllAuthenticator{})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, response, err := websocket.Dial(ctx, wsURL, nil)

	if err != nil {
		t.Fatalf("expected no error from request: got %v", err)
	}

	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("Expected 101 response status, got %v", response.Status)
	}

	managerCreatedSession := testutil.PollEvent(t, testutil.SMALL_DELAY, 5, func() bool {
		return server.GetManagerSessionCount(manager) >= 1
	})
	if !managerCreatedSession {
		t.Fatal("expected session to be created")
	}

}

func TestManager_AcceptsWhenNoAuthenticator(t *testing.T) {

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, response, err := websocket.Dial(ctx, wsURL, nil)

	if err != nil {
		t.Fatalf("expected no error from request: got %v", err)
	}

	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("Expected 101 response status, got %v", response.Status)
	}

	managerCreatedSession := testutil.PollEvent(t, testutil.SMALL_DELAY, 5, func() bool {
		return server.GetManagerSessionCount(manager) >= 1
	})
	if !managerCreatedSession {
		t.Fatal("expected session to be created")
	}

}

type rejectAllAuthenticator struct{}

func (_ rejectAllAuthenticator) Authenticate(_ *http.Request) (map[string]any, error) {
	return nil, errors.New("Rejected")
}

type acceptAllAuthenticator struct{}

func (_ acceptAllAuthenticator) Authenticate(_ *http.Request) (map[string]any, error) {
	return nil, nil
}

func TestManager_MiddlewareAllowsEvent(t *testing.T) {
	middlewareRan := make(chan struct{}, 1)
	eventRan := make(chan struct{}, 1)

	allowMiddleware := func(event *protocol.ClientSentEvent, session *server.Session) bool {
		select  {
			case middlewareRan<-struct{}{}:
			default:
		}
		return true
	}

	event := func(event *protocol.ClientSentEvent, session *server.Session) {
		select  {
			case eventRan<-struct{}{}:
			default:
		}
	}

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	manager.On("test_event", event)
	manager.UseMiddleware(allowMiddleware)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Run(ctx)
	clientConn := testutil.ConnectToServer(t, manager, wsURL)

	testutil.WriteToConnection(t, clientConn, []byte(`
		{ "type": "test_event" }
	`))

	select {
		case <-middlewareRan:
		case <-time.After(3 * time.Second):
			t.Fatal("Middleware did not run when expected to")
	}

	select {
		case <-eventRan:
		case <-time.After(3 * time.Second):
			t.Fatal("Event did not run when expected to")
	}
}

func TestManager_MiddlewareDeniesEvent(t *testing.T) {
	middlewareRan := make(chan struct{}, 1)
	eventBuffer := make(chan *protocol.ClientSentEvent, 1)

	denyMiddleware := func(event *protocol.ClientSentEvent, session *server.Session) bool {
		select  {
			case middlewareRan<-struct{}{}:
			default:
		}
		return event.EventType != "denied_event"
	}

	eventHandler := func(event *protocol.ClientSentEvent, session *server.Session) {
		select  {
		case eventBuffer<-event:
			default:
		}
	}
	

	manager, wsURL, cleanup := testutil.SetupTestServer(t)
	defer cleanup()

	manager.On("denied_event", eventHandler)
	manager.On("allowed_event", eventHandler)
	manager.UseMiddleware(denyMiddleware)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Run(ctx)
	clientConn := testutil.ConnectToServer(t, manager, wsURL)

	testutil.WriteToConnection(t, clientConn, []byte(`
		{ "type": "denied_event" }
	`))

	testutil.WriteToConnection(t, clientConn, []byte(`
		{ "type": "allowed_event" }
	`))
	select {
		case <-middlewareRan:
		case <-time.After(3 * time.Second):
			t.Fatal("Middleware did not run when expected to")
	}

	select {
	case eventThatRan := <- eventBuffer:
		if eventThatRan.EventType == "denied_event" {
			t.Fatalf("Event ran when expected not to")
		}
		if eventThatRan.EventType != "allowed_event" {
			t.Fatalf("allowed_event did not run when expected to")
		}
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected allowed_event to run, did not run within duration")
	}

}
