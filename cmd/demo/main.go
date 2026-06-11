package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/op-comm/op-comm/protocol"
	"github.com/op-comm/op-comm/server"
)

func main() {
	manager := server.NewManager()
	manager.SetAllowedOrigins([]string{"http://localhost:5173"})
	manager.On("test:echo", func(event *protocol.ClientSentEvent, session *server.Session) {
		fmt.Printf("Receieved echo request from %s: %v\n", session.ID, event.Data)

		session.OutputBuffer <- protocol.ServerSentEvent{
			EventType: "test:echo_response",
			Data:      event.Data,
		}
	})

	http.HandleFunc("/ws", manager.HandleWSUpgradeRequest)

	ctx := context.Background()

	go manager.Run(ctx)

	fmt.Println("Server Started at ws://localhost:8080/ws")
	serverErr := http.ListenAndServe(":8080", nil)
	if serverErr != nil {
		panic(serverErr)
	}

}
