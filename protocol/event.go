package protocol

import "encoding/json"

type Event struct {
}

type ClientSentEvent struct {
	EventType string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

type ServerSentEvent struct {
	EventType string      `json:"type"`
	Data      interface{} `json:"data"`
	//metadata returned to client can be added here:
	
}
