package protocol

import "encoding/json"

type Event struct {
}

type ClientSentEvent struct {
	EventType string          `json:"type"`
	Data      json.RawMessage `json:"data,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
}

type ServerSentEvent struct {
	EventType string          `json:"type"`
	Data      json.RawMessage `json:"data,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
	Error     string          `json:"error,omitempty"`
}
