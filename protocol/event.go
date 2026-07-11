package protocol

import "encoding/json"

type Event struct {
}

type Request struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

type Response struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Error string `json:"error,omitempty"`
	Data  any    `json:"data,omitempty"`
}

type Broadcast struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}
