package protocol

type Event struct {
}

type ClientSentEvent struct {
	eventType string
	data      interface{}
}

type ServerSentEvent struct {
	ClientSentEvent
}