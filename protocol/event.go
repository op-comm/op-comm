package protocol

type Event struct {
}

type ClientSentEvent struct {
	EventType string
	Data      interface{}
}

type ServerSentEvent struct {
	ClientSentEvent
	//metadata returned to client can be added here:

}
