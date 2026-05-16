## Session

The Session struct holds the actual connection to the websocket itself. It is responsible for writing and reading to the socket. Each session represents a client connection, and each client connection gets its own socket and therefore its own Session struct.

### Lifecycle

When a client connects to the server, a websocket connection is formed, a session struct instance is created and it's readPump & writePump start across two different goroutines.

**readPump**: The readPump is an infinite loop that forwards data read from the websocket connection to a channel in the manager.
The only way the readPump ends is when an error occurs from reading the websocket connection. This can occur when the connection has been closed.

**writePump**: The writePump is an infinite loop that forwards data taken from the session's OutputBuffer.
The Session struct has limited customization other than a constants that determine handling when to error.
A cancellable context is passed to the write pump and can be canceld via **session.cancel()**.
It can also end if the write request exceeds the **WRITE_TIMEOUT**.

### Cleanup

A session must be cleaned up as well as the websocket itself upon deletion.

The **manager.removeSession** Will remove a session from it's concurrent map and will call the session's **close** and **cancel** method.

The **manager.removeSession** Will be called by the **readPump** after it exits this loop. This would happen during an error response from the read request, and would automatically cleanup the **writePump** via the cancel call or the eventual failed write request.

In the case the **writePump** errors and exits, it will also close the socket to ensure the **readPump** is also exited.

The **readPump** must be closed and exited to handle cleaning up the manager session connection.

## Manager

The Manager struct acts as a traffic controller for all data, and manages the sessions. The manager stores all of the sessions in a thread-safe map.
All custom functionality should be exposed via the manager,

## Request Lifecycle

A client will initially connect with the server and establish a websocket connection.
With a websocket connection the client will send data to the server.
Each session has a goroutine with the purpose of forwarding the data to the manager.
The manager will handle the data based on the event.
THe manager will tell the appropriate sessions to write data back to their socket.
