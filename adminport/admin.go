// Other than mutation path and query path, most of the components in secondary
// index talk to each other via admin port. Admin port can also be used for
// collecting statistics, administering and managing cluster.
//
// An admin port is started as a server daemon and listens for request messages,
// where every request is serviced by sending back a response to the client.

package adminport

import (
	"errors"
	"net/http"
)

// errors codes

var ErrorInternal = errors.New("Internal error in adminport")

var ErrorInvalidRequest = errors.New("Invalid http request")

var ErrorInvalidServerType = errors.New("Invalid http server type for handler")

// Server API for adminport
type Server interface {

	// Start server routine and wait for incoming request, Register() and
	// Unregister() APIs cannot be called after starting the server.
	Start() error

	// Stop server routine. TODO: server routine shall quite only after
	// outstanding requests are serviced.
	Stop()
}

// Request API for server application to handle incoming request.
type Request interface {
	// Get http request from request packet.
	GetHttpRequest() *http.Request

	// Send a response back to the client.
	Send(response interface{}) error

	// Send error back to the client.
	SendError(error) error
}

// handler for Request
type RequestHandler interface{
        // extends http.Handler interface 
        http.Handler
        // sets server, which will be doing the actual handling
        SetServer(Server) error
        GetServer() Server
}
