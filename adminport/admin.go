// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
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
	Start() chan error

	// Stop server routine. TODO: server routine shall quite only after
	// outstanding requests are serviced.
	Stop()
}

// Request API for server application to handle incoming request.
type Request interface {
	// Get http request from request packet.
	GetHttpRequest() *http.Request

	// Send a response back to the client.
	Send(response *Response) error

	// Send error back to the client.
	SendError(error) error
}

// handler for Request
type RequestHandler interface {
	// extends http.Handler interface
	http.Handler
	// sets server, which will be doing the actual handling
	SetServer(Server) error
	GetServer() Server
}

// response returned from request handler
type Response struct {
	StatusCode int
	Body       []byte

	// If set, then the body contains sensitive data and should be redacted in debug log
	TagPrintingBody bool
}
