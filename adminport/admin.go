// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
//
// An admin port is started as a server daemon and listens for request messages,
// where every request is serviced by sending back a response to the client.

package adminport

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
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

type ContentType int

const (
	ContentTypeJson           ContentType = iota
	ContentTypeText           ContentType = iota
	ContentTypePrometheusText ContentType = iota
)

func (ct ContentType) String() string {
	switch ct {
	case ContentTypeJson:
		return base.JsonContentType
	case ContentTypeText:
		return base.PlainTextContentType
	case ContentTypePrometheusText:
		return fmt.Sprintf("%v%v", base.PlainTextContentType, base.PrometheusTextContentType)
	default:
		panic("FIXME")
	}
}

// response returned from request handler
type Response struct {
	StatusCode  int
	Body        []byte
	ContentType ContentType

	// If set, then the body contains sensitive data and should be redacted in debug log
	TagPrintingBody bool
}
