// Copyright (c) 2013-2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// admin server to handle admin and system messages.
//
// Example server {
//      reqch  := make(chan adminport.Request)
//      server := adminport.NewHTTPServer("projector", "localhost:9999", "/adminport", reqch)
//      server.Register(&protobuf.RequestMessage{})
//
//      loop:
//      for {
//          select {
//          case req, ok := <-reqch:
//              if ok {
//                  msg := req.GetMessage()
//                  // interpret request and compose a response
//                  respMsg := &protobuf.ResponseMessage{}
//                  err := msg.Send(respMsg)
//              } else {
//                  break loop
//              }
//          }
//      }
// }

// TODO: IMPORTANT:
//  Go 1.3 is supposed to have graceful shutdown of http server.
//  Refer https://code.google.com/p/go/issues/detail?id=4674

package adminport

import (
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"net/http"
	"sync"
)
import _ "expvar"

var logger_server *log.CommonLogger = log.NewLogger("HttpServer", log.DefaultLoggerContext)

type httpServer struct {
	mu        sync.RWMutex   // handle concurrent updates to this object
	srv       *http.Server   // http server
	urlPrefix string         // URL path prefix for adminport
	reqch     chan<- Request // request channel back to application

	logPrefix string
}

// NewHTTPServer creates an instance of admin-server. Start() will actually
// start the server.
func NewHTTPServer(name, connAddr, urlPrefix string, reqch chan<- Request, handler RequestHandler) Server {

	s := &httpServer{
		reqch:     reqch,
		urlPrefix: urlPrefix,
		logPrefix: fmt.Sprintf("[%s:%s]", name, connAddr),
	}
	logger_server.Infof("%v new http server %v %v %v\n", s.logPrefix, name, connAddr, urlPrefix)
	http.Handle(s.urlPrefix, handler)
	handler.SetServer(s)
	s.srv = &http.Server{
		Addr:           connAddr,
		Handler:        nil,
		ReadTimeout:    base.AdminportReadTimeout,
		WriteTimeout:   base.AdminportWriteTimeout,
		MaxHeaderBytes: 1 << 20,
	}
	return s
}

// Start is part of Server interface.
func (s *httpServer) Start() chan error {
	errCh := make(chan error, 1)

	// Server routine
	go func() {
		defer s.shutdown()

		logger_server.Infof("%s starting ...\n", s.logPrefix)
		// ListenAndServe blocks and returns a non-nil error if something wrong happens
		err := s.srv.ListenAndServe()
		logger_server.Errorf("%s exited with error %v\n", s.logPrefix, err)
		errCh <- err
	}()
	return errCh
}

// Stop is part of Server interface. Once stopped, Start() cannot be called again
func (s *httpServer) Stop() {
	s.shutdown()
	logger_server.Infof("%s ... stopped\n", s.logPrefix)
}

func (s *httpServer) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.srv != nil {
		s.srv.Close()
		close(s.reqch)
		s.srv = nil
	}
}

// handle incoming request.
func (s *httpServer) systemHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	// Fault-tolerance. No need to crash the server in case of panic.
	defer func() {
		if r := recover(); r != nil {
			logger_server.Errorf("adminport.request.recovered `%v`\n", r)
		} else if err != nil {
			logger_server.Errorf("%v\n", err)
		}
	}()

	execFunc := func() error {
		waitch := make(chan interface{}, 1)
		// send and wait
		s.reqch <- &httpAdminRequest{srv: s, req: r, waitch: waitch}
		val := <-waitch

		switch v := (val).(type) {
		case error:
			http.Error(w, v.Error(), http.StatusInternalServerError)
			err = fmt.Errorf("%v, %v", ErrorInternal, v)
			logger_server.Errorf("%v", err)
			return err
		case *Response:
			if v.TagPrintingBody && logger_server.GetLogLevel() >= log.LogLevelDebug {
				bodyRedact := base.TagUDBytes(base.DeepCopyByteArray(v.Body))
				logger_server.Debugf("Response from goxdcr rest server. status=%v\n body in string form=%v\n", v.StatusCode, string(bodyRedact))
			} else {
				logger_server.Debugf("Response from goxdcr rest server. status=%v\n body in string form=%v", v.StatusCode, string(v.Body))
			}

			w.Header().Set(base.ContentType, base.JsonContentType)
			w.WriteHeader(v.StatusCode)
			w.Write(v.Body)
		}
		return nil
	}
	err = base.ExecWithTimeout(execFunc, base.KeepAlivePeriod, logger_server)
	if err != nil {
		logger_server.Errorf(err.Error())
	}
}

// concrete type implementing Request interface
type httpAdminRequest struct {
	srv    *httpServer
	req    *http.Request
	waitch chan interface{}
}

// GetMessage is part of Request interface.
func (r *httpAdminRequest) GetHttpRequest() *http.Request {
	return r.req
}

// Send is part of Request interface.
func (r *httpAdminRequest) Send(response *Response) error {
	r.waitch <- response
	close(r.waitch)
	return nil
}

// SendError is part of Request interface.
func (r *httpAdminRequest) SendError(err error) error {
	r.waitch <- err
	close(r.waitch)
	return nil
}

//xdcr implementaton of RequestHandler
type Handler struct {
	server *httpServer
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if logger_server.GetLogLevel() >= log.LogLevelDebug {
		rr := base.CloneAndTagHttpRequest(r) // rr == redactedRequest
		logger_server.Debugf("Request received from ServeHTTP. r=%v\nwith path, %v and method, %v\n", rr, rr.URL.Path, rr.Method)
	}
	h.server.systemHandler(w, r)
}

func (h *Handler) SetServer(s Server) error {
	server, ok := s.(*httpServer)
	if !ok {
		return ErrorInvalidServerType
	}
	h.server = server
	return nil
}

func (h *Handler) GetServer() Server {
	return h.server
}
