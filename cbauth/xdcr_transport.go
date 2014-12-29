// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package cbauth

import (
	"net/http"
	"github.com/couchbase/cbauth"
)

type XdcrRoundTripper struct {
        slave http.RoundTripper
}

func (rt *XdcrRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
        creds, err := cbauth.AuthWebCreds(req)
        if err != nil {
                return nil, err
        }
        
        if creds.Name() == "" {
          	req = dupRequest(req)
        	// if no credentials are provides, this is a request on local cluster
        	// call cbauth to automatically fill in auth info
       		if err := cbauth.SetRequestAuth(req); err != nil {
                return nil, err
      		}
      	}
        return rt.slave.RoundTrip(req)
}

// WrapHTTPTransport constructs http transport that automatically does
// SetRequestAuth for requests when needed. 
func WrapHTTPTransport(transport http.RoundTripper) http.RoundTripper {
        return &XdcrRoundTripper{
                slave: transport,
        }
}

func dupRequest(req *http.Request) *http.Request {
        rv := *req
        rv.Header = dupHeader(req.Header)
        rv.Trailer = dupHeader(req.Trailer)
        return &rv
}

func dupHeader(h http.Header) http.Header {
        rv := make(http.Header)
        for k, v := range h {
                rv[k] = duplicateStringsSlice(v)
        }
        return rv
}

func duplicateStringsSlice(in []string) []string {
        return append([]string{}, in...)
}