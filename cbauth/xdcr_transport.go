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
	"github.com/couchbase/cbauth"
	"net/http"
)

type XdcrRoundTripper struct {
	httpSlave   http.RoundTripper
	cbauthSlave http.RoundTripper
}

func (rt *XdcrRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	creds, err := cbauth.AuthWebCreds(req)
	if err != nil {
		return nil, err
	}

	if creds.Name() == "" || creds.Name() == "anonymous" {
		// if no credentials are provided, this is a request on local cluster
		// call cbauth to automatically fill in auth info
		return rt.cbauthSlave.RoundTrip(req)
	} else {
		// if credentials are provided, bypass cbauth. This is needed when dealing with remote cluster
		return rt.httpSlave.RoundTrip(req)
	}
}

// WrapHTTPTransport constructs http transport that automatically does
// SetRequestAuth for requests when needed.
func WrapHTTPTransport(transport http.RoundTripper) http.RoundTripper {
	return &XdcrRoundTripper{
		httpSlave:   transport,
		cbauthSlave: cbauth.WrapHTTPTransport(transport, nil),
	}
}
