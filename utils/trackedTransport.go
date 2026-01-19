/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package utils

import (
	"io"
	"net/http"
	"sync/atomic"
)

type countingReadCloser struct {
	io.ReadCloser
	counter *int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if n > 0 {
		atomic.AddInt64(c.counter, int64(n))
	}
	return n, err
}

// onetimeTrackedTransport is used where the Transport needs to be overridden by a single REST call and then
// discarded
type onetimeTrackedTransport struct {
	Transport http.RoundTripper
	BytesSent *int64
	BytesRecv *int64
}

func (o *onetimeTrackedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Track request size
	// XDCR right now only sends non-chunked requests. This need to be accounted for if that changes in the future
	if req.Body != nil && o.BytesSent != nil && req.ContentLength > 0 {
		atomic.AddInt64(o.BytesSent, req.ContentLength)
	}

	resp, err := o.Transport.RoundTrip(req)

	if resp != nil && resp.Body != nil {
		isChunked := false
		for _, enc := range resp.TransferEncoding {
			if enc == "chunked" {
				isChunked = true
				break
			}
		}

		if !isChunked && resp.ContentLength > 0 {
			atomic.AddInt64(o.BytesRecv, resp.ContentLength)
		} else {
			// Wrap the body with a counting reader
			resp.Body = &countingReadCloser{
				ReadCloser: resp.Body,
				counter:    o.BytesRecv,
			}
		}
	}
	return resp, err
}
