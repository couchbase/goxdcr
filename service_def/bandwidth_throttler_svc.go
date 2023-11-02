// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

type BandwidthThrottlerSvc interface {
	// input:
	// 1. numberOfBytes - the total number of bytes that caller wants to send
	// 2. minNumberOfBytes - the minimum number of bytes that caller wants to send, if numberOfBytes cannot be send
	// 3. numberOfBytesOfFirstItem - the number of bytes of the first item in the caller's list
	// output:
	// 1. bytesCanSend - the number of bytes that caller can proceed to send.
	//    it can take one of three values: numberOfBytes, minNumberOfBytes, 0
	// 2. bytesAllowed - the number of bytes remaining in bandwidth allowance that caller can ATTEMPT to send
	//    caller cannot just send this number of bytes, though. It has to call Throttle() again
	Throttle(numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem int64) (bytesCanSend int64, bytesAllowed int64)

	// blocks till the next measurement interval, when bandwidth usage allowance may become available
	Wait() error
}
