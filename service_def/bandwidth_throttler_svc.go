// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
	Wait()
}
