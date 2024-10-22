// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package throttlerSvc

// Keys for configurable service settings
const HighTokensKey = "HighTokens"
const MaxReassignableHighTokensKey = "MaxReassignableHighTokens"
const LowTokensKey = "LowTokens"
const NeedToCalibrateKey = "NeedToCalibrate"

type ThroughputThrottlerSvc interface {
	Start() error
	Stop() error

	// update various settings, e.g., high tokens and throughput limit
	UpdateSettings(setting map[string]interface{}) map[string]error

	// output:
	// true - if the mutation can be sent
	// false - if the mutation cannot be sent
	CanSend(isHighPriorityReplication bool) bool

	// blocks till the next measurement interval, when throughput allowance may become available
	Wait()
}
