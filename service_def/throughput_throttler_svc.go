// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

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
