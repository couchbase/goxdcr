// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import ()

//PipelineRuntimeContext manages all the services that attaches to the feed
type PipelineRuntimeContext interface {
	Start(map[string]interface{}) error
	Stop() error

	Pipeline() Pipeline

	//return a service handle
	Service(svc_name string) PipelineService

	//register a new service
	//if the feed is active, the service would be started rightway
	RegisterService(svc_name string, svc PipelineService) error

	//attach the service from the feed and stop the service's goroutine
	UnregisterService(srv_name string) error

	UpdateSettings(settings map[string]interface{}) error
}
