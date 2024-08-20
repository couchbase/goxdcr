// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

//PipelineRuntimeContext manages all the services that attaches to the feed
type PipelineRuntimeContext interface {
	Start(metadata.ReplicationSettingsMap) error
	Stop() base.ErrorMap

	Pipeline() Pipeline

	//return a service handle
	Service(svc_name string) PipelineService

	//register a new service
	//if the feed is active, the service would be started rightway
	RegisterService(svc_name string, svc PipelineService) error

	//attach the service from the feed and stop the service's goroutine
	UnregisterService(srv_name string) error

	UpdateSettings(settings metadata.ReplicationSettingsMap) error
}
