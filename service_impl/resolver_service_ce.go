// Copyright (c) 2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// +build !enterprise

package service_impl

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/service_def"
)

type ResolverSvc struct {
}

func NewResolverSvc(service_def.XDCRCompTopologySvc) *ResolverSvc {
	return &ResolverSvc{}
}
func (rs *ResolverSvc) ResolveAsync(params *base.ConflictParams, finish_ch chan bool) {
	return
}
func (rs *ResolverSvc) InitDefaultFunc() {
	return
}

func (rs *ResolverSvc) Start(sourceKVHost string, xdcrRestPort uint16) {
	return
}

func (rs *ResolverSvc) Started() bool {
	return false
}
