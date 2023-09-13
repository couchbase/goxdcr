// Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

//go:build !enterprise
// +build !enterprise

package service_impl

import (
	"github.com/couchbase/goxdcr/crMeta"
	"github.com/couchbase/goxdcr/service_def"
)

type ResolverSvc struct {
}

func NewResolverSvc(service_def.XDCRCompTopologySvc) *ResolverSvc {
	return &ResolverSvc{}
}
func (rs *ResolverSvc) ResolveAsync(params *crMeta.ConflictParams, finish_ch chan bool) {
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

func (rs *ResolverSvc) CheckMergeFunction(string) error {
	return nil
}
