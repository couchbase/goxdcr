// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

type PeriodicPushHandler struct{}

func NewPeriodicPushHandler() *PeriodicPushHandler {
	return &PeriodicPushHandler{}
}

func (p *PeriodicPushHandler) Start() error {
	return nil
}

func (p *PeriodicPushHandler) Stop() error {
	return nil
}

func (p *PeriodicPushHandler) RegisterOpaque(req Request, opts *SendOpts) error {
	return nil
}
