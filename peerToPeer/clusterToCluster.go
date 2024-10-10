// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

// /xdcr/c2cCommunications compatible P2P OpCodes
var c2cCompatibleOpCodes = map[OpCode]bool{
	ReqSrcHeartbeat: true,
}

func IsC2cCompatibleOpCode(op OpCode) bool {
	return c2cCompatibleOpCodes[op]
}
