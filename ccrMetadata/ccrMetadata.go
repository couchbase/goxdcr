/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package ccrMetadata

type ConflictDetectionResult uint32

const (
	// LoseWithLargerCas means the source HLV loses but its CAS is larger. This can happen
	// when merges happen at multiple places, and target merged in more conflcits than source,
	// and did it earlier than the source. In this case we want to set the target mutation back
	// to source so the replication converges.
	Win               ConflictDetectionResult = iota
	Lose              ConflictDetectionResult = iota
	LoseWithLargerCas ConflictDetectionResult = iota
	Conflict          ConflictDetectionResult = iota
	Equal             ConflictDetectionResult = iota
	NoResult          ConflictDetectionResult = iota
)

// Custom CR related constants
const (
	// These are the fields in the HLV
	HLV_SRC_FIELD = "src" // src and ver combined is the cv in the design
	HLV_VER_FIELD = "ver" //
	HLV_MV_FIELD  = "mv"  // the MV field in _xdcr
	HLV_PV_FIELD  = "pv"  // The PV field in _xdcr

	// This is the HLV XATTR name. The leading "_" indicates a system XATTR
	XATTR_HLV = "_vv" // The HLV XATTR

	XATTR_SRC_PATH = XATTR_HLV + "." + HLV_SRC_FIELD
	XATTR_VER_PATH = XATTR_HLV + "." + HLV_VER_FIELD
	XATTR_MV_PATH  = XATTR_HLV + "." + HLV_MV_FIELD
	XATTR_PV_PATH  = XATTR_HLV + "." + HLV_PV_FIELD
)
