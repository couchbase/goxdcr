/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package crMeta

import (
	"fmt"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/hlv"
	"github.com/couchbase/goxdcr/v8/log"
)

// Return values from conflict resolution, based on conflict detection results.
type ConflictResolutionResult uint32

const (
	CRNone            ConflictResolutionResult = iota // No conflict resolution. This should ideally never happen.
	CRSendToTarget    ConflictResolutionResult = iota // When souce wins.
	CRSkip            ConflictResolutionResult = iota // When target wins.
	CRMerge           ConflictResolutionResult = iota // When there is a conflict.
	CRSetBackToSource ConflictResolutionResult = iota // When source has larger CAS but target wins. This can happen when target MV wins.
	CRError           ConflictResolutionResult = iota // When we have an error detecting conflict. This should ideally never happen.
)

func (cr ConflictResolutionResult) String() string {
	switch cr {
	case CRNone:
		return "None"
	case CRSendToTarget:
		return "SendToTarget"
	case CRSkip:
		return "Skip"
	case CRMerge:
		return "Merge"
	case CRSetBackToSource:
		return "SetBackToSource"
	case CRError:
		return "Error"
	}
	return "Unknown"
}

// Timestamp (CAS) based Conflict resolution.
// Optionally peforms conflict detection if conflict logging is enabled for the replication i.e. logConflict is true.
// Parses Hlv and other needed xattrs in mobile mode.
func ResolveConflictByCAS(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId,
	xattrEnabled, logConflict bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (ConflictDetectionResult, ConflictResolutionResult, error) {

	// No target document, replicate the source document.
	if resp.Status == mc.KEY_ENOENT {
		return CDWin, CRSendToTarget, nil
	}

	// Conflict Detection if conflict logging feature is enabled.
	CDResult, sourceDocMeta, targetDocMeta, err :=
		DetectConflictIfNeeded(req, resp, specs, sourceId, targetId, xattrEnabled, logConflict, uncompressFunc, logger)
	if err == base.ErrorDocumentNotFound {
		return CDWin, CRSendToTarget, nil
	} else if err != nil {
		return CDError, CRError, err
	}

	// Winner decided on document CAS.
	// If conflict logging is in use,
	// manual conflict resolution needs to be done, based on conflicts logged if needed.
	sourceWin := true
	if targetDocMeta.Cas > sourceDocMeta.Cas {
		sourceWin = false
	} else if targetDocMeta.Cas == sourceDocMeta.Cas {
		if targetDocMeta.RevSeq > sourceDocMeta.RevSeq {
			sourceWin = false
		} else if targetDocMeta.RevSeq == sourceDocMeta.RevSeq {
			// if the outgoing mutation is deletion and its revSeq and cas are the
			// same as the target side document, it would lose the conflict resolution
			if sourceDocMeta.Deletion || (targetDocMeta.Expiry > sourceDocMeta.Expiry) {
				sourceWin = false
			} else if targetDocMeta.Expiry == sourceDocMeta.Expiry {
				if targetDocMeta.Flags > sourceDocMeta.Flags {
					sourceWin = false
				} else if targetDocMeta.Flags == sourceDocMeta.Flags {
					sourceWin = resolveConflictByXattr(sourceDocMeta, targetDocMeta, true)
				}
			}
		}
	}
	if sourceWin {
		return CDResult, CRSendToTarget, nil
	} else {
		return CDResult, CRSkip, nil
	}
}

// Seqno (RevSeqno) based Conflict resolution.
// Optionally peforms conflict detection if conflict logging is enabled for the replication i.e. logConflict is true.
// Parses Hlv and other needed xattrs in mobile mode.
func ResolveConflictByRevSeq(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId,
	xattrEnabled, logConflict bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (ConflictDetectionResult, ConflictResolutionResult, error) {

	// No target document, replicate the source document
	if resp.Status == mc.KEY_ENOENT {
		return CDWin, CRSendToTarget, nil
	}

	// Conflict Detection if conflict logging feature is enabled.
	CDResult, sourceDocMeta, targetDocMeta, err :=
		DetectConflictIfNeeded(req, resp, specs, sourceId, targetId, xattrEnabled, logConflict, uncompressFunc, logger)
	if err == base.ErrorDocumentNotFound {
		return CDWin, CRSendToTarget, nil
	} else if err != nil {
		return CDError, CRError, err
	}

	// Winner decided on document RevSeqNo.
	// If conflict logging is in use,
	// manual conflict resolution needs to be done, based on conflicts logged if needed.
	sourceWin := true
	if targetDocMeta.RevSeq > sourceDocMeta.RevSeq {
		sourceWin = false
	} else if targetDocMeta.RevSeq == sourceDocMeta.RevSeq {
		if targetDocMeta.Cas > sourceDocMeta.Cas {
			sourceWin = false
		} else if targetDocMeta.Cas == sourceDocMeta.Cas {
			//if the outgoing mutation is deletion and its revSeq and cas are the
			//same as the target side document, it would lose the conflict resolution
			if sourceDocMeta.Deletion || (targetDocMeta.Expiry > sourceDocMeta.Expiry) {
				sourceWin = false
			} else if targetDocMeta.Expiry == sourceDocMeta.Expiry {
				if targetDocMeta.Flags > sourceDocMeta.Flags {
					sourceWin = false
				} else if targetDocMeta.Flags == sourceDocMeta.Flags {
					sourceWin = resolveConflictByXattr(sourceDocMeta, targetDocMeta, true)
				}
			}
		}
	}
	if sourceWin {
		return CDResult, CRSendToTarget, nil
	} else {
		return CDResult, CRSkip, nil
	}
}

// Custom Conflict resolution.
// Always peforms conflict detection irrepective of the value of logConflict.
// Always parses Hlv and resolves conflict using hlv.
func ResolveConflictByHlv(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId,
	xattrEnabled, logConflict bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (ConflictDetectionResult, ConflictResolutionResult, error) {

	// No target document, replicate the source document.
	if resp.Status == mc.KEY_ENOENT {
		return CDWin, CRSendToTarget, nil
	}

	// Conflict Detection is always performed in CCR.
	CDResult, sourceDocMeta, targetDocMeta, err :=
		DetectConflictIfNeeded(req, resp, specs, sourceId, targetId, xattrEnabled, true, uncompressFunc, logger)
	if err == base.ErrorDocumentNotFound {
		return CDWin, CRSendToTarget, nil
	} else if err != nil {
		return CDError, CRError, err
	}

	// Conflict Resolution using HLV.
	if sourceDocMeta.Cas < targetDocMeta.Cas {
		// We can only replicate from larger CAS to smaller.
		return CDResult, CRSkip, nil
	}
	switch CDResult {
	case CDWin:
		return CDResult, CRSendToTarget, nil
	case CDLose:
		if sourceDocMeta.Cas > targetDocMeta.Cas {
			return CDResult, CRSetBackToSource, nil
		} else {
			return CDResult, CRSkip, nil
		}
	case CDConflict:
		return CDResult, CRMerge, nil
	case CDEqual:
		if sourceDocMeta.Cas > targetDocMeta.Cas {
			// When HLV says equal because they contain each other, if source Cas is larger, we want to send so the CAS will converge.
			return CDResult, CRSendToTarget, nil
		} else {
			return CDResult, CRSkip, nil
		}
	}

	return CDResult, CRError,
		fmt.Errorf("bad value for CCR, req=%v%s%v, reqBody=%v%v%v, resp=%v%s%v, respBody=%v%v%v, cdRes=%v",
			base.UdTagBegin, req.Req, base.UdTagEnd,
			base.UdTagBegin, req.Req.Body, base.UdTagEnd,
			base.UdTagBegin, resp.Status, base.UdTagEnd,
			base.UdTagBegin, resp.Body, base.UdTagEnd,
			CDResult,
		)
}
