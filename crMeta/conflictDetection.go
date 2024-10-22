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

// Return values from conflict detection.
type ConflictDetectionResult uint32

const (
	CDNone     ConflictDetectionResult = iota // There was no conflict detection done. Eg: If conflict logging is not enabled or if hlv is not used.
	CDWin      ConflictDetectionResult = iota // No conflict, source wins.
	CDLose     ConflictDetectionResult = iota // No conflict, target wins.
	CDConflict ConflictDetectionResult = iota // Conflict detected between source and target doc.
	CDEqual    ConflictDetectionResult = iota // Source and target docs are equal.
	CDError    ConflictDetectionResult = iota // There was a error in conflict detection. This should ideally never happen.
)

func (cdr ConflictDetectionResult) String() string {
	switch cdr {
	case CDNone:
		return "None"
	case CDWin:
		return "Win"
	case CDLose:
		return "Lose"
	case CDConflict:
		return "Conflict"
	case CDEqual:
		return "Equal"
	}
	return "Unknown"
}

func (cdr ConflictDetectionResult) IsConflict() bool {
	return cdr == CDConflict
}

// Detect conflict by using source and target HLVs.
func DetectConflict(source *CRMetadata, target *CRMetadata) (ConflictDetectionResult, error) {
	sourceHLV := source.GetHLV()
	targetHLV := target.GetHLV()

	if sourceHLV == nil || targetHLV == nil {
		return CDError, fmt.Errorf("cannot detect conflict with nil HLVs, sourceHLV=%v, targetHLV=%v", sourceHLV, targetHLV)
	}

	c1 := sourceHLV.Contains(targetHLV)
	c2 := targetHLV.Contains(sourceHLV)
	if c1 && c2 {
		return CDEqual, nil
	} else if c1 {
		return CDWin, nil
	} else if c2 {
		return CDLose, nil
	}
	return CDConflict, nil
}

// Given source mutation and target doc response,
// Peform conflict detection (needToDetectConflict is true) if:
// 1. Conflict resolution type is CCR.
// 2. Conflict logging feature is turned on.
// Returns conflict detection result if performed,
// also returns the source and target document metadata for subsequent conflict resolution.
func DetectConflictIfNeeded(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId,
	xattrEnabled, needToDetectConflict bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (ConflictDetectionResult, base.DocumentMetadata, base.DocumentMetadata, error) {

	// source dcp mutation as "source document"
	var sourceDoc *SourceDocument
	// target getMeta or subdocOp response as "target document"
	var targetDoc *TargetDocument
	// source and target document metadata like CAS, RevSeqno etc, except xattrs (hlv).
	var sourceDocMeta, targetDocMeta base.DocumentMetadata
	// source and target entire metadata including document metadata like CAS, RevSeqno etc, and xattrs like hlv.
	var targetMeta, sourceMeta *CRMetadata
	var err error

	if resp.Opcode == base.GET_WITH_META {
		// GET_WITH_META will also be used when only ECCV is on (mobile is off) and cas < max_cas.
		// source HLV is not parsed and target HLV is not fetched.

		// non-hlv based replications
		// No conflict detection required
		sourceDocMeta = base.DecodeSetMetaReq(req)
		targetDocMeta, err = base.DecodeGetMetaResp(req.Req.Key, resp, xattrEnabled)
		if err != nil {
			err = fmt.Errorf("error decoding GET_META response for key=%v%s%v, respBody=%v%v%v, xattrEnabled=%v, err=%v",
				base.UdTagBegin, req.Req.Key, base.UdTagEnd,
				base.UdTagBegin, resp.Body, base.UdTagEnd,
				xattrEnabled, err)
			return CDError, sourceDocMeta, targetDocMeta, err
		}

		return CDNone, sourceDocMeta, targetDocMeta, nil
	}

	// We always parse the HLV in the source mutation for CR, if mobile is on, given that there could
	// be import on source cluster and it could be an active site in the mixed mode of the SGW+XDCR active-passive
	// setup, which is supported.
	// However for the target doc, we only fetch HLV if cas >= max_cas and if ECCV is on.
	// For the following cases, we don't fetch target HLV:
	// 1. mobile is on, ECCV is on and cas < max_cas (mixed mode) - this is fine as long as it was an
	// active-passive setup when this mutation was created.
	// 2. mobile is on, but ECCV is off - not supported and can lead to data loss as import on target can win CR.
	// Hence, target is not expected to have import in mixed mode, assuming that it is a passive site in the mixed
	// mode. Doing import on target, which is a passive site during mixed mode may cause data loss.

	if resp.Opcode != mc.SUBDOC_MULTI_LOOKUP {
		err = fmt.Errorf("unknown response %v for CR, for key=%v%s%v, req=%v%s%v, reqBody=%v%v%v, err=%v", resp.Opcode,
			base.UdTagBegin, req.Req.Key, base.UdTagEnd,
			base.UdTagBegin, req.Req, base.UdTagEnd,
			base.UdTagBegin, req.Req.Body, base.UdTagEnd, err)
		return CDError, sourceDocMeta, targetDocMeta, err
	}

	// source document metadata
	sourceDoc = NewSourceDocument(req, sourceId)
	sourceMeta, err = sourceDoc.GetMetadata(uncompressFunc)
	if err != nil {
		err = fmt.Errorf("error decoding source mutation for key=%v%s%v, req=%v%s%v, reqBody=%v%v%v, err=%v",
			base.UdTagBegin, req.Req.Key, base.UdTagEnd,
			base.UdTagBegin, req.Req, base.UdTagEnd,
			base.UdTagBegin, req.Req.Body, base.UdTagEnd, err)
		return CDError, sourceDocMeta, targetDocMeta, err
	}
	sourceDocMeta = *sourceMeta.docMeta

	// target document metadata
	targetDoc, err = NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled, req.HLVModeOptions.IncludeTgtHlv)
	if err == base.ErrorDocumentNotFound {
		return CDNone, sourceDocMeta, targetDocMeta, err
	} else if err != nil {
		err = fmt.Errorf("error creating target document for key=%v%s%v, respBody=%v, resp=%s, specs=%v, xattrEnabled=%v, err=%v",
			base.UdTagBegin, req.Req.Key, base.UdTagEnd,
			resp.Body, resp.Status, specs, xattrEnabled, err)
		return CDError, sourceDocMeta, targetDocMeta, err
	}
	targetMeta, err = targetDoc.GetMetadata()
	if err == base.ErrorDocumentNotFound {
		return CDNone, sourceDocMeta, targetDocMeta, err
	} else if err != nil {
		err = fmt.Errorf("error decoding target SUBDOC_MULTI_LOOKUP response for key=%v%s%v, respBody=%v, resp=%s, specs=%v, xattrEnabled=%v, err=%v",
			base.UdTagBegin, req.Req.Key, base.UdTagEnd,
			resp.Body, resp.Status, specs, xattrEnabled, err)
		return CDError, sourceDocMeta, targetDocMeta, err
	}
	targetDocMeta = *targetMeta.docMeta

	// decide if we need to perform subdoc op to avoid target CAS rollback.
	setSubdocOpIfNeeded(sourceMeta, targetMeta, req)

	// perform conflict detection if needed
	var cdResult ConflictDetectionResult
	if needToDetectConflict {
		cdResult, err = DetectConflict(sourceMeta, targetMeta)
		if err != nil {
			err = fmt.Errorf("error detecting conflict key=%v%s%v, sourceMeta=%s, targetMeta=%s, err=%v",
				base.UdTagBegin, req.Req.Key, base.UdTagEnd,
				sourceMeta, targetMeta, err)
			return CDError, sourceDocMeta, targetDocMeta, err
		}
	}

	if logger.GetLogLevel() >= log.LogLevelDebug {
		logger.Debugf("req=%v%s%v,reqBody=%v%v%v,resp=%s,respBody=%v,sourceMeta=%s,targetMeta=%s,cdRes=%v",
			base.UdTagBegin, req.Req, base.UdTagEnd,
			base.UdTagBegin, req.Req.Body, base.UdTagEnd,
			resp.Status, resp.Body, sourceMeta, targetMeta, cdResult,
		)
	}

	return cdResult, sourceDocMeta, targetDocMeta, nil
}
