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
)

// Representation of source document information involved in CR.
// It encapsulates the mutation received over source dcp.
type SourceDocument struct {
	source hlv.DocumentSourceId // This would be the source bucket UUID converted to the DocumentSourceId format
	req    *base.WrappedMCRequest
}

func NewSourceDocument(req *base.WrappedMCRequest, source hlv.DocumentSourceId) *SourceDocument {
	return &SourceDocument{
		source: source,
		req:    req,
	}
}

func (doc *SourceDocument) GetMetadata(uncompressFunc base.UncompressFunc) (*CRMetadata, error) {
	docMeta := base.DecodeSetMetaReq(doc.req)
	cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev, err := getHlvFromMCRequest(doc.req, uncompressFunc)
	if err != nil {
		return nil, err
	}

	meta := CRMetadata{
		docMeta:   &docMeta,
		actualCas: docMeta.Cas,
		hadMou:    importCas > 0, // we will use the presence of importCAS (_mou.cas) to determine if we have _mou
	}

	err = meta.UpdateHLVIfNeeded(doc.source, cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev)
	if err != nil {
		return nil, err
	}

	doc.req.ImportMutation = meta.isImport

	return &meta, nil
}

// Representation of target document information involved in CR.
// It encapsulates subdoc lookup response from target bucket.
type TargetDocument struct {
	source       hlv.DocumentSourceId // This would be the target bucket UUID converted to the DcoumentSource format
	resp         *base.SubdocLookupResponse
	key          []byte
	xattrEnabled bool // This affects how to interpret getMeta response.
	includeHlv   bool
}

func NewTargetDocument(key []byte, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, source hlv.DocumentSourceId, xattrEnabled, includeHlv bool) (*TargetDocument, error) {
	if resp.Status == mc.KEY_ENOENT {
		return nil, base.ErrorDocumentNotFound
	}
	return &TargetDocument{
		source: source,
		resp: &base.SubdocLookupResponse{
			Specs: specs,
			Resp:  resp,
		},
		key:          key,
		xattrEnabled: xattrEnabled,
		includeHlv:   includeHlv,
	}, nil
}

func (doc *TargetDocument) GetMetadata() (*CRMetadata, error) {
	if doc.resp.Resp.Status == mc.KEY_ENOENT {
		return nil, base.ErrorDocumentNotFound
	}
	if doc.resp.Resp.Opcode == base.GET_WITH_META {
		// This is a getMeta response from target
		docMeta, err := base.DecodeGetMetaResp(doc.key, doc.resp.Resp, doc.xattrEnabled)
		if err != nil {
			return nil, err
		}
		meta := CRMetadata{
			docMeta:   &docMeta,
			hlv:       nil,
			actualCas: docMeta.Cas,
		}
		return &meta, err
	} else if doc.resp.Specs != nil && doc.resp.Resp.Opcode == mc.SUBDOC_MULTI_LOOKUP {
		// This is a sub doc response from target
		docMeta, err := base.DecodeSubDocResp(doc.key, doc.resp)
		if err != nil {
			return nil, err
		}
		meta := CRMetadata{
			docMeta:   &docMeta,
			actualCas: docMeta.Cas,
		}
		if doc.includeHlv {
			cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev, err := getHlvFromMCResponse(doc.resp)
			if err != nil {
				return nil, err
			}

			// We will use the presence of importCAS (_mou.cas) to determine if we have _mou
			meta.hadMou = importCas > 0

			err = meta.UpdateHLVIfNeeded(doc.source, cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev)
			if err != nil {
				return nil, err
			}
		}
		return &meta, nil
	}
	return nil, fmt.Errorf("GetMetadata: bad target document opcode %v, specs %v", doc.resp.Resp.Opcode, doc.resp.Specs)
}
