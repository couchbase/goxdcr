/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package filter

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbaselabs/gojsonsm"
)

const collateErrDesc = " Collate was used to determine outcome"

const (
	NotFiltered                 base.FilteringStatusType = iota
	FilteredOnATRDocument       base.FilteringStatusType = iota
	FilteredOnTxnClientRecord   base.FilteringStatusType = iota
	FilteredOnTxnsXattr         base.FilteringStatusType = iota
	FilteredOnUserDefinedFilter base.FilteringStatusType = iota
	FilteredOnMobileRecord      base.FilteringStatusType = iota
	FilteredOnOthers            base.FilteringStatusType = iota // could be filtered because of error, binary doc, expiry, deletion etc which do not need to be explicitly distinguished
)

type FilterImpl struct {
	id                       string
	hasFilterExpression      bool
	filterExpressionInternal string
	utils                    FilterUtils
	matcher                  gojsonsm.Matcher
	dp                       base.DataPool
	flags                    base.FilterFlagType
	slicesToBeReleasedBuf    [][]byte
	skipUncommittedTxn       uint32
	skipBinaryDocs           uint32
	mobileCompatible         uint32
}

func NewFilterWithSharedDP(id string, filterExpression string, utils FilterUtils, dp base.DataPool, filterModes base.FilterExpDelType, mobileCompatible int) (*FilterImpl, error) {
	filter := &FilterImpl{
		id:                    id,
		utils:                 utils,
		dp:                    dp,
		slicesToBeReleasedBuf: make([][]byte, 0, 2),
	}
	filter.mobileCompatible = uint32(mobileCompatible)

	if filterModes.IsSkipReplicateUncommittedTxnSet() {
		filter.skipUncommittedTxn = 1
	}

	if filterModes.IsSkipBinarySet() {
		filter.skipBinaryDocs = 1
	}

	if len(filterExpression) == 0 {
		return filter, nil
	}

	// if filter expression has been defined, proceed to populate expression filtering related fields
	filter.hasFilterExpression = true
	filter.filterExpressionInternal = base.ReplaceKeyWordsForExpression(filterExpression)
	matcher, err := base.GoJsonsmGetFilterExprMatcher(filter.filterExpressionInternal)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse expression: %v Err: %v", filter.filterExpressionInternal, err.Error())
	}
	filter.matcher = matcher

	if filter.matcher == nil {
		return nil, base.ErrorNoMatcher
	}

	if !base.FilterContainsXattrExpression(filterExpression) {
		filter.flags |= base.FilterFlagSkipXattr
	} else if base.FilterOnlyContainsXattrExpression(filterExpression) {
		filter.flags |= base.FilterFlagXattrOnly
	}

	if !base.FilterContainsKeyExpression(filterExpression) {
		filter.flags |= base.FilterFlagSkipKey
	} else if base.FilterOnlyContainsKeyExpression(filterExpression) {
		filter.flags |= base.FilterFlagKeyOnly
	}

	return filter, nil
}

func NewFilter(id string, filterExpression string, utils FilterUtils, filterModes base.FilterExpDelType, mobileCompatible int) (*FilterImpl, error) {
	dpPtr := utils.NewDataPool()
	if dpPtr == nil {
		return nil, base.ErrorNoDataPool
	}

	return NewFilterWithSharedDP(id, filterExpression, utils, dpPtr, filterModes, mobileCompatible)
}

func (filter *FilterImpl) GetInternalExpr() string {
	return filter.filterExpressionInternal
}

func (filter *FilterImpl) ShouldSkipUncommittedTxn() bool {
	return atomic.LoadUint32(&filter.skipUncommittedTxn) > 0
}

func (filter *FilterImpl) ShouldSkipBinaryDocs() bool {
	return atomic.LoadUint32(&filter.skipBinaryDocs) > 0
}

func (filter *FilterImpl) SetShouldSkipUncommittedTxn(val bool) {
	if val {
		atomic.StoreUint32(&filter.skipUncommittedTxn, 1)
	} else {
		atomic.StoreUint32(&filter.skipUncommittedTxn, 0)
	}
}

func (filter *FilterImpl) SetShouldSkipBinaryDocs(val bool) {
	if val {
		atomic.StoreUint32(&filter.skipBinaryDocs, 1)
	} else {
		atomic.StoreUint32(&filter.skipBinaryDocs, 0)
	}
}

func (filter *FilterImpl) SetMobileCompatibility(val uint32) {
	atomic.StoreUint32(&filter.mobileCompatible, val)
}

func (filter *FilterImpl) getMobileCompatibility() uint32 {
	return atomic.LoadUint32(&filter.mobileCompatible)
}

func (filter *FilterImpl) FilterUprEvent(wrappedUprEvent *base.WrappedUprEvent) (bool, error, string, int64, base.FilteringStatusType) {
	if wrappedUprEvent == nil || wrappedUprEvent.UprEvent == nil {
		return false, base.ErrorInvalidInput, "UprEvent or wrappedUprEvent is nil", 0, FilteredOnOthers
	}
	// Mobile filter applies to all scopes/collections so let's do that first
	if filter.filterMobileRelatedUprEvent(wrappedUprEvent.UprEvent) == false {
		return false, nil, "", 0, FilteredOnMobileRecord
	}
	// User defined filter doesn't apply to system scope
	if wrappedUprEvent.ColNamespace != nil && wrappedUprEvent.ColNamespace.ScopeName == base.SystemScopeName {
		return true, nil, "", 0, NotFiltered
	}

	// filter.slicesToBeReleasedBuf may be used to store temporary objects created in this method
	// make sure that these temporary objects are released before the method returns
	defer func() {
		for _, aSlice := range filter.slicesToBeReleasedBuf {
			filter.dp.PutByteSlice(aSlice)
		}
		filter.slicesToBeReleasedBuf = filter.slicesToBeReleasedBuf[:0]
	}()

	needToReplicate, body, endBodyPos, err, errDesc, totalFailedDpCnt, bodyHasBeenModified, filterStatus := filter.filterTransactionRelatedUprEvent(wrappedUprEvent.UprEvent, &filter.slicesToBeReleasedBuf)
	if err != nil || !needToReplicate {
		return false, err, errDesc, totalFailedDpCnt, filterStatus
	}

	dataTypeIsJson := wrappedUprEvent.UprEvent.DataType&mcc.JSONDataType > 0
	isTombstone := (wrappedUprEvent.UprEvent.Opcode == gomemcached.UPR_DELETION || wrappedUprEvent.UprEvent.Opcode == gomemcached.UPR_EXPIRATION)
	if filter.ShouldSkipBinaryDocs() && !dataTypeIsJson && !isTombstone {
		// Skip binary document
		return false, nil, "", totalFailedDpCnt, FilteredOnOthers
	}

	var failedDpCnt int64
	if filter.hasFilterExpression {
		needToReplicate, err, errDesc, failedDpCnt = filter.filterUprEvent(wrappedUprEvent.UprEvent, body, endBodyPos, &filter.slicesToBeReleasedBuf)
		if failedDpCnt > 0 {
			totalFailedDpCnt += failedDpCnt
		}

		if !needToReplicate {
			return needToReplicate, err, errDesc, totalFailedDpCnt, FilteredOnUserDefinedFilter
		}
	}

	// When body is not nil, and has been modified,
	// it means the body is meant to be used - it contains decompressed values of the original
	// compressed DCP document, and it has been stripped of any transactional related xattrs
	// Save the body so that it can be copied later and reused if it hasn't been done before (determined via flag)
	if body != nil && bodyHasBeenModified {
		valueBod, err := wrappedUprEvent.ByteSliceGetter(uint64(endBodyPos))
		if err != nil {
			return needToReplicate, err, "wrappedUprEvent.ByteSliceGetter", totalFailedDpCnt, filterStatus
		}
		copy(valueBod, body[0:endBodyPos])
		wrappedUprEvent.DecompressedValue = valueBod
		wrappedUprEvent.Flags.SetShouldUseDecompressedValue()
	}
	return needToReplicate, err, errDesc, totalFailedDpCnt, filterStatus
}

func (filter *FilterImpl) filterMobileRelatedUprEvent(uprEvent *mcc.UprEvent) (needToReplicate bool) {
	switch filter.getMobileCompatibility() {
	case base.MobileCompatibilityActive:
		if bytes.HasPrefix(uprEvent.Key, base.MobileDocPrefixSync) && !bytes.HasPrefix(uprEvent.Key, base.MobileDocPrefixSyncAtt) {
			return false
		} else {
			return true
		}
	}
	return true
}

// Returns:
//  1. needToReplicate bool - Whether or not the mutation should be allowed to continue through the pipeline
//  2. body []byte - body slice (will be stripped of SDK transaction metadata if non-legacy mode)
//  3. endBodyPos int - position of last valid byte in body
//  4. err error
//  5. errDesc string - If err is not nil, additional description
//  6. failedDpCnt int64 - Total bytes of failed datapool gets - which means len of []byte alloc (garbage)
//  7. If body has been modified due to stripping the transactional xattr
//  8. Status of txn based filtering - not filtered based on txns, not filtered because of error, not filtered because it is an ATR document,
//     not filtered because of txt xattrs, not filtered because of client txn records
//
// Note that body in 2 is not nil only in the following scenario:
// (1). uprEvent is compressed
// (2). We will need to perform filtering on body later.
// (3). There was transactional metadata in the Xattribute and has been stripped
// The body will be recycled. So any other users must copy otherwise the data slice will be returned to the datapool
// In this scenario body is a newly allocated byte slice, which holds the decompressed document body,
// and also contains extra bytes to accommodate key, so that it can be reused by advanced filtering later.
// In other scenarios body is nil and endBodyPos is -1
func (filter *FilterImpl) filterTransactionRelatedUprEvent(uprEvent *mcc.UprEvent, slicesToBeReleased *[][]byte) (bool, []byte, int, error, string, int64, bool, base.FilteringStatusType) {
	if base.Equals(uprEvent.Key, base.TransactionClientRecordKey) {
		// filter out transaction client records
		return false, nil, 0, nil, "", 0, false, FilteredOnTxnClientRecord
	}

	// active transaction records look like "_txn:atr-[VbucketId]-#[a-f1-9]+"
	if base.ActiveTxnRecordRegexp.Match(uprEvent.Key) {
		// filter out active transaction record
		return false, nil, 0, nil, "", 0, false, FilteredOnATRDocument
	}

	if uprEvent.DataType&mcc.XattrDataType == 0 {
		// no xattrs, no op
		return true, nil, 0, nil, "", 0, false, NotFiltered
	}

	// Whether we will need to filter on body later
	needToFilterBody := filter.hasFilterExpression && (filter.flags&base.FilterFlagKeyOnly == 0)

	// For UPR_MUTATION with xattrs, continue to filter based on whether transaction xattrs are present
	hasTransXattrs, body, endBodyPos, err, errDesc, failedDpCnt, uncompressedUprValue := filter.utils.CheckForTransactionXattrsInUprEvent(uprEvent, filter.dp, slicesToBeReleased, needToFilterBody)
	if err != nil {
		return false, nil, 0, err, errDesc, failedDpCnt, false, FilteredOnOthers
	}

	var bodyHasBeenModified bool
	filterStatus := NotFiltered
	passedFilter := true
	if filter.ShouldSkipUncommittedTxn() {
		// if mutation has transaction xattrs, do not replicate it
		passedFilter = !hasTransXattrs
		if !passedFilter {
			filterStatus = FilteredOnTxnsXattr
		}
	} else if hasTransXattrs {
		// Strip out transaction xattributes
		newBodySlice, err := filter.dp.GetByteSlice(uint64(len(uncompressedUprValue)))
		if err != nil {
			return false, nil, 0, err, errDesc, failedDpCnt, false, FilteredOnOthers
		}
		*slicesToBeReleased = append(*slicesToBeReleased, newBodySlice)

		xattrIterator, err := base.NewXattrIterator(uncompressedUprValue)
		if err != nil {
			return false, nil, 0, err, errDesc, failedDpCnt, false, FilteredOnOthers
		}
		bodyWithoutXttr, err := base.StripXattrAndGetBody(uncompressedUprValue)
		if err != nil {
			return false, nil, 0, err, errDesc, failedDpCnt, false, FilteredOnOthers
		}

		xattrComposer := base.NewXattrComposer(newBodySlice)

		for xattrIterator.HasNext() {
			key, value, err := xattrIterator.Next()
			if err != nil {
				errDesc = fmt.Sprintf("error during xattribute walk")
				if err != nil {
					return false, nil, 0, err, errDesc, failedDpCnt, false, FilteredOnOthers
				}
			}
			if base.Equals(key, base.TransactionXattrKey) {
				continue
			}
			err = xattrComposer.WriteKV(key, value)
			if err != nil {
				errDesc = fmt.Sprintf("error during xattribute composition")
				return false, nil, 0, err, errDesc, failedDpCnt, false, FilteredOnOthers
			}
		}

		var modifiedBodyHasAtLeastOneXattr bool
		body, modifiedBodyHasAtLeastOneXattr = xattrComposer.FinishAndAppendDocValue(bodyWithoutXttr, nil, nil)
		endBodyPos = len(body)
		bodyHasBeenModified = true
		if uprEvent.DataType&mcc.XattrDataType > 0 && !modifiedBodyHasAtLeastOneXattr {
			// Since Transactional Xattr was the only xattribute, the new document value should not have any xattribute
			uprEvent.DataType &^= mcc.XattrDataType
		}
	}

	return passedFilter, body, endBodyPos, nil, "", failedDpCnt, bodyHasBeenModified, filterStatus
}

// Passed in body, if not nil, is a decompressed byte slice produced by earlier processing steps
// If body is not nil, filterUprEvent will simply use it instead of having to perform decompression again
// The body will also be free of any transactional metadata that has been stripped unless that is opted out
// Returns:
// 1. bool - condition that the UPR event matched the filter expression, & hence needs to be replicated
// 2. err code
// 3. If err is not nil, additional description
// 4. Total bytes of failed datapool gets - which means len of []byte alloc (garbage)
func (filter *FilterImpl) filterUprEvent(uprEvent *mcc.UprEvent, body []byte, endBodyPos int, slicesToBeReleased *[][]byte) (bool, error, string, int64) {
	if uprEvent.Opcode == gomemcached.UPR_DELETION || uprEvent.Opcode == gomemcached.UPR_EXPIRATION {
		// For now, pass through
		return true, nil, "", 0
	}

	sliceToBeFiltered, err, errDesc, failedDpCnt := filter.utils.ProcessUprEventForFiltering(uprEvent, body, endBodyPos, filter.dp, filter.flags, slicesToBeReleased)
	if err != nil {
		if err == base.FilterForcePassThrough {
			return true, nil, "", failedDpCnt
		} else {
			return false, err, errDesc, failedDpCnt
		}
	}
	matched, status, err := filter.FilterByteSlice(sliceToBeFiltered)
	if err != nil {
		errDesc = fmt.Sprintf("gojsonsm filter returned err %v (%v) for document %v%v%v, data: %v%v%v",
			err.Error(), errDesc, base.UdTagBegin, string(uprEvent.Key), base.UdTagEnd, base.UdTagBegin, string(sliceToBeFiltered), base.UdTagEnd)
	} else if status&gojsonsm.MatcherCollateUsed > 0 {
		// no error returned
		errDesc = collateErrDesc
	}

	return matched, err, errDesc, failedDpCnt
}

func (filter *FilterImpl) FilterByteSlice(slice []byte) (matched bool, status int, err error) {
	defer filter.matcher.Reset()
	return base.MatchWrapper(filter.matcher, slice)
}
