// Copyright (c) 2018-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package parts

import (
	"fmt"
	"github.com/couchbase/gojsonsm"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	utilities "github.com/couchbase/goxdcr/utils"
)

type FilterIface interface {
	// Returns:
	// 1. bool - Whether or not it was a match
	// 2. err code
	// 3. If err is not nil, additional description
	// 4. Total bytes of failed datapool gets - which means len of []byte alloc (garbage)
	FilterUprEvent(uprEvent *mcc.UprEvent) (bool, error, string, int64)
}

const collateErrDesc = " Collate was used to determine outcome"

type Filter struct {
	id                       string
	hasFilterExpression      bool
	filterExpressionInternal string
	utils                    utilities.UtilsIface
	matcher                  gojsonsm.Matcher
	dp                       utilities.DataPoolIface
	flags                    base.FilterFlagType
	slicesToBeReleasedBuf    [][]byte
}

func NewFilter(id string, filterExpression string, utils utilities.UtilsIface) (*Filter, error) {
	dpPtr := utilities.NewDataPool()
	if dpPtr == nil {
		return nil, base.ErrorNoDataPool
	}

	filter := &Filter{
		id:                    id,
		utils:                 utils,
		dp:                    dpPtr,
		slicesToBeReleasedBuf: make([][]byte, 0, 2),
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

func (filter *Filter) FilterUprEvent(uprEvent *mcc.UprEvent) (bool, error, string, int64) {
	if uprEvent == nil {
		return false, base.ErrorInvalidInput, "UprEvent is nil", 0
	}

	// filter.slicesToBeReleasedBuf may be used to store temporary objects created in this method
	// make sure that these temporary objects are released before the method returns
	defer func() {
		for _, aSlice := range filter.slicesToBeReleasedBuf {
			filter.dp.PutByteSlice(aSlice)
		}
		filter.slicesToBeReleasedBuf = filter.slicesToBeReleasedBuf[:0]
	}()

	needToReplicate, body, endBodyPos, err, errDesc, totalFailedDpCnt := filter.filterTransactionRelatedUprEvent(uprEvent, &filter.slicesToBeReleasedBuf)
	if err != nil {
		return false, err, errDesc, totalFailedDpCnt
	}

	if !needToReplicate {
		return false, nil, "", totalFailedDpCnt
	}

	var matched bool
	var failedDpCnt int64
	if filter.hasFilterExpression {
		matched, err, errDesc, failedDpCnt = filter.filterUprEvent(uprEvent, body, endBodyPos, &filter.slicesToBeReleasedBuf)
		if failedDpCnt > 0 {
			totalFailedDpCnt += failedDpCnt
		}
		return matched, err, errDesc, totalFailedDpCnt
	}

	return true, nil, "", totalFailedDpCnt
}

// Returns:
// 1. needToReplicate bool - Whether or not the mutation should be allowed to continue through the pipeline
// 2. body []byte - body slice
// 3. endBodyPos int - position of last valid byte in body
// 4. err error
// 5. errDesc string - If err is not nil, additional description
// 6. failedDpCnt int64 - Total bytes of failed datapool gets - which means len of []byte alloc (garbage)
// Note that body in 2 is not nil only in the following scenario:
// (1). uprEvent is compressed
// (2). We will need to perform filtering on body later.
// In this scenario body is a newly allocated byte slice, which holds the decompressed document body,
// and also contains extra bytes to accommodate key, so that it can be reused by advanced filtering later.
// In other scenarios body is nil and endBodyPos is -1
func (filter *Filter) filterTransactionRelatedUprEvent(uprEvent *mcc.UprEvent, slicesToBeReleased *[][]byte) (bool, []byte, int, error, string, int64) {
	if base.Equals(uprEvent.Key, base.TransactionClientRecordKey) {
		// filter out transaction client records
		return false, nil, 0, nil, "", 0
	}

	// active transaction records look like "_txn:atr-[VbucketId]-#[a-f1-9]+"
	if base.ActiveTxnRecordRegexp.Match(uprEvent.Key) {
		// filter out active transaction record
		return false, nil, 0, nil, "", 0
	}

	if uprEvent.Opcode == mc.UPR_DELETION || uprEvent.Opcode == mc.UPR_EXPIRATION {
		// these mutations do not have xattrs and do not need xattr processing
		return true, nil, 0, nil, "", 0
	}

	if uprEvent.DataType&mcc.XattrDataType == 0 {
		// no xattrs, no op
		return true, nil, 0, nil, "", 0
	}

	// Whether we will need to filter on body later
	needToFilterBody := filter.hasFilterExpression && (filter.flags&base.FilterFlagKeyOnly == 0)

	// For UPR_MUTATION with xattrs, continue to filter based on whether transaction xattrs are present

	hasTransXattrs, body, endBodyPos, err, errDesc, failedDpCnt := filter.utils.CheckForTransactionXattrsInUprEvent(uprEvent, filter.dp, slicesToBeReleased, needToFilterBody)
	if err != nil {
		return false, nil, 0, err, errDesc, failedDpCnt
	}

	// if mutation has transaction xattrs, do not replicate it
	return !hasTransXattrs, body, endBodyPos, nil, "", failedDpCnt
}

// Passed in body, if not nil, is a decompressed byte slice produced by earlier processing steps
// If body is not nil, filterUprEvent will simply use it instead of having to perform decompression again
// Returns:
// 1. bool - Whether or not it was a match
// 2. err code
// 3. If err is not nil, additional description
// 4. Total bytes of failed datapool gets - which means len of []byte alloc (garbage)
func (filter *Filter) filterUprEvent(uprEvent *mcc.UprEvent, body []byte, endBodyPos int, slicesToBeReleased *[][]byte) (bool, error, string, int64) {
	if uprEvent.Opcode == mc.UPR_DELETION || uprEvent.Opcode == mc.UPR_EXPIRATION {
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

func (filter *Filter) FilterByteSlice(slice []byte) (matched bool, status int, err error) {
	defer filter.matcher.Reset()
	return base.MatchWrapper(filter.matcher, slice)
}
